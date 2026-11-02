use std::num::NonZero;

use o3::mem::slice::Ext as _;

use crate::{Error, input::is_whitespace, segment::Atom, source::string::String};

pub(crate) const MAX_DEPTH: usize = 128;
const PER_WORD: usize = u64::BITS as usize / 4;
const WORDS: usize = MAX_DEPTH / PER_WORD;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum Phase {
    ObjectOpen,
    ObjectKey,
    ObjectColon,
    ObjectValue,
    ObjectNext,
    ArrayOpen,
    ArrayValue,
    ArrayNext,
}

/// Four grammar bits per container, with no input owner or borrowed segment.
#[derive(Clone, Copy)]
pub(crate) struct Nesting {
    stack: [u64; WORDS],
    depth: u8,
    limit: NonZero<u8>,
    mode: Mode,
}

#[derive(Clone, Copy)]
enum Mode {
    Structural,
    String(String),
    Atom(Atom),
    Admitted { quoted: bool, escaped: bool },
}

enum Change {
    Mode(Mode),
    Set { phase: Phase, mode: Mode },
    Open { phase: Phase, child: Phase },
    Close,
}

impl Nesting {
    /// Validates a run that leaves the current grammar state unchanged.
    pub(crate) fn skip<'a>(&self, chunk: &'a [u8]) -> &'a [u8] {
        match &self.mode {
            Mode::String(string) => string.skip(chunk),
            Mode::Structural => chunk.split_while(|byte| is_whitespace(*byte)).0,
            Mode::Atom(_) => &chunk[..0],
            Mode::Admitted { quoted, escaped } => {
                if *escaped {
                    return &chunk[..0];
                }
                let len = chunk
                    .iter()
                    .position(|byte| {
                        if *quoted {
                            matches!(byte, b'"' | b'\\')
                        } else {
                            matches!(byte, b'"' | b'{' | b'[' | b'}' | b']')
                        }
                    })
                    .unwrap_or(chunk.len());
                &chunk[..len]
            }
        }
    }

    pub(crate) fn start(open: u8) -> crate::Result<Self> {
        Self::start_depth(open, crate::parsing::Depth::ROOT)
    }

    pub(crate) fn start_depth(open: u8, depth: crate::parsing::Depth) -> crate::Result<Self> {
        let limit = depth.nonzero()?;
        let phase = match open {
            b'{' => Phase::ObjectOpen,
            b'[' => Phase::ArrayOpen,
            _ => return Err(Error::Invalid),
        };
        let mut nesting = Self {
            stack: [0; WORDS],
            depth: 1,
            limit,
            mode: Mode::Structural,
        };
        nesting.set(phase);
        Ok(nesting)
    }

    /// The enclosing immutable input owns a complete, previously checked compound.
    pub(crate) fn admitted() -> Self {
        Self {
            stack: [0; WORDS],
            depth: 1,
            limit: NonZero::<u8>::MIN.saturating_add((MAX_DEPTH - 1) as u8),
            mode: Mode::Admitted {
                quoted: false,
                escaped: false,
            },
        }
    }

    /// Validates the next transition once, then commits its established grammar proof.
    /// Invalid input and depth overflow leave the tracker unchanged.
    pub(crate) fn step(&mut self, byte: u8) -> crate::Result<bool> {
        let change = match self.mode {
            Mode::Admitted {
                ref mut quoted,
                ref mut escaped,
            } => {
                if *escaped {
                    *escaped = false;
                } else if *quoted {
                    match byte {
                        b'\\' => *escaped = true,
                        b'"' => *quoted = false,
                        _ => {}
                    }
                } else {
                    match byte {
                        b'"' => *quoted = true,
                        b'{' | b'[' => self.depth += 1,
                        b'}' | b']' => self.depth -= 1,
                        _ => {}
                    }
                }
                return Ok(self.depth == 0);
            }
            _ if self.depth == 0 => return Err(Error::Invalid),
            Mode::String(mut string) => {
                let mode = if string.step(byte)?.is_none() {
                    Mode::Structural
                } else {
                    Mode::String(string)
                };
                Change::Mode(mode)
            }
            Mode::Atom(atom) => {
                if !is_whitespace(byte) && !matches!(byte, b',' | b'}' | b']') {
                    Change::Mode(Mode::Atom(atom.step(byte)?))
                } else {
                    atom.finish()?;
                    self.transition(byte)?
                }
            }
            Mode::Structural => self.transition(byte)?,
        };
        match change {
            Change::Mode(mode) => self.mode = mode,
            Change::Set { phase, mode } => {
                self.set(phase);
                self.mode = mode;
            }
            Change::Open { phase, child } => {
                self.set(phase);
                self.depth += 1;
                self.set(child);
                self.mode = Mode::Structural;
            }
            Change::Close => {
                self.depth -= 1;
                self.mode = Mode::Structural;
            }
        }
        Ok(self.depth == 0)
    }

    fn transition(&mut self, byte: u8) -> crate::Result<Change> {
        if is_whitespace(byte) {
            return Ok(Change::Mode(Mode::Structural));
        }
        let phase = self.phase();
        match phase {
            Phase::ObjectOpen if byte == b'}' => Ok(Change::Close),
            Phase::ObjectOpen | Phase::ObjectKey => {
                if byte != b'"' {
                    return Err(Error::Invalid);
                }
                Ok(Change::Set {
                    phase: Phase::ObjectColon,
                    mode: Mode::String(String::new()),
                })
            }
            Phase::ObjectColon => {
                if byte != b':' {
                    return Err(Error::Invalid);
                }
                Ok(Change::Set {
                    phase: Phase::ObjectValue,
                    mode: Mode::Structural,
                })
            }
            Phase::ObjectNext => match byte {
                b'}' => Ok(Change::Close),
                b',' => Ok(Change::Set {
                    phase: Phase::ObjectKey,
                    mode: Mode::Structural,
                }),
                _ => Err(Error::Invalid),
            },
            Phase::ArrayOpen if byte == b']' => Ok(Change::Close),
            Phase::ArrayNext => match byte {
                b']' => Ok(Change::Close),
                b',' => Ok(Change::Set {
                    phase: Phase::ArrayValue,
                    mode: Mode::Structural,
                }),
                _ => Err(Error::Invalid),
            },
            Phase::ObjectValue | Phase::ArrayOpen | Phase::ArrayValue => {
                let phase = if phase == Phase::ObjectValue {
                    Phase::ObjectNext
                } else {
                    Phase::ArrayNext
                };
                self.value(byte, phase)
            }
        }
    }

    fn value(&self, byte: u8, phase: Phase) -> crate::Result<Change> {
        let mode = match byte {
            b'"' => Mode::String(String::new()),
            b'{' | b'[' => {
                if self.depth == self.limit.get() {
                    return Err(Error::Depth);
                }
                let child = if byte == b'{' {
                    Phase::ObjectOpen
                } else {
                    Phase::ArrayOpen
                };
                return Ok(Change::Open { phase, child });
            }
            _ => Mode::Atom(Atom::new(byte, false)?.step(byte)?),
        };
        Ok(Change::Set { phase, mode })
    }

    fn phase(&mut self) -> Phase {
        const PHASES: [Phase; 8] = [
            Phase::ObjectOpen,
            Phase::ObjectKey,
            Phase::ObjectColon,
            Phase::ObjectValue,
            Phase::ObjectNext,
            Phase::ArrayOpen,
            Phase::ArrayValue,
            Phase::ArrayNext,
        ];
        let (word, shift) = self.top();
        let bits = (*word >> shift) as usize;
        PHASES[bits & 7]
    }

    fn set(&mut self, phase: Phase) {
        let (word, shift) = self.top();
        *word = (*word & !(0xf << shift)) | (u64::from(phase as u8) << shift);
    }

    fn top(&mut self) -> (&mut u64, usize) {
        let index = usize::from(self.depth - 1);
        // SAFETY: checked traversal reaches phase/set only at depth 1..=limit,
        // and Depth admission bounds limit by MAX_DEPTH. Startup uses depth 1;
        // admitted replay never projects grammar words.
        let word = unsafe { self.stack.get_unchecked_mut(index / PER_WORD) };
        (word, (index % PER_WORD) * 4)
    }
}

const _: () = assert!(MAX_DEPTH != 0 && MAX_DEPTH <= u8::MAX as usize);
const _: () = assert!(MAX_DEPTH.is_multiple_of(PER_WORD));
