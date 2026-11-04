use crate::{field::metadata::map::record, uri::Source};

const LANE: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Byte {
        at: usize,
        byte: u8,
    },
    Escape {
        at: usize,
        first: Option<u8>,
        second: Option<u8>,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct Plan {
    end: usize,
    query: At,
}

#[derive(Clone, Copy)]
pub(crate) enum At {
    Path,
    Query(usize),
}

impl Plan {
    pub(crate) const fn end(self) -> usize {
        self.end
    }

    pub(crate) const fn query(self) -> At {
        self.query
    }

    pub(crate) fn path_end(self) -> usize {
        match self.query {
            At::Path => self.end,
            At::Query(at) => at,
        }
    }
}

pub(crate) enum Outcome {
    Complete(Plan),
    Incomplete,
    Invalid(Error),
}

#[repr(transparent)]
pub(crate) struct Scan<'a>(&'a [u8]);

impl<'a> Scan<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    pub(crate) fn prefix(self) -> Outcome {
        match run::<true>(self.0, Cursor::new(), |at, context| {
            self.locate(at, context)
        }) {
            Ok(Stage::Found(plan)) => Outcome::Complete(plan),
            Ok(Stage::Exhausted(_)) => Outcome::Incomplete,
            Err(error) => Outcome::Invalid(error),
        }
    }

    /// Parses the whole input; reaching its end is a completed plan.
    pub(crate) fn complete(self) -> Result<Plan, Error> {
        let len = self.0.len();
        match run::<false>(self.0, Cursor::new(), |at, context| {
            self.locate(at, context)
        })? {
            Stage::Found(plan) => Ok(plan),
            Stage::Exhausted(cursor) => Ok(cursor.finish(len)),
        }
    }

    fn locate(&self, start: usize, context: Context) -> Progress {
        let Some(input) = self.0.get(start..) else {
            return Progress::End;
        };
        let (lanes, tail) = input.as_chunks::<LANE>();
        let Some((first, rest)) = lanes.split_first() else {
            return Progress::Tail(start);
        };
        if let Some(offset) = Self::first(Self::mask(*first, context)) {
            return Progress::Special(start + offset);
        }
        let mut index = start + LANE;
        let (groups, rest) = rest.as_chunks::<4>();
        for group in groups {
            let masks = group.map(|chunk| Self::mask(chunk, context));
            if masks[0] | masks[1] | masks[2] | masks[3] != 0 {
                for (lane, mask) in masks.into_iter().enumerate() {
                    if let Some(offset) = Self::first(mask) {
                        return Progress::Special(index + lane * LANE + offset);
                    }
                }
            }
            index += 4 * LANE;
        }
        for lane in rest {
            if let Some(offset) = Self::first(Self::mask(*lane, context)) {
                return Progress::Special(index + offset);
            }
            index += LANE;
        }
        if tail.is_empty() {
            Progress::End
        } else {
            Progress::Tail(index)
        }
    }

    /// Offset of the first nonzero byte in a little-endian lane bitmap.
    fn first(bits: u128) -> Option<usize> {
        (bits != 0).then(|| bits.trailing_zeros() as usize / u8::BITS as usize)
    }

    fn mask(chunk: [u8; LANE], context: Context) -> u128 {
        u128::from_le_bytes(
            chunk.map(|byte| u8::from(!Self::is_path(byte, context)).wrapping_neg()),
        )
    }

    fn is_path(byte: u8, context: Context) -> bool {
        byte.is_ascii_alphabetic()
            | (byte.wrapping_sub(b'&') <= b';' - b'&')
            | (byte == b'!')
            | (byte == b'$')
            | (byte == b'=')
            | (byte == b'@')
            | (byte == b'_')
            | (byte == b'~')
            | (matches!(context, Context::Query) & (byte == b'?'))
    }
}

/// Parses a complete path/query without joining its borrowed segments.
pub(crate) fn complete_span(value: record::Span<'_>) -> Result<Plan, Error> {
    let (first, second) = value.parts();
    if second.is_empty() {
        return Scan::new(first).complete();
    }
    if first.is_empty() {
        return Scan::new(second).complete();
    }
    match run::<false>(value, Cursor::new(), |at, _| Progress::Tail(at))? {
        Stage::Found(plan) => Ok(plan),
        Stage::Exhausted(cursor) => Ok(cursor.finish(value.len())),
    }
}

fn run<'a, const PREFIX: bool>(
    source: impl Source<'a>,
    mut state: Cursor,
    locate: impl Fn(usize, Context) -> Progress,
) -> Result<Stage, Error> {
    loop {
        let context = state.context();
        let at = match locate(state.at(), context) {
            Progress::Special(at) => at,
            Progress::Tail(at) => {
                let Some(tail) = source.sub(at, source.len()) else {
                    return Ok(Stage::Exhausted(state));
                };
                let Some(offset) = tail
                    .span()
                    .iter()
                    .position(|byte| !Scan::is_path(*byte, context))
                else {
                    return Ok(Stage::Exhausted(state));
                };
                at + offset
            }
            Progress::End => return Ok(Stage::Exhausted(state)),
        };
        let Some(byte) = source.get(at) else {
            return Ok(Stage::Exhausted(state));
        };
        match byte {
            b' ' if PREFIX => return Ok(Stage::Found(state.finish(at))),
            b'?' if matches!(context, Context::Path) => {
                state.at = at + 1;
                state.query = At::Query(at);
            }
            b'%' => match escape(source, at) {
                Ok(next) => state.at = next,
                Err(Error::Escape { first, second, .. })
                    if PREFIX
                        && second.is_none()
                        && first.is_none_or(|byte| byte.is_ascii_hexdigit()) =>
                {
                    return Ok(Stage::Exhausted(state));
                }
                Err(error) => return Err(error),
            },
            byte => return Err(Error::Byte { at, byte }),
        }
    }
}

fn escape<'a>(source: impl Source<'a>, at: usize) -> Result<usize, Error> {
    let first = at.checked_add(1).and_then(|index| source.get(index));
    let second = at.checked_add(2).and_then(|index| source.get(index));
    if first.is_some_and(|byte| byte.is_ascii_hexdigit())
        && second.is_some_and(|byte| byte.is_ascii_hexdigit())
    {
        return at.checked_add(3).ok_or(Error::Escape { at, first, second });
    }
    Err(Error::Escape { at, first, second })
}

enum Progress {
    Special(usize),
    Tail(usize),
    End,
}

/// Terminal of one scan: a located terminator, or the input running out.
enum Stage {
    Found(Plan),
    Exhausted(Cursor),
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum Context {
    Path,
    Query,
}

#[derive(Clone, Copy)]
struct Cursor {
    at: usize,
    query: At,
}

impl Cursor {
    const fn new() -> Self {
        Self {
            at: 0,
            query: At::Path,
        }
    }

    fn at(self) -> usize {
        self.at
    }

    fn context(self) -> Context {
        match self.query {
            At::Path => Context::Path,
            At::Query(_) => Context::Query,
        }
    }

    fn finish(self, end: usize) -> Plan {
        Plan {
            end,
            query: self.query,
        }
    }
}

const _: () = assert!(core::mem::size_of::<Scan<'_>>() == core::mem::size_of::<&[u8]>());
const _: () = assert!(core::mem::size_of::<At>() == core::mem::size_of::<Option<usize>>());
const _: () = assert!(core::mem::size_of::<Context>() == core::mem::size_of::<bool>());
