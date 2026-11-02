use crate::{input::Taken, segment::input::Input};

mod replay;
pub(super) use replay::Replay;

pub(super) struct Cursor {
    pub(super) absolute: usize,
    pub(super) segment: usize,
    local: usize,
}

pub(super) struct Chunk<'a> {
    bytes: &'a [u8],
}

pub(super) struct Advance {
    bytes: usize,
    reaches_end: bool,
}

impl<'a> Chunk<'a> {
    pub(super) fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Advances over a prefix of this chunk's bytes, as required of each caller.
    pub(super) fn advance(self, skipped: &[u8]) -> Advance {
        Advance {
            bytes: skipped.len(),
            reaches_end: skipped.len() == self.bytes.len(),
        }
    }
}

impl Cursor {
    pub(super) const fn new() -> Self {
        Self {
            absolute: 0,
            segment: 0,
            local: 0,
        }
    }

    pub(super) fn at(input: &Input<'_>, absolute: usize) -> Option<Self> {
        if absolute == input.len() {
            return Some(Self {
                absolute,
                segment: input.count(),
                local: 0,
            });
        }
        let (segment, local) = input.locate(absolute)?;
        Some(Self {
            absolute,
            segment,
            local,
        })
    }

    pub(super) fn read(&self, input: &Input<'_>) -> Option<(u8, Advance)> {
        let tail = input.bytes_at(self.segment, self.local)?;
        let byte = *tail.first()?;
        Some((
            byte,
            Advance {
                bytes: 1,
                reaches_end: tail.len() == 1,
            },
        ))
    }

    pub(super) fn chunk<'a>(&self, input: &'a Input<'_>) -> Option<Chunk<'a>> {
        input
            .bytes_at(self.segment, self.local)
            .map(|bytes| Chunk { bytes })
    }

    pub(super) fn take(&mut self, input: &Input<'_>, want: u8) -> Taken {
        self.take_if(input, |byte| byte == want)
    }

    pub(super) fn take_ws(&mut self, input: &Input<'_>) -> Taken {
        self.take_if(input, crate::input::is_whitespace)
    }

    fn take_if(&mut self, input: &Input<'_>, want: impl FnOnce(u8) -> bool) -> Taken {
        let Some((byte, advance)) = self.read(input) else {
            return Taken::No;
        };
        if !want(byte) {
            return Taken::No;
        }
        self.advance(advance);
        Taken::Yes
    }

    pub(super) fn advance(&mut self, advance: Advance) {
        self.absolute += advance.bytes;
        self.local += advance.bytes;
        if advance.reaches_end {
            self.segment += 1;
            self.local = 0;
        }
    }
}
