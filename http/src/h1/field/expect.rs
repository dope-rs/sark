use crate::{field, h1::framing};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Unsupported,
}

/// Borrow-free action admitted from an HTTP/1 `Expect` field.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Decision {
    #[default]
    None,
    Continue,
}

impl Decision {
    pub const fn is_continue(self) -> bool {
        matches!(self, Self::Continue)
    }
}

/// One-byte, borrow-free request expectation state.
#[repr(transparent)]
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct State(u8);

impl State {
    const CONTINUE: u8 = 1 << 0;
    const UNSUPPORTED: u8 = 1 << 1;

    pub fn push(mut self, value: &[u8]) -> Self {
        for raw in value.split(|byte| *byte == b',') {
            let token = field::Error::trim_ows(raw);
            if token.is_empty() {
                continue;
            }
            if token.eq_ignore_ascii_case(b"100-continue") {
                self.0 |= Self::CONTINUE;
            } else {
                self.0 |= Self::UNSUPPORTED;
            }
        }
        self
    }

    pub const fn finish(self, body: framing::RequestBody) -> Result<Decision, Error> {
        if self.0 & Self::UNSUPPORTED != 0 {
            return Err(Error::Unsupported);
        }
        Ok(
            if self.0 & Self::CONTINUE != 0 && !matches!(body, framing::RequestBody::Empty) {
                Decision::Continue
            } else {
                Decision::None
            },
        )
    }
}

const _: () = assert!(core::mem::size_of::<Decision>() == 1);
const _: () = assert!(core::mem::size_of::<State>() == 1);
