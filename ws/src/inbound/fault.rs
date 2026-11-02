use o3::alloc;

use crate::{
    fragment, frame,
    inbound::{Code, Mask},
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Fault {
    Frame(frame::Error),
    Mask { expected: Mask, actual: Mask },
    Opcode { actual: u8 },
    Control { fin: bool, length: usize },
    Fragment(fragment::Error),
    Payload { expected: usize, actual: usize },
    CloseLength { actual: usize, required: usize },
    CloseCode { actual: u16 },
    Text(fragment::Utf8),
    Allocation(alloc::Error),
    CloseText(fragment::Utf8),
    Changed { expected: usize, actual: usize },
}

impl Fault {
    pub const fn close_code(self) -> u16 {
        self.code() as u16
    }

    pub(crate) const fn code(self) -> Code {
        match self {
            Self::Allocation(_) => Code::Internal,
            Self::Text(_) | Self::CloseText(_) => Code::InvalidPayload,
            Self::Frame(frame::Error::Payload { .. })
            | Self::Fragment(fragment::Error::PayloadTooLarge) => Code::TooLarge,
            _ => Code::Protocol,
        }
    }
}

impl From<fragment::PrepareError> for Fault {
    fn from(error: fragment::PrepareError) -> Self {
        match error {
            fragment::PrepareError::Text(error) => Self::Text(error),
            fragment::PrepareError::Allocation(error) => Self::Allocation(error),
        }
    }
}

impl From<frame::Error> for Fault {
    fn from(error: frame::Error) -> Self {
        Self::Frame(error)
    }
}
