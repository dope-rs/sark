pub mod controls;
pub mod layout;
pub mod numbers;
pub mod payload;

pub const HEADER_LEN: usize = 9;

/// Whether a control frame requires or completes acknowledgment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Ack {
    Required,
    Complete,
}

impl Ack {
    pub(crate) fn from(flags: numbers::Flags) -> Self {
        if flags.contains(numbers::Flags::ACK) {
            Self::Complete
        } else {
            Self::Required
        }
    }

    pub(crate) const fn flags(self) -> u8 {
        match self {
            Self::Required => 0,
            Self::Complete => numbers::Flags::ACK,
        }
    }
}

const _: () = {
    assert!(core::mem::size_of::<Ack>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Ack>() == core::mem::align_of::<bool>());
};

/// Whether one header block continues in another frame.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Block {
    More,
    End,
}

const _: () = {
    assert!(core::mem::size_of::<Block>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Block>() == core::mem::align_of::<bool>());
};
