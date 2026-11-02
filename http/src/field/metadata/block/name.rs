use crate::field::{
    head,
    metadata::{block, map::record},
};

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Name(head::Name);

impl Default for Name {
    fn default() -> Self {
        Self::REGULAR
    }
}

impl Name {
    pub const REGULAR: Self = Self(head::Name::Regular);
    pub const INVALID: Self = Self(head::Name::Invalid);
    pub(crate) const COMPACT: [Self; 18] = [
        Self(head::Name::Invalid),
        Self(head::Name::Method),
        Self(head::Name::Scheme),
        Self(head::Name::Path),
        Self(head::Name::Status),
        Self(head::Name::Authority),
        Self(head::Name::Protocol),
        Self(head::Name::Host),
        Self(head::Name::Connection),
        Self(head::Name::ContentLength),
        Self(head::Name::TransferEncoding),
        Self(head::Name::Expect),
        Self(head::Name::AcceptEncoding),
        Self(head::Name::Te),
        Self(head::Name::KeepAlive),
        Self(head::Name::ProxyConnection),
        Self(head::Name::Upgrade),
        Self(head::Name::Regular),
    ];

    pub const fn from_head(name: head::Name) -> Self {
        Self(name)
    }

    pub const fn head(self) -> head::Name {
        self.0
    }

    pub const fn compact(self) -> u8 {
        self.0 as u8
    }

    pub const fn from_compact(value: u8) -> Option<Self> {
        if (value as usize) < Self::COMPACT.len() {
            Some(Self::COMPACT[value as usize])
        } else {
            None
        }
    }

    pub const fn is_forbidden(self) -> bool {
        matches!(
            self.0,
            head::Name::Invalid
                | head::Name::Connection
                | head::Name::TransferEncoding
                | head::Name::KeepAlive
                | head::Name::ProxyConnection
                | head::Name::Upgrade
        )
    }

    pub fn from_span(name: record::Span<'_>) -> Self {
        let mut scan = block::Scan::new();
        for byte in name.iter().copied() {
            scan.push(byte);
        }
        scan.finish()
    }

    pub const fn classify(name: &[u8]) -> Self {
        let mut scan = block::Scan::new();
        let mut index = 0;
        while index < name.len() {
            scan.push(name[index]);
            index += 1;
        }
        scan.finish()
    }
}

const _: () = assert!(core::mem::size_of::<Name>() == 1);
