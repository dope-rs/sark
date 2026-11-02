#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Field {
    ContentLength,
    TransferEncoding,
    Host,
    Trailer,
    Connection,
    KeepAlive,
    Te,
    Upgrade,
    ProxyAuthenticate,
    ProxyAuthorization,
}

impl Field {
    const VALUES: [Self; 10] = [
        Self::ContentLength,
        Self::TransferEncoding,
        Self::Host,
        Self::Trailer,
        Self::Connection,
        Self::KeepAlive,
        Self::Te,
        Self::Upgrade,
        Self::ProxyAuthenticate,
        Self::ProxyAuthorization,
    ];

    pub(in crate::h1) const NAMES: [&'static [u8]; 10] = [
        b"content-length",
        b"transfer-encoding",
        b"host",
        b"trailer",
        b"connection",
        b"keep-alive",
        b"te",
        b"upgrade",
        b"proxy-authenticate",
        b"proxy-authorization",
    ];

    pub(in crate::h1::chunk) fn get(set: u16, len: usize) -> Option<Self> {
        if set.count_ones() != 1 {
            return None;
        }
        let slot = set.trailing_zeros() as usize;
        let name = Self::NAMES.get(slot)?;
        let field = Self::VALUES.get(slot).copied()?;
        (name.len() == len).then_some(field)
    }
}

const _: () = {
    assert!(Field::NAMES[0].len() == 14);
    assert!(Field::NAMES[1].len() == 17);
    assert!(Field::NAMES[2].len() == 4);
    assert!(Field::NAMES[3].len() == 7);
    assert!(Field::NAMES[4].len() == 10);
    assert!(Field::NAMES[5].len() == 10);
    assert!(Field::NAMES[6].len() == 2);
    assert!(Field::NAMES[7].len() == 7);
    assert!(Field::NAMES[8].len() == 18);
    assert!(Field::NAMES[9].len() == 19);
};
