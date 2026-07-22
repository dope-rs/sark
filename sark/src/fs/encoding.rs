#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Encoding {
    Br,
    Gzip,
}

impl Encoding {
    pub(super) fn index(self) -> usize {
        match self {
            Self::Br => 0,
            Self::Gzip => 1,
        }
    }

    pub(super) fn header(self) -> &'static str {
        match self {
            Self::Br => "br",
            Self::Gzip => "gzip",
        }
    }

    pub(super) fn suffix(self) -> &'static str {
        match self {
            Self::Br => ".br",
            Self::Gzip => ".gz",
        }
    }

    fn token(self) -> &'static [u8] {
        match self {
            Self::Br => b"br",
            Self::Gzip => b"gzip",
        }
    }

    pub(super) fn negotiate(
        accept_encoding: &[u8],
        precompressed_br: bool,
        precompressed_gzip: bool,
    ) -> impl Iterator<Item = Encoding> {
        let br = precompressed_br
            .then(|| Self::quality(accept_encoding, Self::Br.token()))
            .flatten();
        let gzip = precompressed_gzip
            .then(|| Self::quality(accept_encoding, Self::Gzip.token()))
            .flatten();

        let ordered = match (br, gzip) {
            (Some(b), Some(g)) if g > b => [Some(Self::Gzip), Some(Self::Br)],
            (Some(_), Some(_)) => [Some(Self::Br), Some(Self::Gzip)],
            (Some(_), None) => [Some(Self::Br), None],
            (None, Some(_)) => [Some(Self::Gzip), None],
            (None, None) => [None, None],
        };
        ordered.into_iter().flatten()
    }

    fn quality(accept_encoding: &[u8], token: &[u8]) -> Option<u32> {
        let mut direct = None;
        let mut star = None;
        for entry in accept_encoding.split(|&byte| byte == b',') {
            let entry = entry.trim_ascii();
            let mut parts = entry.split(|&byte| byte == b';');
            let coding = parts.next().unwrap_or(b"").trim_ascii();
            let mut q = 1000;
            for param in parts {
                let param = param.trim_ascii();
                if param.len() >= 2 && param[0].eq_ignore_ascii_case(&b'q') && param[1] == b'=' {
                    q = Self::parse_quality(&param[2..]);
                }
            }
            if coding.eq_ignore_ascii_case(token) {
                direct = Some(q);
            } else if coding == b"*" {
                star = Some(q);
            }
        }
        match direct.or(star)? {
            0 => None,
            q => Some(q),
        }
    }

    fn parse_quality(value: &[u8]) -> u32 {
        let value = value.trim_ascii();
        let mut parts = value.splitn(2, |&byte| byte == b'.');
        if parts.next().unwrap_or(b"") == b"1" {
            return 1000;
        }
        let mut q = 0;
        let mut scale = 100;
        for &byte in parts.next().unwrap_or(b"").iter().take(3) {
            if !byte.is_ascii_digit() {
                break;
            }
            q += u32::from(byte - b'0') * scale;
            scale /= 10;
        }
        q
    }
}
