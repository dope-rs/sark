pub(super) struct StaticTable;

pub(super) struct StaticMatch {
    pub(super) exact: Option<usize>,
    pub(super) name: usize,
}

pub(super) const ENTRIES: [(&[u8], &[u8]); 61] = [
    (b":authority", b""),
    (b":method", b"GET"),
    (b":method", b"POST"),
    (b":path", b"/"),
    (b":path", b"/index.html"),
    (b":scheme", b"http"),
    (b":scheme", b"https"),
    (b":status", b"200"),
    (b":status", b"204"),
    (b":status", b"206"),
    (b":status", b"304"),
    (b":status", b"400"),
    (b":status", b"404"),
    (b":status", b"500"),
    (b"accept-charset", b""),
    (b"accept-encoding", b"gzip, deflate"),
    (b"accept-language", b""),
    (b"accept-ranges", b""),
    (b"accept", b""),
    (b"access-control-allow-origin", b""),
    (b"age", b""),
    (b"allow", b""),
    (b"authorization", b""),
    (b"cache-control", b""),
    (b"content-disposition", b""),
    (b"content-encoding", b""),
    (b"content-language", b""),
    (b"content-length", b""),
    (b"content-location", b""),
    (b"content-range", b""),
    (b"content-type", b""),
    (b"cookie", b""),
    (b"date", b""),
    (b"etag", b""),
    (b"expect", b""),
    (b"expires", b""),
    (b"from", b""),
    (b"host", b""),
    (b"if-match", b""),
    (b"if-modified-since", b""),
    (b"if-none-match", b""),
    (b"if-range", b""),
    (b"if-unmodified-since", b""),
    (b"last-modified", b""),
    (b"link", b""),
    (b"location", b""),
    (b"max-forwards", b""),
    (b"proxy-authenticate", b""),
    (b"proxy-authorization", b""),
    (b"range", b""),
    (b"referer", b""),
    (b"refresh", b""),
    (b"retry-after", b""),
    (b"server", b""),
    (b"set-cookie", b""),
    (b"strict-transport-security", b""),
    (b"transfer-encoding", b""),
    (b"user-agent", b""),
    (b"vary", b""),
    (b"via", b""),
    (b"www-authenticate", b""),
];

impl StaticTable {
    pub(super) const LEN: usize = 61;

    pub(super) fn get(index: usize) -> Option<(&'static [u8], &'static [u8])> {
        if index == 0 || index > Self::LEN {
            return None;
        }
        Some(ENTRIES[index - 1])
    }

    pub(super) fn lookup(name: &[u8], value: &[u8]) -> Option<StaticMatch> {
        let (start, end) = match name {
            b":authority" => (0, 1),
            b":method" => (1, 3),
            b":path" => (3, 5),
            b":scheme" => (5, 7),
            b":status" => (7, 14),
            b"accept-charset" => (14, 15),
            b"accept-encoding" => (15, 16),
            b"accept-language" => (16, 17),
            b"accept-ranges" => (17, 18),
            b"accept" => (18, 19),
            b"access-control-allow-origin" => (19, 20),
            b"age" => (20, 21),
            b"allow" => (21, 22),
            b"authorization" => (22, 23),
            b"cache-control" => (23, 24),
            b"content-disposition" => (24, 25),
            b"content-encoding" => (25, 26),
            b"content-language" => (26, 27),
            b"content-length" => (27, 28),
            b"content-location" => (28, 29),
            b"content-range" => (29, 30),
            b"content-type" => (30, 31),
            b"cookie" => (31, 32),
            b"date" => (32, 33),
            b"etag" => (33, 34),
            b"expect" => (34, 35),
            b"expires" => (35, 36),
            b"from" => (36, 37),
            b"host" => (37, 38),
            b"if-match" => (38, 39),
            b"if-modified-since" => (39, 40),
            b"if-none-match" => (40, 41),
            b"if-range" => (41, 42),
            b"if-unmodified-since" => (42, 43),
            b"last-modified" => (43, 44),
            b"link" => (44, 45),
            b"location" => (45, 46),
            b"max-forwards" => (46, 47),
            b"proxy-authenticate" => (47, 48),
            b"proxy-authorization" => (48, 49),
            b"range" => (49, 50),
            b"referer" => (50, 51),
            b"refresh" => (51, 52),
            b"retry-after" => (52, 53),
            b"server" => (53, 54),
            b"set-cookie" => (54, 55),
            b"strict-transport-security" => (55, 56),
            b"transfer-encoding" => (56, 57),
            b"user-agent" => (57, 58),
            b"vary" => (58, 59),
            b"via" => (59, 60),
            b"www-authenticate" => (60, 61),
            _ => return None,
        };
        let exact = ENTRIES[start..end]
            .iter()
            .position(|(_, entry_value)| *entry_value == value)
            .map(|offset| start + offset + 1);
        Some(StaticMatch {
            exact,
            name: start + 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{ENTRIES, StaticTable};

    #[test]
    fn lookup_covers_every_static_entry() {
        for (offset, &(name, value)) in ENTRIES.iter().enumerate() {
            let found = StaticTable::lookup(name, value).expect("known name");
            assert_eq!(found.exact, Some(offset + 1));
        }
    }
}
