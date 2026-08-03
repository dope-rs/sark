use sark_core::http::{Field, KnownHeadName};

pub struct StaticTable;

pub(crate) struct StaticMatch {
    pub(crate) exact: Option<u64>,
    pub(crate) name: u64,
}

const ENTRIES: &[(&str, &str)] = &[
    (":authority", ""),
    (":path", "/"),
    ("age", "0"),
    ("content-disposition", ""),
    ("content-length", "0"),
    ("cookie", ""),
    ("date", ""),
    ("etag", ""),
    ("if-modified-since", ""),
    ("if-none-match", ""),
    ("last-modified", ""),
    ("link", ""),
    ("location", ""),
    ("referer", ""),
    ("set-cookie", ""),
    (":method", "CONNECT"),
    (":method", "DELETE"),
    (":method", "GET"),
    (":method", "HEAD"),
    (":method", "OPTIONS"),
    (":method", "POST"),
    (":method", "PUT"),
    (":scheme", "http"),
    (":scheme", "https"),
    (":status", "103"),
    (":status", "200"),
    (":status", "304"),
    (":status", "404"),
    (":status", "503"),
    ("accept", "*/*"),
    ("accept", "application/dns-message"),
    ("accept-encoding", "gzip, deflate, br"),
    ("accept-ranges", "bytes"),
    ("access-control-allow-headers", "cache-control"),
    ("access-control-allow-headers", "content-type"),
    ("access-control-allow-origin", "*"),
    ("cache-control", "max-age=0"),
    ("cache-control", "max-age=2592000"),
    ("cache-control", "max-age=604800"),
    ("cache-control", "no-cache"),
    ("cache-control", "no-store"),
    ("cache-control", "public, max-age=31536000"),
    ("content-encoding", "br"),
    ("content-encoding", "gzip"),
    ("content-type", "application/dns-message"),
    ("content-type", "application/javascript"),
    ("content-type", "application/json"),
    ("content-type", "application/x-www-form-urlencoded"),
    ("content-type", "image/gif"),
    ("content-type", "image/jpeg"),
    ("content-type", "image/png"),
    ("content-type", "text/css"),
    ("content-type", "text/html; charset=utf-8"),
    ("content-type", "text/plain"),
    ("content-type", "text/plain;charset=utf-8"),
    ("range", "bytes=0-"),
    ("strict-transport-security", "max-age=31536000"),
    (
        "strict-transport-security",
        "max-age=31536000; includesubdomains",
    ),
    (
        "strict-transport-security",
        "max-age=31536000; includesubdomains; preload",
    ),
    ("vary", "accept-encoding"),
    ("vary", "origin"),
    ("x-content-type-options", "nosniff"),
    ("x-xss-protection", "1; mode=block"),
    (":status", "100"),
    (":status", "204"),
    (":status", "206"),
    (":status", "302"),
    (":status", "400"),
    (":status", "403"),
    (":status", "421"),
    (":status", "425"),
    (":status", "500"),
    ("accept-language", ""),
    ("access-control-allow-credentials", "FALSE"),
    ("access-control-allow-credentials", "TRUE"),
    ("access-control-allow-headers", "*"),
    ("access-control-allow-methods", "get"),
    ("access-control-allow-methods", "get, post, options"),
    ("access-control-allow-methods", "options"),
    ("access-control-expose-headers", "content-length"),
    ("access-control-request-headers", "content-type"),
    ("access-control-request-method", "get"),
    ("access-control-request-method", "post"),
    ("alt-svc", "clear"),
    ("authorization", ""),
    (
        "content-security-policy",
        "script-src 'none'; object-src 'none'; base-uri 'none'",
    ),
    ("early-data", "1"),
    ("expect-ct", ""),
    ("forwarded", ""),
    ("if-range", ""),
    ("origin", ""),
    ("purpose", "prefetch"),
    ("server", ""),
    ("timing-allow-origin", "*"),
    ("upgrade-insecure-requests", "1"),
    ("user-agent", ""),
    ("x-forwarded-for", ""),
    ("x-frame-options", "deny"),
    ("x-frame-options", "sameorigin"),
];

impl StaticTable {
    pub const LEN: u64 = ENTRIES.len() as u64;

    pub fn get(index: u64) -> Option<Field<'static>> {
        let (name, value) = ENTRIES.get(usize::try_from(index).ok()?)?;
        Some(Field::new(name.as_bytes(), value.as_bytes()))
    }

    pub fn name(index: u64) -> Option<&'static [u8]> {
        let (name, _) = ENTRIES.get(usize::try_from(index).ok()?)?;
        Some(name.as_bytes())
    }

    pub(crate) const fn known_name(index: u64) -> KnownHeadName {
        match index {
            0 => KnownHeadName::Authority,
            1 => KnownHeadName::Path,
            4 => KnownHeadName::ContentLength,
            15..=21 => KnownHeadName::Method,
            22..=23 => KnownHeadName::Scheme,
            24..=28 | 63..=71 => KnownHeadName::Status,
            _ => KnownHeadName::Regular,
        }
    }

    pub fn find(field: Field<'_>) -> Option<u64> {
        Self::lookup(field).and_then(|found| found.exact)
    }

    pub fn find_name(name: &[u8]) -> Option<u64> {
        Self::lookup(Field::new(name, b"")).map(|found| found.name)
    }

    pub(crate) fn lookup(field: Field<'_>) -> Option<StaticMatch> {
        let (first_start, first_end, second_start, second_end) = match field.name {
            b":authority" => (0, 1, 0, 0),
            b":path" => (1, 2, 0, 0),
            b":method" => (15, 22, 0, 0),
            b":scheme" => (22, 24, 0, 0),
            b":status" => (24, 29, 63, 72),
            b"age" => (2, 3, 0, 0),
            b"content-disposition" => (3, 4, 0, 0),
            b"content-length" => (4, 5, 0, 0),
            b"cookie" => (5, 6, 0, 0),
            b"date" => (6, 7, 0, 0),
            b"etag" => (7, 8, 0, 0),
            b"if-modified-since" => (8, 9, 0, 0),
            b"if-none-match" => (9, 10, 0, 0),
            b"last-modified" => (10, 11, 0, 0),
            b"link" => (11, 12, 0, 0),
            b"location" => (12, 13, 0, 0),
            b"referer" => (13, 14, 0, 0),
            b"set-cookie" => (14, 15, 0, 0),
            b"accept" => (29, 31, 0, 0),
            b"accept-encoding" => (31, 32, 0, 0),
            b"accept-ranges" => (32, 33, 0, 0),
            b"access-control-allow-headers" => (33, 35, 75, 76),
            b"access-control-allow-origin" => (35, 36, 0, 0),
            b"cache-control" => (36, 42, 0, 0),
            b"content-encoding" => (42, 44, 0, 0),
            b"content-type" => (44, 55, 0, 0),
            b"range" => (55, 56, 0, 0),
            b"strict-transport-security" => (56, 59, 0, 0),
            b"vary" => (59, 61, 0, 0),
            b"x-content-type-options" => (61, 62, 0, 0),
            b"x-xss-protection" => (62, 63, 0, 0),
            b"accept-language" => (72, 73, 0, 0),
            b"access-control-allow-credentials" => (73, 75, 0, 0),
            b"access-control-allow-methods" => (76, 79, 0, 0),
            b"access-control-expose-headers" => (79, 80, 0, 0),
            b"access-control-request-headers" => (80, 81, 0, 0),
            b"access-control-request-method" => (81, 83, 0, 0),
            b"alt-svc" => (83, 84, 0, 0),
            b"authorization" => (84, 85, 0, 0),
            b"content-security-policy" => (85, 86, 0, 0),
            b"early-data" => (86, 87, 0, 0),
            b"expect-ct" => (87, 88, 0, 0),
            b"forwarded" => (88, 89, 0, 0),
            b"if-range" => (89, 90, 0, 0),
            b"origin" => (90, 91, 0, 0),
            b"purpose" => (91, 92, 0, 0),
            b"server" => (92, 93, 0, 0),
            b"timing-allow-origin" => (93, 94, 0, 0),
            b"upgrade-insecure-requests" => (94, 95, 0, 0),
            b"user-agent" => (95, 96, 0, 0),
            b"x-forwarded-for" => (96, 97, 0, 0),
            b"x-frame-options" => (97, 99, 0, 0),
            _ => return None,
        };
        let exact = ENTRIES[first_start..first_end]
            .iter()
            .position(|(_, value)| value.as_bytes() == field.value)
            .map(|offset| (first_start + offset) as u64)
            .or_else(|| {
                ENTRIES[second_start..second_end]
                    .iter()
                    .position(|(_, value)| value.as_bytes() == field.value)
                    .map(|offset| (second_start + offset) as u64)
            });
        Some(StaticMatch {
            exact,
            name: first_start as u64,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{ENTRIES, StaticTable};
    use sark_core::http::Field;

    #[test]
    fn lookup_covers_every_static_entry() {
        for (index, &(name, value)) in ENTRIES.iter().enumerate() {
            let found = StaticTable::lookup(Field::new(name.as_bytes(), value.as_bytes()))
                .expect("known name");
            assert_eq!(found.exact, Some(index as u64));
        }
    }
}
