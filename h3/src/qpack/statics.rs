//! The fixed QPACK field index.
use sark_http::field::{
    self,
    metadata::{
        block::table,
        map::record::{self, Entry},
    },
};

/// Names and value validity are admitted once by the shared HTTP table owner.
pub(crate) const STATIC: Table<'_> = Table(field::literal!(table::Table::try_new(ENTRIES)));

pub(crate) struct Table<'a>(table::Table<'a, { ENTRIES.len() }>);

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Index(u64);

impl Index {
    fn from_usize(index: usize) -> Self {
        Self(index as u64)
    }

    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    Reference { index: u64 },
}

const ENTRIES: [(&[u8], &[u8]); 99] = [
    (b":authority", b""),
    (b":path", b"/"),
    (b"age", b"0"),
    (b"content-disposition", b""),
    (b"content-length", b"0"),
    (b"cookie", b""),
    (b"date", b""),
    (b"etag", b""),
    (b"if-modified-since", b""),
    (b"if-none-match", b""),
    (b"last-modified", b""),
    (b"link", b""),
    (b"location", b""),
    (b"referer", b""),
    (b"set-cookie", b""),
    (b":method", b"CONNECT"),
    (b":method", b"DELETE"),
    (b":method", b"GET"),
    (b":method", b"HEAD"),
    (b":method", b"OPTIONS"),
    (b":method", b"POST"),
    (b":method", b"PUT"),
    (b":scheme", b"http"),
    (b":scheme", b"https"),
    (b":status", b"103"),
    (b":status", b"200"),
    (b":status", b"304"),
    (b":status", b"404"),
    (b":status", b"503"),
    (b"accept", b"*/*"),
    (b"accept", b"application/dns-message"),
    (b"accept-encoding", b"gzip, deflate, br"),
    (b"accept-ranges", b"bytes"),
    (b"access-control-allow-headers", b"cache-control"),
    (b"access-control-allow-headers", b"content-type"),
    (b"access-control-allow-origin", b"*"),
    (b"cache-control", b"max-age=0"),
    (b"cache-control", b"max-age=2592000"),
    (b"cache-control", b"max-age=604800"),
    (b"cache-control", b"no-cache"),
    (b"cache-control", b"no-store"),
    (b"cache-control", b"public, max-age=31536000"),
    (b"content-encoding", b"br"),
    (b"content-encoding", b"gzip"),
    (b"content-type", b"application/dns-message"),
    (b"content-type", b"application/javascript"),
    (b"content-type", b"application/json"),
    (b"content-type", b"application/x-www-form-urlencoded"),
    (b"content-type", b"image/gif"),
    (b"content-type", b"image/jpeg"),
    (b"content-type", b"image/png"),
    (b"content-type", b"text/css"),
    (b"content-type", b"text/html; charset=utf-8"),
    (b"content-type", b"text/plain"),
    (b"content-type", b"text/plain;charset=utf-8"),
    (b"range", b"bytes=0-"),
    (b"strict-transport-security", b"max-age=31536000"),
    (
        b"strict-transport-security",
        b"max-age=31536000; includesubdomains",
    ),
    (
        b"strict-transport-security",
        b"max-age=31536000; includesubdomains; preload",
    ),
    (b"vary", b"accept-encoding"),
    (b"vary", b"origin"),
    (b"x-content-type-options", b"nosniff"),
    (b"x-xss-protection", b"1; mode=block"),
    (b":status", b"100"),
    (b":status", b"204"),
    (b":status", b"206"),
    (b":status", b"302"),
    (b":status", b"400"),
    (b":status", b"403"),
    (b":status", b"421"),
    (b":status", b"425"),
    (b":status", b"500"),
    (b"accept-language", b""),
    (b"access-control-allow-credentials", b"FALSE"),
    (b"access-control-allow-credentials", b"TRUE"),
    (b"access-control-allow-headers", b"*"),
    (b"access-control-allow-methods", b"get"),
    (b"access-control-allow-methods", b"get, post, options"),
    (b"access-control-allow-methods", b"options"),
    (b"access-control-expose-headers", b"content-length"),
    (b"access-control-request-headers", b"content-type"),
    (b"access-control-request-method", b"get"),
    (b"access-control-request-method", b"post"),
    (b"alt-svc", b"clear"),
    (b"authorization", b""),
    (
        b"content-security-policy",
        b"script-src 'none'; object-src 'none'; base-uri 'none'",
    ),
    (b"early-data", b"1"),
    (b"expect-ct", b""),
    (b"forwarded", b""),
    (b"if-range", b""),
    (b"origin", b""),
    (b"purpose", b"prefetch"),
    (b"server", b""),
    (b"timing-allow-origin", b"*"),
    (b"upgrade-insecure-requests", b"1"),
    (b"user-agent", b""),
    (b"x-forwarded-for", b""),
    (b"x-frame-options", b"deny"),
    (b"x-frame-options", b"sameorigin"),
];

impl<'a> Table<'a> {
    fn find(rows: &[(&[u8], &[u8])], value: &[u8], start: usize) -> Option<Index> {
        let offset = rows.iter().position(|row| row.1 == value)?;
        Some(Index::from_usize(start + offset))
    }

    pub(crate) fn resolve(&self, index: u64) -> Result<table::Entry<'_, 'a>, Error> {
        let raw = index;
        if raw > usize::MAX as u64 {
            return Err(Error::Reference { index: raw });
        }
        self.0
            .get(raw as usize)
            .ok_or(Error::Reference { index: raw })
    }

    pub(crate) fn lookup(&self, field: Entry<'_>) -> Option<record::Match<Index, Index>> {
        let (start, end, next, limit) = match field.name {
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
        let direct = self.0.entries().get(start..end)?;
        let extra = self.0.entries().get(next..limit)?;
        let exact = match Self::find(direct, field.value, start) {
            Some(index) => Some(index),
            None => Self::find(extra, field.value, next),
        };
        Some(record::Match {
            exact,
            name: Index::from_usize(start),
        })
    }
}

const _: () =
    assert!(core::mem::size_of::<table::Entry<'_, '_>>() <= 2 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Index>() == core::mem::size_of::<u64>());
