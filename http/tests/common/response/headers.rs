use sark_http::{
    field::{
        self, head,
        metadata::{
            block::{self, Connect, Gate, Kind, Validation, Version},
            map::record::Span,
        },
    },
    payload::response::{
        Method,
        headers::{Entry, Error, Name, head::Template},
    },
};

const fn entry<'a>(name: &'a [u8], value: &'a [u8]) -> Result<Entry<'a>, Error> {
    let name = match Name::try_bytes(name) {
        Ok(name) => name,
        Err(error) => return Err(error),
    };
    let value = match field::Value::try_bytes(value) {
        Ok(value) => value,
        Err(error) => return Err(Error::Syntax(error)),
    };
    Ok(Entry::new(name, value))
}

pub const OPAQUE: &[u8] = b"\x80\xff\tready";
pub const REPEATED: &[u8] = b"HTTP/1.1 200 OK\r\nSet-Cookie: a=1\r\nSet-Cookie: \x80\xff\tready\r\nContent-Length: 0\r\n\r\n";
pub const TRAILERS: &[u8] = b"HTTP/1.1 200 OK\r\nx-head: head\r\ntransfer-encoding: chunked\r\n\r\n0\r\nX-Tail: \x80\xff\tready\r\n\r\n";
pub const STATUS: &[u8] = b"HTTP/1.1 599 High\r\nContent-Length: 0\r\n\r\n";
pub const EOF_BODY: &[u8] = b"HTTP/1.1 200 OK\r\n\r\nbody";

pub const ASTERISK: &[(head::Name, &[u8])] = &[
    (head::Name::Path, b"*"),
    (head::Name::Scheme, b"https"),
    (head::Name::Authority, b"example.com"),
];

pub const ASTERISK_METHODS: [(Kind, &[u8], Validation); 5] = [
    (Kind::Request, b"OPTIONS", Validation::Valid),
    (Kind::Request, b"GET", Validation::Malformed),
    (Kind::PushPromise, b"GET", Validation::Malformed),
    (Kind::PushPromise, b"HEAD", Validation::Malformed),
    (Kind::Request, b"CONNECT", Validation::Malformed),
];

pub const METHODS: [(&[u8], Method); 4] = [
    (b"HEAD", Method::Head),
    (b"head", Method::Other),
    (b"OPTIONS", Method::Other),
    (b"GET", Method::Other),
];

pub const TARGET: &[(head::Name, &[u8])] = &[
    (head::Name::Path, b"/"),
    (head::Name::Scheme, b"https"),
    (head::Name::Authority, b"example.com"),
];

pub const STANDARD: [&str; 83] = [
    "accept",
    "accept-charset",
    "accept-encoding",
    "accept-language",
    "accept-ranges",
    "access-control-allow-credentials",
    "access-control-allow-headers",
    "access-control-allow-methods",
    "access-control-allow-origin",
    "access-control-expose-headers",
    "access-control-max-age",
    "access-control-request-headers",
    "access-control-request-method",
    "age",
    "allow",
    "alt-svc",
    "authorization",
    "cache-control",
    "cache-status",
    "cdn-cache-control",
    "connection",
    "content-disposition",
    "content-encoding",
    "content-language",
    "content-length",
    "content-location",
    "content-range",
    "content-security-policy",
    "content-security-policy-report-only",
    "content-type",
    "cookie",
    "date",
    "dnt",
    "etag",
    "expect",
    "expires",
    "forwarded",
    "from",
    "host",
    "if-match",
    "if-modified-since",
    "if-none-match",
    "if-range",
    "if-unmodified-since",
    "keep-alive",
    "last-modified",
    "link",
    "location",
    "max-forwards",
    "origin",
    "pragma",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "public-key-pins",
    "public-key-pins-report-only",
    "range",
    "referer",
    "referrer-policy",
    "refresh",
    "retry-after",
    "sec-websocket-accept",
    "sec-websocket-extensions",
    "sec-websocket-key",
    "sec-websocket-protocol",
    "sec-websocket-version",
    "server",
    "set-cookie",
    "strict-transport-security",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "upgrade-insecure-requests",
    "user-agent",
    "vary",
    "via",
    "warning",
    "www-authenticate",
    "x-content-type-options",
    "x-dns-prefetch-control",
    "x-frame-options",
    "x-xss-protection",
];

pub const PRIVATE: &str = "X-Private";

const STATIC_FIELDS: &[Entry<'static>] = &[
    field::literal!(entry(b"content-type", b"text/plain")),
    field::literal!(entry(b"x-static", b"yes")),
];
pub const STATIC: Template = Template::new(STATIC_FIELDS);
pub const EMPTY: Template = Template::new(&[]);
const SEMANTIC_FIELDS: &[Entry<'static>] = &[
    field::literal!(entry(b"x-first", b"one")),
    field::literal!(entry(b"x-second", b" two")),
];
pub const SEMANTIC: Template = Template::new(SEMANTIC_FIELDS);

pub trait Admit: Sized {
    fn h3(connect: Connect, fields: &[(head::Name, &[u8])]) -> (Option<block::Plan>, Validation);
    fn fields(self, fields: &[(head::Name, &[u8])]) -> (Option<block::Plan>, Validation);
    fn response(version: Version, status: Span<'_>) -> block::Outcome;
}

impl Admit for Gate {
    fn h3(connect: Connect, fields: &[(head::Name, &[u8])]) -> (Option<block::Plan>, Validation) {
        Self::new(Kind::Request, connect, Version::Three).fields(fields)
    }

    fn fields(mut self, fields: &[(head::Name, &[u8])]) -> (Option<block::Plan>, Validation) {
        for (name, value) in fields {
            let check = self.accept_name(*name);
            if check != Validation::Valid {
                return (None, check);
            }
            let check = self.accept_value(*name, Span::from_slice(value));
            if check != Validation::Valid {
                return (None, check);
            }
        }
        self.finish().into_parts()
    }

    fn response(version: Version, status: Span<'_>) -> block::Outcome {
        let mut gate = Self::new(Kind::Response, Connect::Disabled, version);
        gate.accept_name(head::Name::Status);
        gate.accept_value(head::Name::Status, status);
        gate.finish()
    }
}
