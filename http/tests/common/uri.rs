use std::num::NonZero;

use sark_http::uri::Error;

pub const NAMES: &[(&[u8], &[u8], bool)] = &[
    (b"a", b"%41", true),
    (b"%2f", b"%2F", true),
    (b"!", b"%21", false),
    (b"127.0.0.1", b"%31%32%37.0.0.1", true),
    (b"127.0.0.1", b"%31%32%37.0.0.2", false),
    (b"127.0.0.1", b"127.0.0.01", false),
    (b"one.example", b"two.example", false),
];

pub const PORTS: [(&[u8], Option<u16>); 4] = [
    (b"example.test", None),
    (b"example.test:", None),
    (b"example.test:0", Some(0)),
    (b"[::1]:065535", Some(u16::MAX)),
];
pub const PORT_OVERFLOW: &[u8] = b"example.test:65536";

pub const IPV6: &[(&[u8], [u16; 8])] = &[
    (b"[::]", [0; 8]),
    (b"[1::]", [1, 0, 0, 0, 0, 0, 0, 0]),
    (b"[2001:db8::1]", [0x2001, 0xdb8, 0, 0, 0, 0, 0, 1]),
    (b"[1::2:3:4:5:6:7]", [1, 0, 2, 3, 4, 5, 6, 7]),
    (b"[1:2:3:4:5:6:7:8]", [1, 2, 3, 4, 5, 6, 7, 8]),
    (
        b"[::ffff:192.0.2.1]",
        [0, 0, 0, 0, 0, 0xffff, 0xc000, 0x201],
    ),
    (
        b"[1:2:3:4:5:6:192.0.2.1]",
        [1, 2, 3, 4, 5, 6, 0xc000, 0x201],
    ),
];

pub const REJECTED_IPV6: &[&[u8]] = &[
    b"[1::2::3]",
    b"[1:2:3:4:5:6:7:8::]",
    b"[1:2:3:4:5:6:7:8:9]",
    b"[1:2:3:4:5:6:7:192.0.2.1]",
    b"[::ffff:192.0.2.1.1]",
];

pub const REJECTED_HOSTS: &[(&str, Option<NonZero<u16>>, Error)] = &[
    ("bad host", None, Error::InvalidHost),
    ("bad:host", None, Error::InvalidHost),
    ("", Some(NonZero::<u16>::MIN), Error::InvalidHost),
    ("[::1]:80", Some(NonZero::<u16>::MIN), Error::InvalidPort),
];

pub const AUTHORITY_ONLY: &str = "http:80";

pub const BASE: &str = "https://example.test/a/b?old";
pub const RELATIVE: &str = "../c?new";

pub const RESOLVED: &[(&str, &str, &str)] = &[
    (BASE, "", BASE),
    (BASE, "?new", "https://example.test/a/b?new"),
    (BASE, "/fixed?", "https://example.test/fixed?"),
    (BASE, ".", "https://example.test/a/"),
    (BASE, "..", "https://example.test/"),
    (BASE, "../../../c?", "https://example.test/c?"),
    (BASE, "c//d/../e", "https://example.test/a/c//e"),
    (BASE, "/a//b/../c", "https://example.test/a//c"),
    (BASE, "/a//..", "https://example.test/a/"),
    (BASE, "/a/.", "https://example.test/a/"),
    (BASE, "/a/../..", "https://example.test/"),
    (
        BASE,
        "/%2e/%2E%2E/%2F/./c?x=/../",
        "https://example.test/%2e/%2E%2E/%2F/c?x=/../",
    ),
    (
        "HTTPS://[2001:db8::1]:0443/a/../b?old",
        "?new",
        "HTTPS://[2001:db8::1]:0443/a/../b?new",
    ),
    (
        "HTTPS://[2001:db8::1]:0443/a/../b?old",
        "c",
        "HTTPS://[2001:db8::1]:0443/c",
    ),
    (
        "https://example.test//a/",
        "../b",
        "https://example.test//b",
    ),
    ("https://example.test/", ".", "https://example.test/"),
];
