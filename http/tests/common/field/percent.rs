pub const REPAIRED: &[(&[u8], &str)] = &[
    (b"", ""),
    (b"%E2%28%A1", "?(?"),
    (b"%E0%80%80", "???"),
    (b"%F0%9F", "??"),
    (b"\xE2literal", "?literal"),
    (b"%G0", "%G0"),
    (b"\xE2\x98\x83", "\u{2603}"),
    (b"plain%20text", "plain text"),
];

pub const WINDOWS: &[(usize, usize)] = &[(0, 0), (5, 1), (5, 5)];
