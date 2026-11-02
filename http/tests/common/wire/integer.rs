pub const VALUES: &[u64] = &[
    0,
    1,
    30,
    31,
    32,
    126,
    127,
    128,
    255,
    256,
    u32::MAX as u64,
    u64::MAX,
];

pub const TRUNCATED: &[u8] = &[0x1f, 0x80];
pub const OVERFLOW: &[u8] = &[
    0x1f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
];
