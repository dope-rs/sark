pub const MAX_SECONDS: u64 = u64::MAX / 1_000_000_000;

pub const CASES: [(u64, &[u8; 29]); 4] = [
    (0, b"Thu, 01 Jan 1970 00:00:00 GMT"),
    (951_782_400, b"Tue, 29 Feb 2000 00:00:00 GMT"),
    (2_147_483_647, b"Tue, 19 Jan 2038 03:14:07 GMT"),
    (253_402_300_799, b"Fri, 31 Dec 9999 23:59:59 GMT"),
];

pub fn reference(seconds: u64) -> [u8; 29] {
    const DAYS: [&[u8; 3]; 7] = [b"Thu", b"Fri", b"Sat", b"Sun", b"Mon", b"Tue", b"Wed"];
    const MONTHS: [&[u8; 3]; 12] = [
        b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov",
        b"Dec",
    ];

    let epoch = seconds / 86_400;
    let daytime = seconds % 86_400;
    let mut days = epoch;
    let mut year = 1970_u64;
    loop {
        let span = if is_leap(year) { 366 } else { 365 };
        if days < span {
            break;
        }
        days -= span;
        year += 1;
    }
    let lengths = [
        31,
        if is_leap(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 0;
    while days >= lengths[month] {
        days -= lengths[month];
        month += 1;
    }

    let mut bytes = *b"Thu, 01 Jan 1970 00:00:00 GMT";
    bytes[..3].copy_from_slice(DAYS[(epoch % 7) as usize]);
    pair(&mut bytes, 5, days + 1);
    bytes[8..11].copy_from_slice(MONTHS[month]);
    pair(&mut bytes, 12, year / 100);
    pair(&mut bytes, 14, year % 100);
    pair(&mut bytes, 17, daytime / 3_600);
    pair(&mut bytes, 20, daytime % 3_600 / 60);
    pair(&mut bytes, 23, daytime % 60);
    bytes
}

fn pair(bytes: &mut [u8; 29], at: usize, value: u64) {
    bytes[at] = b'0' + (value / 10) as u8;
    bytes[at + 1] = b'0' + (value % 10) as u8;
}

fn is_leap(year: u64) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}
