use core::{
    mem::{align_of, needs_drop, size_of},
    ops,
};

use o3::base::time;

const DAYS: [[u8; 3]; 7] = [
    *b"Thu", *b"Fri", *b"Sat", *b"Sun", *b"Mon", *b"Tue", *b"Wed",
];
const MONTHS: [[u8; 3]; 12] = [
    *b"Jan", *b"Feb", *b"Mar", *b"Apr", *b"May", *b"Jun", *b"Jul", *b"Aug", *b"Sep", *b"Oct",
    *b"Nov", *b"Dec",
];
const DIGITS: [[u8; 2]; 100] = digits();

/// An IMF-fixdate field value with its wire length encoded in the type.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Stamp([u8; 29]);

impl Stamp {
    pub const LEN: usize = 29;

    /// Formats a Unix timestamp when its civil year fits IMF-fixdate.
    pub const fn from_unix(unix: time::Unix<time::Seconds>) -> Option<Self> {
        let seconds = unix.ticks();
        let days = seconds / 86_400;
        let day = days as i64;
        let (year, month, date) = civil(day);
        if year > 9_999 {
            return None;
        }

        let daytime = seconds % 86_400;
        let hour = (daytime / 3_600) as usize;
        let minute = (daytime % 3_600 / 60) as usize;
        let second = (daytime % 60) as usize;
        let mut bytes = *b"Thu, 01 Jan 1970 00:00:00 GMT";
        write(&mut bytes, 0, &DAYS[(days % 7) as usize]);
        pair(&mut bytes, 5, date as usize);
        // SAFETY: civil's nonnegative era-day calculation yields months 1 through 12.
        let month = unsafe { &*MONTHS.as_ptr().add(month as usize - 1) };
        write(&mut bytes, 8, month);
        pair(&mut bytes, 12, year as usize / 100);
        pair(&mut bytes, 14, year as usize % 100);
        pair(&mut bytes, 17, hour);
        pair(&mut bytes, 20, minute);
        pair(&mut bytes, 23, second);
        Some(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; Self::LEN] {
        &self.0
    }
}

impl ops::Deref for Stamp {
    type Target = [u8; Self::LEN];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

const fn civil(day: i64) -> (i64, u8, u8) {
    let shifted = day + 719_468;
    let era = shifted / 146_097;
    let era_day = shifted - era * 146_097;
    let era_year = (era_day - era_day / 1_460 + era_day / 36_524 - era_day / 146_096) / 365;
    let mut year = era_year + era * 400;
    let year_day = era_day - (365 * era_year + era_year / 4 - era_year / 100);
    let index = (5 * year_day + 2) / 153;
    let date = year_day - (153 * index + 2) / 5 + 1;
    let month = if index < 10 { index + 3 } else { index - 9 };
    if month <= 2 {
        year += 1;
    }
    (year, month as u8, date as u8)
}

const fn pair(bytes: &mut [u8; Stamp::LEN], at: usize, value: usize) {
    // SAFETY: civil dates, daytime remainders, and the admitted four-digit year
    // supply only values below 100. Each call selects a fixed two-byte output slot.
    let pair = unsafe { &*DIGITS.as_ptr().add(value) };
    write(bytes, at, pair);
}

const fn write<const N: usize>(bytes: &mut [u8; Stamp::LEN], at: usize, value: &[u8; N]) {
    // SAFETY: this module's fixed two- and three-byte slots all end at or before
    // byte 25 of the exclusively borrowed 29-byte output. Arrays of u8 align to 1.
    unsafe { bytes.as_mut_ptr().add(at).cast::<[u8; N]>().write(*value) };
}

const fn digits() -> [[u8; 2]; 100] {
    let mut table = [[0; 2]; 100];
    let mut value = 0;
    while value < 100 {
        table[value] = [b'0' + (value / 10) as u8, b'0' + (value % 10) as u8];
        value += 1;
    }
    table
}

const _: () = {
    assert!(size_of::<Stamp>() == Stamp::LEN);
    assert!(align_of::<Stamp>() == 1);
    assert!(!needs_drop::<Stamp>());
};
