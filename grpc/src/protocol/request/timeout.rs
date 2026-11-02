use std::time::Duration;

use sark_http::field::metadata::map::record;

#[derive(Debug)]
pub enum Error {
    Length { actual: usize },
    Digit { index: usize, byte: u8 },
    Unit { byte: u8 },
    Duplicate,
}

#[repr(u8)]
#[derive(Clone, Copy)]
pub(in crate::protocol) enum Unit {
    Absent,
    Hours,
    Minutes,
    Seconds,
    Millis,
    Micros,
    Nanos,
}

#[derive(Clone, Copy)]
pub(in crate::protocol) struct Value {
    pub(in crate::protocol) value: u32,
    pub(in crate::protocol) unit: Unit,
}

impl Value {
    pub(in crate::protocol) const NONE: Self = Self {
        value: 0,
        unit: Unit::Absent,
    };

    /// The length admission bounds every decimal prefix to at most eight digits.
    pub(in crate::protocol) fn parse(input: record::Span<'_>) -> Result<Self, Error> {
        let len = input.len();
        if !(2..=9).contains(&len) {
            return Err(Error::Length { actual: len });
        }
        let mut timeout = Self::NONE;
        for (index, byte) in input.iter().copied().enumerate() {
            if index == len - 1 {
                timeout.unit = match byte {
                    b'H' => Unit::Hours,
                    b'M' => Unit::Minutes,
                    b'S' => Unit::Seconds,
                    b'm' => Unit::Millis,
                    b'u' => Unit::Micros,
                    b'n' => Unit::Nanos,
                    _ => return Err(Error::Unit { byte }),
                };
            } else {
                let digit = match byte {
                    b'0'..=b'9' => u32::from(byte - b'0'),
                    _ => return Err(Error::Digit { index, byte }),
                };
                timeout.value = timeout.value.wrapping_mul(10).wrapping_add(digit);
            }
        }
        Ok(timeout)
    }

    pub(in crate::protocol) fn duration(self) -> Option<Duration> {
        let value = u64::from(self.value);
        Some(match self.unit {
            Unit::Absent => return None,
            Unit::Hours => Duration::from_secs(value * 3_600),
            Unit::Minutes => Duration::from_secs(value * 60),
            Unit::Seconds => Duration::from_secs(value),
            Unit::Millis => Duration::from_millis(value),
            Unit::Micros => Duration::from_micros(value),
            Unit::Nanos => Duration::from_nanos(value),
        })
    }
}
