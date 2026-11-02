use sark_http::field;

use crate::upgrade::{Error, FieldError, Need};

#[derive(Clone, Copy)]
enum Part {
    Protocol,
    Version,
}

#[repr(transparent)]
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct State(u8);

impl State {
    const PRESENT: u8 = 1 << 0;
    const WEBSOCKET: u8 = 1 << 1;

    pub(super) fn push(&mut self, value: &[u8]) -> Result<(), Error> {
        let mut next = *self;
        let mut cursor = 0;
        for raw in value.split(|byte| *byte == b',') {
            let ows = raw
                .iter()
                .take_while(|byte| matches!(byte, b' ' | b'\t'))
                .count();
            let item = field::Error::trim_ows(raw);
            let base = cursor + ows;
            cursor += raw.len();
            if cursor < value.len() {
                cursor += 1;
            }
            if item.is_empty() {
                continue;
            }
            let split = item
                .iter()
                .position(|byte| *byte == b'/')
                .map(|index| (&item[..index], &item[index + 1..]));
            let (protocol, version) = match split {
                Some((protocol, version)) => {
                    let start = base + protocol.len() + 1;
                    if let Some(offset) = version.iter().position(|byte| *byte == b'/') {
                        return Err(Error::Field(FieldError::Slash {
                            offset: start + offset,
                        }));
                    }
                    (protocol, Some((version, start)))
                }
                None => (item, None),
            };
            validate(protocol, base, Part::Protocol)?;
            if let Some((version, start)) = version {
                validate(version, start, Part::Version)?;
            }
            next.0 |= Self::PRESENT;
            if item.eq_ignore_ascii_case(b"websocket") {
                next.0 |= Self::WEBSOCKET;
            }
        }
        if next.0 & Self::PRESENT == 0 {
            return Err(Error::Field(FieldError::Missing {
                actual: value.len(),
            }));
        }
        *self = next;
        Ok(())
    }

    pub(super) const fn require(self) -> Result<(), Error> {
        if self.0 & Self::PRESENT == 0 {
            Err(Error::Missing(Need::Upgrade))
        } else if self.0 & Self::WEBSOCKET == 0 {
            Err(Error::Protocol)
        } else {
            Ok(())
        }
    }
}

fn validate(value: &[u8], offset: usize, part: Part) -> Result<(), Error> {
    let bad = value
        .iter()
        .copied()
        .enumerate()
        .find(|(_, byte)| !field::Error::is_name_byte(*byte));
    let (offset, byte) = match bad {
        Some((index, byte)) => (offset + index, Some(byte)),
        None if value.is_empty() => (offset, None),
        None => return Ok(()),
    };
    Err(Error::Field(match part {
        Part::Protocol => FieldError::Protocol { offset, byte },
        Part::Version => FieldError::Version { offset, byte },
    }))
}

const _: () = assert!(core::mem::size_of::<State>() == 1);
