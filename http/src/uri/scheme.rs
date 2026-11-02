use crate::{field::metadata::map::record, uri::Error};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Scheme {
    None,
    Http,
    Https,
    Other,
}

impl Scheme {
    pub(crate) fn validate(value: &[u8]) -> Result<(), Error> {
        Self::check_span(record::Span::from_slice(value))
    }

    pub fn parse(value: &[u8]) -> Result<Self, Error> {
        Self::parse_span(record::Span::from_slice(value))
    }

    pub fn parse_span(value: record::Span<'_>) -> Result<Self, Error> {
        if value.eq_ascii(b"http") {
            Ok(Self::Http)
        } else if value.eq_ascii(b"https") {
            Ok(Self::Https)
        } else {
            Self::check_span(value)?;
            Ok(Self::Other)
        }
    }

    fn check_span(value: record::Span<'_>) -> Result<(), Error> {
        if !value.get(0).is_some_and(|byte| byte.is_ascii_alphabetic())
            || !value
                .iter()
                .skip(1)
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'+' | b'-' | b'.'))
        {
            return Err(Error::InvalidScheme);
        }
        Ok(())
    }
}
