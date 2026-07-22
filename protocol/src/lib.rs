#![no_std]

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseHeaderNameError {
    Empty,
    InvalidByte { index: usize, byte: u8 },
    Managed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeaderValueError {
    pub index: usize,
}

pub const fn validate_response_header_name(name: &str) -> Result<(), ResponseHeaderNameError> {
    let bytes = name.as_bytes();
    if bytes.is_empty() {
        return Err(ResponseHeaderNameError::Empty);
    }

    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if !is_header_name_byte(byte) {
            return Err(ResponseHeaderNameError::InvalidByte { index, byte });
        }
        index += 1;
    }

    if ascii_eq_ignore_case(bytes, b"date")
        || ascii_eq_ignore_case(bytes, b"server")
        || ascii_eq_ignore_case(bytes, b"content-length")
        || ascii_eq_ignore_case(bytes, b"connection")
        || ascii_eq_ignore_case(bytes, b"transfer-encoding")
    {
        return Err(ResponseHeaderNameError::Managed);
    }

    Ok(())
}

pub const fn validate_header_value(value: &[u8]) -> Result<(), HeaderValueError> {
    let mut index = 0;
    while index < value.len() {
        if value[index] == b'\r' || value[index] == b'\n' {
            return Err(HeaderValueError { index });
        }
        index += 1;
    }
    Ok(())
}

pub const fn is_header_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

const fn ascii_eq_ignore_case(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index].to_ascii_lowercase() != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_rfc_token_names() {
        for name in ["content-type", "x-request_id", "x!#$%&'*+-.^_`|~"] {
            assert_eq!(validate_response_header_name(name), Ok(()));
        }
    }

    #[test]
    fn rejects_empty_and_non_token_names() {
        assert_eq!(
            validate_response_header_name(""),
            Err(ResponseHeaderNameError::Empty)
        );
        assert_eq!(
            validate_response_header_name("x bad"),
            Err(ResponseHeaderNameError::InvalidByte {
                index: 1,
                byte: b' '
            })
        );
        assert_eq!(
            validate_response_header_name("x:bad"),
            Err(ResponseHeaderNameError::InvalidByte {
                index: 1,
                byte: b':'
            })
        );
    }

    #[test]
    fn rejects_managed_names_case_insensitively() {
        for name in [
            "date",
            "Server",
            "CONTENT-LENGTH",
            "Connection",
            "Transfer-Encoding",
        ] {
            assert_eq!(
                validate_response_header_name(name),
                Err(ResponseHeaderNameError::Managed)
            );
        }
    }

    #[test]
    fn rejects_only_line_breaks_in_values() {
        assert_eq!(validate_header_value(b"a\tb"), Ok(()));
        assert_eq!(
            validate_header_value(b"a\nb"),
            Err(HeaderValueError { index: 1 })
        );
        assert_eq!(
            validate_header_value(b"a\rb"),
            Err(HeaderValueError { index: 1 })
        );
    }
}
