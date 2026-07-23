use super::{HeaderNameOutcome, HeaderValueOutcome};

pub(super) fn scan_header_name(bytes: &[u8], start: usize) -> HeaderNameOutcome {
    let mut idx = start;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if byte == b':' || byte == b'\r' {
            return HeaderNameOutcome::Found { pos: idx, byte };
        }
        if !sark_protocol::is_header_name_byte(byte) {
            return HeaderNameOutcome::Invalid;
        }
        idx += 1;
    }
    HeaderNameOutcome::None
}

pub(super) fn scan_header_value(bytes: &[u8], start: usize) -> HeaderValueOutcome {
    let mut idx = start;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if byte == b'\r' {
            if idx + 1 == bytes.len() {
                return HeaderValueOutcome::None;
            }
            return if bytes[idx + 1] == b'\n' {
                HeaderValueOutcome::Found { pos: idx }
            } else {
                HeaderValueOutcome::Invalid
            };
        }
        if (byte < 0x20 && byte != b'\t') || byte == 0x7f {
            return HeaderValueOutcome::Invalid;
        }
        idx += 1;
    }
    HeaderValueOutcome::None
}

pub(super) fn request_target_is_valid(bytes: &[u8]) -> bool {
    !bytes.iter().any(|&byte| byte <= 0x20 || byte == 0x7f)
}
