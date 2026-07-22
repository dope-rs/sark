#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    RsvBitsSet,
    PayloadTooLarge,
    LengthOverflow,
}

pub struct FrameHead {
    pub fin: bool,
    pub opcode: u8,
    pub mask: Option<[u8; 4]>,
    pub payload_start: usize,
    pub payload_end: usize,
}

impl FrameHead {
    pub fn parse(
        buf: &[u8],
        start: usize,
        max_payload: usize,
    ) -> Result<Option<FrameHead>, FrameError> {
        if buf.len() < start + 2 {
            return Ok(None);
        }
        let b0 = buf[start];
        let b1 = buf[start + 1];
        if (b0 & 0x70) != 0 {
            return Err(FrameError::RsvBitsSet);
        }
        let fin = (b0 & 0x80) != 0;
        let opcode = b0 & 0x0f;
        let masked = (b1 & 0x80) != 0;

        let mut offset = start + 2;
        let payload_len = match (b1 & 0x7f) as usize {
            n @ 0..=125 => n,
            126 => {
                if buf.len() < offset + 2 {
                    return Ok(None);
                }
                let len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
                offset += 2;
                len
            }
            127 => {
                if buf.len() < offset + 8 {
                    return Ok(None);
                }
                let raw = u64::from_be_bytes([
                    buf[offset],
                    buf[offset + 1],
                    buf[offset + 2],
                    buf[offset + 3],
                    buf[offset + 4],
                    buf[offset + 5],
                    buf[offset + 6],
                    buf[offset + 7],
                ]);
                offset += 8;
                if raw & 0x8000_0000_0000_0000 != 0 {
                    return Err(FrameError::LengthOverflow);
                }
                usize::try_from(raw).map_err(|_| FrameError::LengthOverflow)?
            }
            _ => unreachable!(),
        };

        if payload_len > max_payload {
            return Err(FrameError::PayloadTooLarge);
        }

        let mask = if masked {
            if buf.len() < offset + 4 {
                return Ok(None);
            }
            let m = [
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ];
            offset += 4;
            Some(m)
        } else {
            None
        };

        let payload_end = offset
            .checked_add(payload_len)
            .ok_or(FrameError::LengthOverflow)?;
        if buf.len() < payload_end {
            return Ok(None);
        }

        Ok(Some(FrameHead {
            fin,
            opcode,
            mask,
            payload_start: offset,
            payload_end,
        }))
    }

    pub const fn header_len(payload_len: usize) -> usize {
        if payload_len <= 125 {
            2
        } else if payload_len <= u16::MAX as usize {
            4
        } else {
            10
        }
    }

    pub fn encode_header_into(
        dst: &mut [u8],
        opcode: u8,
        payload_len: usize,
        masked: bool,
    ) -> usize {
        dst[0] = 0x80 | (opcode & 0x0f);
        let mask_bit: u8 = if masked { 0x80 } else { 0 };
        if payload_len <= 125 {
            dst[1] = mask_bit | payload_len as u8;
            2
        } else if payload_len <= u16::MAX as usize {
            dst[1] = mask_bit | 126;
            dst[2..4].copy_from_slice(&(payload_len as u16).to_be_bytes());
            4
        } else {
            dst[1] = mask_bit | 127;
            dst[2..10].copy_from_slice(&(payload_len as u64).to_be_bytes());
            10
        }
    }

    pub fn encode_len(out: &mut Vec<u8>, payload_len: usize, masked: bool) {
        let mask_bit: u8 = if masked { 0x80 } else { 0 };
        if payload_len <= 125 {
            out.push(mask_bit | payload_len as u8);
        } else if payload_len <= u16::MAX as usize {
            out.push(mask_bit | 126);
            out.extend_from_slice(&(payload_len as u16).to_be_bytes());
        } else {
            out.push(mask_bit | 127);
            out.extend_from_slice(&(payload_len as u64).to_be_bytes());
        }
    }
}
