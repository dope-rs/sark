#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageFrame {
    pub compressed: bool,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameError {
    BadCompressionFlag(u8),
    MessageTooLarge { len: usize, max: usize },
    LengthOverflow,
}

impl MessageFrame {
    pub fn encode(compressed: bool, payload: &[u8], out: &mut Vec<u8>) -> Result<(), FrameError> {
        let len = u32::try_from(payload.len()).map_err(|_| FrameError::LengthOverflow)?;
        out.push(u8::from(compressed));
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(payload);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Deframer {
    max_message_len: usize,
    header: [u8; 5],
    header_len: usize,
    body: Vec<u8>,
    needed: usize,
    compressed: bool,
}

impl Deframer {
    pub fn new(max_message_len: usize) -> Self {
        Self {
            max_message_len,
            header: [0; 5],
            header_len: 0,
            body: Vec::new(),
            needed: 0,
            compressed: false,
        }
    }

    pub fn push(
        &mut self,
        mut bytes: &[u8],
        out: &mut Vec<MessageFrame>,
    ) -> Result<usize, FrameError> {
        let original = bytes.len();
        while !bytes.is_empty() {
            if self.header_len < self.header.len() {
                let n = (self.header.len() - self.header_len).min(bytes.len());
                self.header[self.header_len..self.header_len + n].copy_from_slice(&bytes[..n]);
                self.header_len += n;
                bytes = &bytes[n..];
                if self.header_len < self.header.len() {
                    break;
                }
                self.begin_body()?;
                if self.needed == 0 {
                    out.push(MessageFrame {
                        compressed: self.compressed,
                        payload: Vec::new(),
                    });
                    self.reset_header();
                }
            }

            if self.header_len == self.header.len() && self.needed > 0 {
                let n = (self.needed - self.body.len()).min(bytes.len());
                self.body.extend_from_slice(&bytes[..n]);
                bytes = &bytes[n..];
                if self.body.len() == self.needed {
                    out.push(MessageFrame {
                        compressed: self.compressed,
                        payload: core::mem::take(&mut self.body),
                    });
                    self.reset_header();
                }
            }
        }
        Ok(original - bytes.len())
    }

    fn begin_body(&mut self) -> Result<(), FrameError> {
        self.compressed = match self.header[0] {
            0 => false,
            1 => true,
            other => return Err(FrameError::BadCompressionFlag(other)),
        };
        let len = u32::from_be_bytes([
            self.header[1],
            self.header[2],
            self.header[3],
            self.header[4],
        ]) as usize;
        if len > self.max_message_len {
            return Err(FrameError::MessageTooLarge {
                len,
                max: self.max_message_len,
            });
        }
        self.needed = len;
        self.body.clear();
        Ok(())
    }

    fn reset_header(&mut self) {
        self.header_len = 0;
        self.needed = 0;
        self.compressed = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_single_message() {
        let mut bytes = Vec::new();
        MessageFrame::encode(false, b"abc", &mut bytes).unwrap();

        let mut deframer = Deframer::new(16);
        let mut out = Vec::new();
        let n = deframer.push(&bytes, &mut out).unwrap();

        assert_eq!(n, bytes.len());
        assert_eq!(
            out,
            vec![MessageFrame {
                compressed: false,
                payload: b"abc".to_vec()
            }]
        );
    }

    #[test]
    fn handles_fragmented_prefix_and_body() {
        let mut bytes = Vec::new();
        MessageFrame::encode(true, b"abcdef", &mut bytes).unwrap();

        let mut deframer = Deframer::new(16);
        let mut out = Vec::new();
        assert_eq!(deframer.push(&bytes[..2], &mut out).unwrap(), 2);
        assert!(out.is_empty());
        assert_eq!(deframer.push(&bytes[2..7], &mut out).unwrap(), 5);
        assert!(out.is_empty());
        assert_eq!(
            deframer.push(&bytes[7..], &mut out).unwrap(),
            bytes.len() - 7
        );
        assert!(out[0].compressed);
        assert_eq!(out[0].payload, b"abcdef");
    }

    #[test]
    fn rejects_oversized_message() {
        let mut bytes = Vec::new();
        MessageFrame::encode(false, b"abcd", &mut bytes).unwrap();
        let mut deframer = Deframer::new(3);
        let mut out = Vec::new();
        assert!(matches!(
            deframer.push(&bytes, &mut out),
            Err(FrameError::MessageTooLarge { len: 4, max: 3 })
        ));
    }
}
