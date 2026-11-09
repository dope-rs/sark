pub const CLIENT_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

#[derive(Debug, Default)]
pub struct Prefix(u8);

#[derive(Debug)]
pub struct Complete(());

#[derive(Debug)]
pub enum Read {
    Pending(Prefix),
    Complete(Complete),
}

#[derive(Debug)]
pub struct Mismatch {
    prefix: Prefix,
    offset: u8,
    actual: u8,
}

impl AsRef<[u8]> for Prefix {
    fn as_ref(&self) -> &[u8] {
        &CLIENT_PREFACE[..self.0 as usize]
    }
}

impl Prefix {
    pub fn read(self, bytes: &[u8]) -> Result<(usize, Read), Mismatch> {
        let offset = self.0 as usize;
        let expected = &CLIENT_PREFACE[offset..];
        let count = expected.len().min(bytes.len());
        for (index, (&expected, &actual)) in expected.iter().zip(bytes).enumerate() {
            if actual != expected {
                return Err(Mismatch {
                    prefix: self,
                    offset: (offset + index) as u8,
                    actual,
                });
            }
        }
        let matched = offset + count;
        let read = if matched == CLIENT_PREFACE.len() {
            Read::Complete(Complete(()))
        } else {
            Read::Pending(Self(matched as u8))
        };
        Ok((count, read))
    }
}

impl Mismatch {
    pub fn into_parts(self) -> (Prefix, usize, u8, u8) {
        let offset = self.offset as usize;
        (self.prefix, offset, CLIENT_PREFACE[offset], self.actual)
    }
}
