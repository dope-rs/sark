#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentError {
    UnexpectedNonContinuation,
    UnexpectedContinuation,
    PayloadTooLarge,
}

pub enum Push<'a> {
    Direct(u8, &'a [u8]),
    Assembled(u8, Vec<u8>),
    NeedMore,
}

pub struct FragmentBuffer {
    opcode: Option<u8>,
    payload: Vec<u8>,
    max_payload: usize,
}

impl FragmentBuffer {
    pub fn new(max_payload: usize) -> Self {
        Self {
            opcode: None,
            payload: Vec::new(),
            max_payload,
        }
    }

    pub fn set_max_payload(&mut self, max_payload: usize) -> Result<(), FragmentError> {
        if self.payload.len() > max_payload {
            return Err(FragmentError::PayloadTooLarge);
        }
        self.max_payload = max_payload;
        Ok(())
    }

    pub fn push<'a>(
        &mut self,
        opcode: u8,
        fin: bool,
        payload: &'a [u8],
    ) -> Result<Push<'a>, FragmentError> {
        match opcode {
            0x1 | 0x2 => {
                if self.opcode.is_some() {
                    return Err(FragmentError::UnexpectedNonContinuation);
                }
                if fin {
                    return Ok(Push::Direct(opcode, payload));
                }
                if payload.len() > self.max_payload {
                    return Err(FragmentError::PayloadTooLarge);
                }
                self.opcode = Some(opcode);
                self.payload.extend_from_slice(payload);
                Ok(Push::NeedMore)
            }
            0x0 => {
                let Some(orig) = self.opcode else {
                    return Err(FragmentError::UnexpectedContinuation);
                };
                if self.payload.len().saturating_add(payload.len()) > self.max_payload {
                    return Err(FragmentError::PayloadTooLarge);
                }
                self.payload.extend_from_slice(payload);
                if fin {
                    let assembled = std::mem::take(&mut self.payload);
                    self.opcode = None;
                    Ok(Push::Assembled(orig, assembled))
                } else {
                    Ok(Push::NeedMore)
                }
            }
            _ => Err(FragmentError::UnexpectedNonContinuation),
        }
    }
}
