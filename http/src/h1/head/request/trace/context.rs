use crate::field::text::IntError;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Context {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
}

impl Context {
    pub fn present(self) -> Option<Self> {
        if self.trace_id == [0; 16] || self.span_id == [0; 8] {
            None
        } else {
            Some(self)
        }
    }

    pub fn parse(value: &[u8]) -> Option<Self> {
        if value.len() < 55
            || value.get(2) != Some(&b'-')
            || value.get(35) != Some(&b'-')
            || value.get(52) != Some(&b'-')
        {
            return None;
        }
        let version = (Self::hex(value[0])? << 4) | Self::hex(value[1])?;
        if version == u8::MAX
            || (version == 0 && value.len() != 55)
            || value.get(55).is_some_and(|byte| *byte != b'-')
        {
            return None;
        }
        let mut context = Self::default();
        Self::decode_hex(
            value.get(3..35)?.as_chunks::<2>().0.try_into().ok()?,
            &mut context.trace_id,
        )
        .ok()?;
        Self::decode_hex(
            value.get(36..52)?.as_chunks::<2>().0.try_into().ok()?,
            &mut context.span_id,
        )
        .ok()?;
        Self::hex(value[53])?;
        Self::hex(value[54])?;
        context.present()
    }

    fn decode_hex<const N: usize>(
        input: &[[u8; 2]; N],
        output: &mut [u8; N],
    ) -> Result<(), IntError> {
        for (&[high, low], byte) in input.iter().zip(output) {
            *byte = (Self::hex(high).ok_or(IntError::InvalidByte)? << 4)
                | Self::hex(low).ok_or(IntError::InvalidByte)?;
        }
        Ok(())
    }

    const fn hex(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }
}
