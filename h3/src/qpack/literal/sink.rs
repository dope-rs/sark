use o3::buffer::write;

pub(super) enum Error<E> {
    Limit {
        written: usize,
        additional: usize,
        maximum: usize,
    },
    Output(E),
}

pub(super) struct Limited<'a, W> {
    output: &'a mut W,
    written: usize,
    max: usize,
}

impl<'a, W> Limited<'a, W> {
    pub(super) const fn new(output: &'a mut W, max: usize) -> Self {
        Self {
            output,
            written: 0,
            max,
        }
    }
}

impl<W: write::Sink> write::Sink for &mut Limited<'_, W> {
    type Error = Error<W::Error>;

    fn write_byte(&mut self, byte: u8) -> Result<(), Self::Error> {
        if self.written == self.max {
            return Err(Error::Limit {
                written: self.written,
                additional: 1,
                maximum: self.max,
            });
        }
        self.output.write_byte(byte).map_err(Error::Output)?;
        self.written += 1;
        Ok(())
    }

    fn write_slice(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        let end = self
            .written
            .checked_add(bytes.len())
            .filter(|end| *end <= self.max)
            .ok_or(Error::Limit {
                written: self.written,
                additional: bytes.len(),
                maximum: self.max,
            })?;
        self.output.write_slice(bytes).map_err(Error::Output)?;
        self.written = end;
        Ok(())
    }

    fn write_slices<const N: usize>(&mut self, slices: [&[u8]; N]) -> Result<(), Self::Error> {
        let mut end = self.written;
        for slice in &slices {
            end = end.checked_add(slice.len()).ok_or(Error::Limit {
                written: end,
                additional: slice.len(),
                maximum: self.max,
            })?;
        }
        if end > self.max {
            return Err(Error::Limit {
                written: self.written,
                additional: end - self.written,
                maximum: self.max,
            });
        }
        self.output.write_slices(slices).map_err(Error::Output)?;
        self.written = end;
        Ok(())
    }
}
