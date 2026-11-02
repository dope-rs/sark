use std::{convert, num::NonZero, slice};

use o3::buffer::{
    storage::inline,
    write::{self, Sink, consume},
};

pub(crate) enum Output<'a> {
    Inline(inline::Append<'a>),
    Owned(write::Reserved<'a>),
}

impl Output<'_> {
    pub(crate) fn try_write(self, encode: impl FnOnce(&mut Writer<'_>)) -> Result<(), WriteError> {
        match self {
            Self::Inline(output) => {
                output.try_write(|output| Writer::slice(output).try_write(encode))
            }
            Self::Owned(output) => Writer::vector(output.into_staged()).try_write(encode),
        }
    }
}

/// Retains each admitted suffix and its initialized progress until publication.
#[repr(C)]
enum Target<'a> {
    Inline(write::Slice<'a>),
    Owned(write::Staged<'a, usize>),
}

impl Target<'_> {
    fn remaining(&self) -> usize {
        match self {
            Self::Inline(output) => output.remaining(),
            Self::Owned(output) => output.capacity() - output.len(),
        }
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), consume::Error> {
        match self {
            Self::Inline(output) => output.write_slice(bytes),
            Self::Owned(output) => output.write_slice(bytes),
        }
    }
}

pub(crate) struct Writer<'a> {
    target: Target<'a>,
    failed: Option<NonZero<usize>>,
}

#[derive(Debug)]
pub(crate) enum WriteError {
    Write {
        expected: usize,
        written: usize,
        additional: usize,
    },
    Length {
        expected: usize,
        actual: usize,
    },
}

impl<'a> Writer<'a> {
    fn slice(output: &'a mut [u8]) -> Self {
        Self {
            target: Target::Inline(write::Slice::new(output)),
            failed: None,
        }
    }

    fn vector(output: write::Staged<'a, usize>) -> Self {
        Self {
            target: Target::Owned(output),
            failed: None,
        }
    }

    fn try_write(mut self, encode: impl FnOnce(&mut Self)) -> Result<(), WriteError> {
        encode(&mut self);
        match self.target {
            Target::Inline(output) => {
                let left = output.remaining();
                let written = output.finish();
                Self::check(left + written, written, self.failed)
            }
            Target::Owned(output) => {
                Self::check(output.capacity(), output.len(), self.failed)?;
                output.commit();
                Ok(())
            }
        }
    }

    fn check(
        expected: usize,
        actual: usize,
        failed: Option<NonZero<usize>>,
    ) -> Result<(), WriteError> {
        if let Some(added) = failed {
            return Err(WriteError::Write {
                expected,
                written: actual,
                additional: added.get(),
            });
        }
        if actual != expected {
            return Err(WriteError::Length { expected, actual });
        }
        Ok(())
    }

    /// Preserves byte-wise extension's partial write and first failing byte.
    fn write_prefix(&mut self, bytes: &[u8]) {
        if self.failed.is_some() {
            return;
        }
        let len = self.target.remaining().min(bytes.len());
        self.write(&bytes[..len]);
        if len < bytes.len() {
            self.failed = Some(NonZero::<usize>::MIN);
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        if self.failed.is_some() {
            return;
        }
        if let Err(error) = self.target.write(bytes) {
            self.failed = Some(error.into_parts().0);
        }
    }
}

impl Extend<u8> for Writer<'_> {
    fn extend<T: IntoIterator<Item = u8>>(&mut self, iter: T) {
        for byte in iter {
            self.write(slice::from_ref(&byte));
        }
    }
}

impl<'a> Extend<&'a [u8]> for Writer<'_> {
    fn extend<T: IntoIterator<Item = &'a [u8]>>(&mut self, iter: T) {
        for bytes in iter {
            self.write_prefix(bytes);
        }
    }
}

impl Sink for Writer<'_> {
    type Error = convert::Infallible;

    fn write_slice(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.write(bytes);
        Ok(())
    }

    fn write_slices<const N: usize>(&mut self, slices: [&[u8]; N]) -> Result<(), Self::Error> {
        for slice in slices {
            self.write(slice);
        }
        Ok(())
    }
}

const _: () = {
    assert!(size_of::<Output<'_>>() == size_of::<inline::Append<'_>>());
    assert!(size_of::<Writer<'_>>() == 5 * size_of::<usize>());
};
