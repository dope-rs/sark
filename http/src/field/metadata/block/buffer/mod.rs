use o3::{alloc, buffer::write::Sink};

use crate::field::metadata::{
    block::{Name, Named, Scan, Validation, Value, Valued},
    map::record::Span,
};

mod sealed;
pub(super) use sealed::Sealed;

pub type Names = Buffer<Scan>;
pub type Values = Buffer<Value>;

/// The two native metadata scanners accepted by retained buffers.
pub trait Scanner: Sealed + Copy {
    type Proof: Copy;
    const EMPTY: Self;
    fn push(&mut self, byte: u8);
}

impl Scanner for Scan {
    type Proof = Name;
    const EMPTY: Self = Self::new();
    fn push(&mut self, byte: u8) {
        self.push(byte);
    }
}

impl Scanner for Value {
    type Proof = Validation;
    const EMPTY: Self = Self::new();
    fn push(&mut self, byte: u8) {
        self.push(byte);
    }
}

/// Immutable bytes paired with their unfinished metadata scan.
#[derive(Clone, Copy)]
pub struct Prefix<'a, S: Scanner> {
    span: Span<'a>,
    scan: S,
}

impl<'a, S: Scanner> Prefix<'a, S> {
    pub(crate) const fn from_scanned(span: Span<'a>, scan: S) -> Self {
        Self { span, scan }
    }
}

/// Reusable metadata storage that retains classification with its bytes.
pub struct Buffer<S: Scanner> {
    bytes: Vec<u8>,
    scan: S,
    complete: Option<S::Proof>,
}

impl<S: Scanner> Buffer<S> {
    pub const fn new() -> Self {
        Self {
            bytes: Vec::new(),
            scan: S::EMPTY,
            complete: None,
        }
    }

    pub fn clear(&mut self) {
        self.bytes.clear();
        self.scan = S::EMPTY;
        self.complete = None;
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub fn copy_prefix(&mut self, prefix: Prefix<'_, S>) -> Result<(), alloc::Error> {
        self.replace(prefix.span)?;
        self.scan = prefix.scan;
        Ok(())
    }

    fn replace(&mut self, span: Span<'_>) -> Result<(), alloc::Error> {
        self.clear();
        let (first, second) = span.parts();
        self.bytes.write_slices([first, second])
    }

    pub fn writer(&mut self, limit: Option<usize>) -> Writer<'_, S> {
        let mark = (self.bytes.len(), self.scan, self.complete);
        Writer {
            buffer: self,
            mark,
            limit,
            retained: true,
        }
    }
}

impl<S: Scanner> Default for Buffer<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl Buffer<Scan> {
    pub fn named(&self) -> Named<'_> {
        Named::from_scanned(
            Span::from_slice(&self.bytes),
            self.complete.unwrap_or_else(|| self.scan.finish()),
        )
    }

    pub fn copy_from(&mut self, name: Named<'_>) -> Result<(), alloc::Error> {
        self.replace(name.span())?;
        self.complete = Some(name.proof());
        Ok(())
    }
}

impl Buffer<Value> {
    pub fn valued(&self) -> Valued<'_> {
        Valued::from_scanned(
            Span::from_slice(&self.bytes),
            self.complete.unwrap_or_else(|| self.scan.finish()),
        )
    }

    pub fn copy_from(&mut self, value: Valued<'_>) -> Result<(), alloc::Error> {
        self.replace(value.span())?;
        self.complete = Some(value.validation());
        Ok(())
    }
}

/// One append transaction, including its owner-bound rollback checkpoint.
pub struct Writer<'a, S: Scanner> {
    buffer: &'a mut Buffer<S>,
    mark: (usize, S, Option<S::Proof>),
    limit: Option<usize>,
    retained: bool,
}

impl<S: Scanner> Writer<'_, S> {
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub const fn is_retained(&self) -> bool {
        self.retained
    }

    pub fn rollback(self) {
        self.buffer.bytes.truncate(self.mark.0);
        self.buffer.scan = self.mark.1;
        self.buffer.complete = self.mark.2;
    }

    fn reserve(&mut self, extra: Option<usize>) -> Result<bool, alloc::Error> {
        if !self.retained {
            return Ok(false);
        }
        let Some(extra) = extra else {
            self.retained = false;
            return Ok(false);
        };
        let end = self.buffer.len().checked_add(extra);
        if end.is_none_or(|end| self.limit.is_some_and(|limit| end > limit)) {
            self.retained = false;
            return Ok(false);
        }
        alloc::VecExt::try_reserve(&mut self.buffer.bytes, extra)?;
        if extra != 0 && self.buffer.complete.take().is_some() {
            self.buffer.scan = S::EMPTY;
            for &byte in &self.buffer.bytes {
                self.buffer.scan.push(byte);
            }
        }
        Ok(true)
    }

    fn extend(&mut self, bytes: &[u8]) {
        self.buffer.bytes.extend_from_slice(bytes);
        for &byte in bytes {
            self.buffer.scan.push(byte);
        }
    }
}

impl<S: Scanner> Sink for Writer<'_, S> {
    type Error = alloc::Error;

    fn write_slice(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        if self.reserve(Some(bytes.len()))? {
            self.extend(bytes);
        }
        Ok(())
    }

    fn write_slices<const N: usize>(&mut self, slices: [&[u8]; N]) -> Result<(), Self::Error> {
        let extra = slices
            .iter()
            .try_fold(0usize, |len, bytes| len.checked_add(bytes.len()));
        if self.reserve(extra)? {
            for bytes in slices {
                self.extend(bytes);
            }
        }
        Ok(())
    }
}
