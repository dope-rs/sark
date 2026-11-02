use core::{marker, mem, num, ptr, slice};

use scan::state;

mod axis;
pub mod ban;
mod error;
mod limit;
pub(in crate::h1) mod scan;
mod size;
mod syntax;
pub(in crate::h1) mod trailer;

pub use axis::Axis;
pub use error::Error;
pub use limit::Limit;
pub use syntax::Syntax;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Boundary {
    More,
    End,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Fragment<'input> {
    More(usize),
    Data {
        consumed: num::NonZero<usize>,
        bytes: &'input [u8],
        boundary: Boundary,
    },
    Trailer(num::NonZero<usize>),
    Complete {
        consumed: num::NonZero<usize>,
        /// The trailer suffix in this input, excluding the terminating empty line.
        /// It can start within a field when earlier input held its prefix.
        /// Use `codec::decode::trailer` to retain complete admitted field lines.
        bytes: &'input [u8],
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Active {
    Size(size::Size),
    SizeLf(size::Size),
    Data(num::NonZero<usize>),
    DataCr,
    DataLf,
    Trailer(state::State),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Framer {
    phase: Option<Active>,
    wire_bytes: usize,
    body_bytes: usize,
}

impl Default for Framer {
    fn default() -> Self {
        Self::new()
    }
}

impl Framer {
    pub const fn new() -> Self {
        Self {
            phase: Some(Active::Size(size::Size::new())),
            wire_bytes: 0,
            body_bytes: 0,
        }
    }

    pub const fn wire_bytes(&self) -> usize {
        self.wire_bytes
    }

    pub const fn body_bytes(&self) -> usize {
        self.body_bytes
    }

    pub fn reset(&mut self) {
        *self = Self::new();
    }

    pub fn scan<'input>(
        &mut self,
        input: &'input [u8],
        limits: &Limit,
    ) -> Result<Run<'input>, Error> {
        let (next, run) = (*self).prepare::<scan::Whole>(input, limits, usize::MAX)?;
        *self = next;
        Ok(run)
    }

    pub fn decode<'input>(
        self,
        input: &'input [u8],
        limits: &Limit,
        data: usize,
    ) -> Result<(Self, Fragment<'input>), Error> {
        let (next, output) = self.prepare::<scan::Fragments>(input, limits, data)?;
        Ok((next, output.fragment()))
    }

    pub(in crate::h1) fn prepare<'input, M: scan::Mode>(
        self,
        input: &'input [u8],
        limits: &Limit,
        data: usize,
    ) -> Result<(Self, M::Output<'input>), Error> {
        let Some(active) = self.phase else {
            return Ok((self, M::finished(input)));
        };
        self.drive::<M>(active, input, limits, data)
    }

    fn drive<'input, M: scan::Mode>(
        mut self,
        mut active: Active,
        input: &'input [u8],
        limits: &Limit,
        data: usize,
    ) -> Result<(Self, M::Output<'input>), Error> {
        let mut pos = 0usize;
        while let Some(&byte) = input.get(pos) {
            match active {
                Active::Size(size) => {
                    let offset = self.wire_bytes;
                    let wire = Self::wire(self.wire_bytes, 1, limits)?;
                    let (size, progress) = size.scan(byte, offset, limits.line_bytes)?;
                    self.wire_bytes = wire;
                    pos += 1;
                    active = if progress == size::Progress::Complete {
                        Active::SizeLf(size)
                    } else {
                        Active::Size(size)
                    };
                }
                Active::SizeLf(size) => {
                    let offset = self.wire_bytes;
                    let wire = Self::wire(self.wire_bytes, 1, limits)?;
                    let size = size.finish(byte, offset, limits.line_bytes)?;
                    let (body, next) = Self::check_size(self.body_bytes, size.value(), limits)?;
                    self.wire_bytes = wire;
                    self.body_bytes = body;
                    pos += 1;
                    active = next;
                }
                Active::Data(left) => {
                    let tail = &input[pos..];
                    let Some(amount) = num::NonZero::new(left.get().min(tail.len()).min(data))
                    else {
                        break;
                    };
                    self.wire_bytes = Self::wire(self.wire_bytes, amount.get(), limits)?;
                    let bytes = &tail[..amount.get()];
                    let consumed = amount.saturating_add(pos);
                    pos = consumed.get();
                    let boundary = match num::NonZero::new(left.get() - amount.get()) {
                        Some(left) => {
                            active = Active::Data(left);
                            Boundary::More
                        }
                        None => {
                            active = Active::DataCr;
                            Boundary::End
                        }
                    };
                    if let Some(output) = M::data(consumed, bytes, boundary) {
                        self.phase = Some(active);
                        return Ok((self, output));
                    }
                }
                Active::DataCr => {
                    self.wire_bytes =
                        Self::exact(self.wire_bytes, byte, b'\r', Syntax::DataCr, limits)?;
                    pos += 1;
                    active = Active::DataLf;
                }
                Active::DataLf => {
                    self.wire_bytes =
                        Self::exact(self.wire_bytes, byte, b'\n', Syntax::DataLf, limits)?;
                    pos += 1;
                    active = Active::Size(size::Size::default());
                }
                Active::Trailer(state) => {
                    let mut trailer = trailer::Trailer::new(state, self.wire_bytes, *limits);
                    let (progress, bytes) = trailer.scan(&input[pos..])?;
                    match progress {
                        trailer::Progress::More { next, consumed } => {
                            self.wire_bytes = next.wire;
                            self.phase = Some(Active::Trailer(next.state));
                            trailer.cursor = next;
                            let output = match consumed {
                                Some(consumed) => {
                                    M::trailer(input, consumed.saturating_add(pos), bytes, trailer)
                                }
                                None => M::pending(input, pos),
                            };
                            return Ok((self, output));
                        }
                        trailer::Progress::Complete { wire, consumed } => {
                            self.wire_bytes = wire;
                            self.phase = None;
                            return Ok((
                                self,
                                M::complete(input, consumed.saturating_add(pos), bytes),
                            ));
                        }
                    }
                }
            }
        }

        self.phase = Some(active);
        Ok((self, M::pending(input, pos)))
    }

    fn wire(current: usize, amount: usize, limits: &Limit) -> Result<usize, Error> {
        let actual = current.checked_add(amount).ok_or(Error::Overflow {
            axis: Axis::Wire,
            value: current,
            added: amount,
        })?;
        if actual > limits.wire_bytes {
            return Err(Error::Limit {
                axis: Axis::Wire,
                actual,
                maximum: limits.wire_bytes,
            });
        }
        Ok(actual)
    }

    fn exact(
        wire: usize,
        byte: u8,
        expected: u8,
        syntax: Syntax,
        limits: &Limit,
    ) -> Result<usize, Error> {
        let offset = wire;
        let actual = Self::wire(wire, 1, limits)?;
        if byte == expected {
            Ok(actual)
        } else {
            Err(Error::Syntax {
                offset,
                syntax,
                actual: byte,
            })
        }
    }

    fn check_size(current: usize, size: usize, limits: &Limit) -> Result<(usize, Active), Error> {
        let total = current.checked_add(size).ok_or(Error::Overflow {
            axis: Axis::Body,
            value: current,
            added: size,
        })?;
        if total > limits.body_bytes {
            return Err(Error::Limit {
                axis: Axis::Body,
                actual: total,
                maximum: limits.body_bytes,
            });
        }
        let active = match num::NonZero::new(size) {
            Some(size) => Active::Data(size),
            None => Active::Trailer(state::State::new()),
        };
        Ok((total, active))
    }
}

const _: () = {
    assert!(mem::size_of::<Boundary>() == mem::size_of::<bool>());
    assert!(mem::align_of::<Boundary>() == mem::align_of::<bool>());
    assert!(mem::size_of::<Framer>() <= 6 * mem::size_of::<usize>());
    assert!(!mem::needs_drop::<Framer>());
};

#[must_use]
pub struct Run<'input> {
    source: ptr::NonNull<u8>,
    state: usize,
    borrow: marker::PhantomData<&'input [u8]>,
}

impl<'input> Run<'input> {
    const COMPLETE: usize = 1 << (usize::BITS - 1);

    /// A scan that stopped before the input ran out, having consumed the
    /// `scanned` bytes of `input` the scan loop read.
    pub(in crate::h1::chunk) fn pending(input: &'input [u8], scanned: usize) -> Self {
        Self {
            source: ptr::NonNull::from(input).cast(),
            state: scanned,
            borrow: marker::PhantomData,
        }
    }

    /// A scan that consumed the whole chunk framing: the `scanned` bytes of
    /// `input` the scan loop read.
    pub(in crate::h1::chunk) fn complete(input: &'input [u8], scanned: usize) -> Self {
        Self {
            source: ptr::NonNull::from(input).cast(),
            state: scanned | Self::COMPLETE,
            borrow: marker::PhantomData,
        }
    }

    pub fn resolve<T>(self, pending: impl FnOnce() -> T, complete: impl FnOnce() -> T) -> T {
        if self.state & Self::COMPLETE == 0 {
            pending()
        } else {
            complete()
        }
    }

    pub fn consume<T>(self, consume: impl FnOnce(&'input [u8]) -> T) -> T {
        let len = self.state & !Self::COMPLETE;
        // SAFETY: constructors retain the full input's provenance and borrow;
        // the scan proves this prefix fits. Slice lengths are at most isize::MAX,
        // so their scanned count cannot set COMPLETE.
        let prefix = unsafe { slice::from_raw_parts(self.source.as_ptr(), len) };
        consume(prefix)
    }
}

const _: () = {
    assert!(mem::size_of::<Run<'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Run<'static>>() == mem::align_of::<usize>());
    assert!(!mem::needs_drop::<Run<'static>>());
};
