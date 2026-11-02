mod scalar;
mod simd;

use core::ops;

use crate::field::base64::{Encoded, Error, shape::Shape};

pub(super) struct View<'a> {
    bytes: &'a mut [u8],
    origin: usize,
    destination: usize,
    shape: Shape,
    mode: Mode,
}

enum Mode {
    Forward,
    Backward,
    Overlap(usize),
}

#[derive(Clone, Copy)]
pub(super) struct Progress {
    read: usize,
    written: usize,
}

impl<'a> View<'a> {
    pub(super) fn try_new(
        bytes: &'a mut [u8],
        source: ops::Range<usize>,
        dst: usize,
    ) -> Result<Self, Error> {
        let cap = bytes.len();
        let origin = source.start;
        let stop = source.end;
        let input = bytes.get(source).ok_or(Error::Source {
            start: origin,
            end: stop,
            capacity: cap,
        })?;
        let shape = Encoded::new(input).shape()?;
        let len = shape.decoded();
        let end = dst.checked_add(len).ok_or(Error::Output {
            start: dst,
            len,
            capacity: cap,
        })?;
        if end > cap {
            return Err(Error::Output {
                start: dst,
                len,
                capacity: cap,
            });
        }
        let mode = if dst <= origin {
            Mode::Forward
        } else {
            let groups = dst - origin;
            if groups < shape.groups() {
                Mode::Overlap(groups)
            } else {
                Mode::Backward
            }
        };
        Ok(Self {
            bytes,
            origin,
            destination: dst,
            shape,
            mode,
        })
    }
    pub(super) fn commit(mut self) -> usize {
        let shape = self.shape;
        match self.mode {
            Mode::Forward => {
                let progress = simd::decode(&mut self);
                scalar::forward(&mut self, progress.read, progress.written, shape);
            }
            Mode::Backward => scalar::backward(&mut self, shape),
            Mode::Overlap(groups) => {
                let prefix = Shape::full(groups);
                scalar::backward(&mut self, prefix);
                let read = self.origin + prefix.symbols();
                let written = self.destination + prefix.decoded();
                scalar::forward(&mut self, read, written, shape);
            }
        }
        shape.decoded()
    }

    fn read<const N: usize>(&self, at: usize) -> [u8; N] {
        // SAFETY: View admits the exact source and Shape; each scalar group,
        // tail, or SIMD block stays within its remaining symbols on this owner.
        unsafe { self.bytes.as_ptr().add(at).cast::<[u8; N]>().read() }
    }

    fn write<const N: usize>(&mut self, at: usize, value: [u8; N]) {
        // SAFETY: View admits the complete decoded extent on this exclusive
        // owner. The selected direction reads each overlapping group before writing.
        unsafe {
            self.bytes
                .as_mut_ptr()
                .add(at)
                .cast::<[u8; N]>()
                .write(value)
        };
    }
}

impl Progress {
    pub(super) const fn new(read: usize, written: usize) -> Self {
        Self { read, written }
    }
}

const _: () = {
    assert!(core::mem::size_of::<Mode>() == 2 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<View<'static>>() == 8 * core::mem::size_of::<usize>());
};
