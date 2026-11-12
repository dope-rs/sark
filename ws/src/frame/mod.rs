use std::num;

use o3::buffer::{
    storage,
    write::{Sink, consume, spans},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Reserved { bits: u8 },
    NonCanonical { marker: u8, length: u64 },
    Payload { actual: usize, maximum: usize },
    Length { value: u64 },
    Offset { start: usize, amount: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanError {
    Opcode { actual: u8 },
    Control { fin: bool, length: usize },
    Length { payload: usize, masked: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindError {
    Payload { expected: usize, actual: usize },
    Mask { expected: bool, actual: bool },
    Capacity { required: usize, available: usize },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Head {
    pub fin: bool,
    pub opcode: u8,
    pub mask: Option<[u8; 4]>,
    start: num::NonZero<usize>,
    payload_end: num::NonZero<usize>,
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: num::NonZero<usize>,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8], start: usize) -> Result<Option<(Self, &'a [u8; 2])>, Error> {
        let offset = num::NonZero::<usize>::MIN
            .saturating_add(1)
            .checked_add(start)
            .ok_or(Error::Offset { start, amount: 2 })?;
        let Some(head) = bytes.get(start..).and_then(|tail| tail.first_chunk::<2>()) else {
            return Ok(None);
        };
        Ok(Some((Self { bytes, offset }, head)))
    }

    fn take<const N: usize>(&mut self) -> Result<Option<&'a [u8; N]>, Error> {
        let end = self.offset.checked_add(N).ok_or(Error::Offset {
            start: self.offset.get(),
            amount: N,
        })?;
        let Some(bytes) = self
            .bytes
            .get(self.offset.get()..)
            .and_then(|tail| tail.first_chunk::<N>())
        else {
            return Ok(None);
        };
        self.offset = end;
        Ok(Some(bytes))
    }
}

pub const MAX_CONTROL_PAYLOAD: usize = 125;
pub const MAX_SERVER_CONTROL_WIRE: usize = 2 + MAX_CONTROL_PAYLOAD;

/// Exact immutable wire plan shared by direct server writes and owned client
/// frames. Opcode, control shape, and length admission happen once; encoding
/// retains those proofs and never grows a buffer.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Plan {
    opcode: u8,
    fin: bool,
    payload_len: usize,
    masked: bool,
    wire_len: usize,
    head: u8,
}

const _: () = assert!(size_of::<Plan>() == 3 * size_of::<usize>());

#[derive(Clone, Copy)]
pub struct Frame<'a> {
    plan: Plan,
    payload: &'a [u8],
    mask: Option<[u8; 4]>,
}

pub struct Writer<'a, 'p> {
    frame: Frame<'p>,
    dst: &'a mut [u8],
}

/// A masked frame sequence retaining its admitted payload and header widths.
#[derive(Clone, Copy)]
pub(crate) struct Frames<'a> {
    data: &'a [u8],
    chunk: num::NonZero<usize>,
    opcode: u8,
    head: u8,
    last: u8,
    done: bool,
}

pub(crate) struct Masked<'a> {
    plan: Plan,
    payload: &'a [u8],
}

impl<'a> Frames<'a> {
    pub(crate) fn new(
        opcode: u8,
        data: &'a [u8],
        chunk: num::NonZero<usize>,
    ) -> Result<Self, PlanError> {
        let full = data.len().min(chunk.get());
        let last = if data.len() <= chunk.get() {
            full
        } else {
            (data.len() - 1) % chunk.get() + 1
        };
        let head = Plan::new(opcode, data.len() <= chunk.get(), full, true)?.head;
        let last = if last == full {
            head
        } else {
            // The continuation is no larger than the admitted first frame, so
            // its header and payload fit within the same wire-length proof.
            Plan::head(last)
        };
        Ok(Self {
            data,
            chunk,
            opcode,
            head,
            last,
            done: false,
        })
    }

    pub(crate) fn lengths(&self) -> (num::NonZero<usize>, usize, usize) {
        let full = self.data.len().min(self.chunk.get());
        let wire = full + usize::from(self.head) + 4;
        if self.data.len() <= self.chunk.get() {
            return (num::NonZero::<usize>::MIN, wire, wire);
        }
        let count =
            num::NonZero::<usize>::MIN.saturating_add((self.data.len() - 1) / self.chunk.get());
        let last = (self.data.len() - 1) % self.chunk.get() + 1;
        (count, wire, last + usize::from(self.last) + 4)
    }

    pub(crate) const fn opcode(&self) -> u8 {
        self.opcode
    }
}

impl<'a> Iterator for Frames<'a> {
    type Item = Masked<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let (payload, tail) = self.data.split_at(self.data.len().min(self.chunk.get()));
        let fin = tail.is_empty();
        let head = if fin { self.last } else { self.head };
        let plan = Plan {
            opcode: self.opcode,
            fin,
            payload_len: payload.len(),
            masked: true,
            wire_len: payload.len() + usize::from(head) + 4,
            head,
        };
        self.data = tail;
        self.opcode = 0;
        self.done = fin;
        Some(Masked { plan, payload })
    }
}

impl<'a> Masked<'a> {
    pub(crate) fn bind(self, mask: [u8; 4]) -> Frame<'a> {
        Frame {
            plan: self.plan,
            payload: self.payload,
            mask: Some(mask),
        }
    }
}

const _: () = assert!(size_of::<Frames<'_>>() == 4 * size_of::<usize>());

impl Plan {
    pub fn new(opcode: u8, fin: bool, len: usize, masked: bool) -> Result<Self, PlanError> {
        match opcode {
            0x0..=0x2 => {}
            0x8..=0xa => {
                if !fin || len > MAX_CONTROL_PAYLOAD {
                    return Err(PlanError::Control { fin, length: len });
                }
            }
            _ => return Err(PlanError::Opcode { actual: opcode }),
        }
        let head = Self::head(len);
        let prefix = usize::from(head) + if masked { 4 } else { 0 };
        let wire = prefix.checked_add(len).ok_or(PlanError::Length {
            payload: len,
            masked,
        })?;
        Ok(Self {
            opcode,
            fin,
            payload_len: len,
            masked,
            wire_len: wire,
            head,
        })
    }

    pub const fn wire_len(self) -> usize {
        self.wire_len
    }

    pub(crate) fn capacity(payload: num::NonZero<u32>) -> num::NonZero<usize> {
        let payload = num::NonZero::<usize>::MIN.saturating_add(payload.get() as usize - 1);
        payload.saturating_add(usize::from(Self::head(payload.get())))
    }

    fn head(len: usize) -> u8 {
        if len <= 125 {
            2
        } else if len <= usize::from(u16::MAX) {
            4
        } else {
            10
        }
    }

    pub fn bind<'a>(self, data: &'a [u8], mask: Option<[u8; 4]>) -> Result<Frame<'a>, BindError> {
        if data.len() != self.payload_len {
            return Err(BindError::Payload {
                expected: self.payload_len,
                actual: data.len(),
            });
        }
        if mask.is_some() != self.masked {
            return Err(BindError::Mask {
                expected: self.masked,
                actual: mask.is_some(),
            });
        }
        Ok(Frame {
            plan: self,
            payload: data,
            mask,
        })
    }
}

impl<'p> Frame<'p> {
    /// Server responses supply fixed text/binary opcodes, a two-byte close, or
    /// an admitted inbound control request. Those callers retain the opcode
    /// and control-payload proofs; every response is final and unmasked.
    pub(crate) fn server(opcode: u8, payload: &'p [u8]) -> Self {
        let len = payload.len();
        let head = Plan::head(len);
        Self {
            plan: Plan {
                opcode,
                fin: true,
                payload_len: len,
                masked: false,
                wire_len: usize::from(head) + len,
                head,
            },
            payload,
            mask: None,
        }
    }

    pub(crate) const fn wire_len(self) -> usize {
        self.plan.wire_len
    }

    pub fn bind<'a>(self, dst: &'a mut [u8]) -> Result<Writer<'a, 'p>, BindError> {
        if dst.len() < self.plan.wire_len {
            return Err(BindError::Capacity {
                required: self.plan.wire_len,
                available: dst.len(),
            });
        }
        Ok(Writer { frame: self, dst })
    }

    /// Builds one exact-capacity shared owner. A masked payload is read and
    /// written once; freezing transfers the same allocation into egress.
    pub fn try_owned(self) -> Result<storage::Shared, storage::BuildError<consume::Error>> {
        storage::Exact::try_build(self.plan.wire_len, |out| self.write_into(out))
            .map(storage::Exact::into_shared)
    }

    pub(crate) fn write_into(self, out: &mut spans::Writer<'_, '_>) -> Result<(), consume::Error> {
        let (prefix, used) = self.prefix();
        // SAFETY: prefix emits at most fourteen initialized bytes from the
        // retained Plan, whose admitted header width and mask remain unchanged.
        out.write_slice(unsafe { prefix.get_unchecked(..used) })?;
        match self.mask {
            Some(mask) => out.try_init(&crate::mask::Key::new(mask).initializer(self.payload))?,
            None => out.write_slice(self.payload)?,
        }
        Ok(())
    }

    pub(crate) fn prefix(self) -> ([u8; 14], usize) {
        let mut dst = [0; 14];
        let head = usize::from(self.plan.head);
        let len = self.plan.payload_len;
        dst[0] = if self.plan.fin {
            0x80 | self.plan.opcode
        } else {
            self.plan.opcode
        };
        let bit = if self.plan.masked { 0x80 } else { 0 };
        let wire = (len as u64).to_be_bytes();
        if head == 2 {
            dst[1] = bit | wire[7];
        } else if head == 4 {
            dst[1] = bit | 126;
            dst[2..4].copy_from_slice(&wire[6..]);
        } else {
            dst[1] = bit | 127;
            dst[2..10].copy_from_slice(&wire);
        }
        let used = head + if self.plan.masked { 4 } else { 0 };
        if let Some(mask) = self.mask {
            // SAFETY: every Frame constructor preserves head in {2, 4, 10}
            // and mask presence equal to plan.masked, so these four bytes fit.
            unsafe { dst.get_unchecked_mut(head..head + 4) }.copy_from_slice(&mask);
        }
        (dst, used)
    }
}

impl Writer<'_, '_> {
    pub fn encode(self) -> usize {
        let len = self.frame.plan.wire_len;
        self.write();
        len
    }

    fn write(self) {
        let (prefix, used) = self.frame.prefix();
        // SAFETY: bind admits this exact destination against the
        // retained Frame. Its complete wire length includes this header.
        let (header, tail) = unsafe { self.dst.split_at_mut_unchecked(used) };
        // SAFETY: prefix emits at most fourteen bytes from the admitted Plan.
        header.copy_from_slice(unsafe { prefix.get_unchecked(..used) });
        // SAFETY: all Frame constructors retain the exact payload, and its
        // length plus used equals the wire length admitted by this Writer.
        let output = unsafe { tail.get_unchecked_mut(..self.frame.payload.len()) };
        match self.frame.mask {
            Some(mask) => {
                crate::mask::Key::new(mask).copy(output, self.frame.payload);
            }
            None => output.copy_from_slice(self.frame.payload),
        }
    }
}

impl Head {
    pub fn parse(buf: &[u8], start: usize, max: usize) -> Result<Option<Head>, Error> {
        let Some((mut reader, &[b0, b1])) = Reader::new(buf, start)? else {
            return Ok(None);
        };
        if (b0 & 0x70) != 0 {
            return Err(Error::Reserved { bits: b0 & 0x70 });
        }
        let fin = (b0 & 0x80) != 0;
        let opcode = b0 & 0x0f;
        let masked = (b1 & 0x80) != 0;

        let marker = b1 & 0x7f;
        let len = match marker {
            126 => {
                let Some(&[high, low]) = reader.take::<2>()? else {
                    return Ok(None);
                };
                let len = usize::from(u16::from_be_bytes([high, low]));
                if len < 126 {
                    return Err(Error::NonCanonical {
                        marker,
                        length: len as u64,
                    });
                }
                len
            }
            127 => {
                let Some(&[a, b, c, d, e, f, g, h]) = reader.take::<8>()? else {
                    return Ok(None);
                };
                let raw = u64::from_be_bytes([a, b, c, d, e, f, g, h]);
                if raw & 0x8000_0000_0000_0000 != 0 {
                    return Err(Error::Length { value: raw });
                }
                if raw <= u64::from(u16::MAX) {
                    return Err(Error::NonCanonical {
                        marker,
                        length: raw,
                    });
                }
                let Ok(len) = usize::try_from(raw) else {
                    return Err(Error::Length { value: raw });
                };
                len
            }
            short => usize::from(short),
        };

        if len > max {
            return Err(Error::Payload {
                actual: len,
                maximum: max,
            });
        }

        let mask = if masked {
            let Some(&[a, b, c, d]) = reader.take::<4>()? else {
                return Ok(None);
            };
            Some([a, b, c, d])
        } else {
            None
        };

        let offset = reader.offset;
        let end = offset.checked_add(len).ok_or(Error::Offset {
            start: offset.get(),
            amount: len,
        })?;
        Ok(Some(Head {
            fin,
            opcode,
            mask,
            start: offset,
            payload_end: end,
        }))
    }

    pub const fn start(&self) -> num::NonZero<usize> {
        self.start
    }

    pub const fn payload_end(&self) -> num::NonZero<usize> {
        self.payload_end
    }

    pub fn payload_len(&self) -> usize {
        self.payload_end.get() - self.start.get()
    }
}

const _: () = {
    assert!(size_of::<Head>() == 3 * size_of::<usize>());
    assert!(size_of::<Option<Head>>() == size_of::<Head>());
    assert!(size_of::<Reader<'static>>() == 3 * size_of::<usize>());
};
