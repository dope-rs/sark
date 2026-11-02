use core::{num::NonZero, ops, ptr};

use sark_http::field::{
    base64::{self, Decoder, Encoded},
    metadata::{map::record, packed::prefix::Prefix},
    scan::Match,
    text::percent::decoder,
};

use crate::{
    details::{Binary, Fault},
    metadata,
    protocol::section::{
        JOINED, Seal,
        owner::{
            check::{Check, Field},
            fold::{Capacity, Failure, Fold, NameLen},
        },
        part::{End, Part},
        seal,
    },
};

enum Capture {
    None,
    First,
    Second,
}

enum Packing {
    Plain,
    Joined,
}

enum Extent {
    Header,
    Field,
}

enum Join {
    Base64(base64::Error),
    Capacity,
}

/// An admitted field on the exact exclusive owner, consumed by one dispatch.
struct Current<'a, 'lease> {
    fold: &'a mut Fold<'lease>,
    value: ops::Range<usize>,
    name_len: NameLen,
}

pub(super) fn field(fold: &mut Fold<'_>, check: &mut Check, limit: usize) -> Result<(), Failure> {
    let bytes = fold.lease.as_slice();
    // SAFETY: the sole caller's fields loop admits read < limit == lease.len().
    // Field dispatch preserves that extent until finish truncates the lease.
    let bytes = unsafe { bytes.get_unchecked(fold.read..) };
    let prefix = Prefix::try_read(bytes).map_err(Failure::Prefix)?;
    let name_len = NameLen::new(prefix.name_len()).ok_or(Capacity)?;
    let start = fold
        .read
        .checked_add(Prefix::LEN)
        .and_then(|start| start.checked_add(name_len.into_usize()))
        .ok_or(Capacity)?;
    let next = start
        .checked_add(prefix.value_len() as usize)
        .filter(|end| *end <= limit)
        .ok_or(Capacity)?;
    let field = Current {
        fold: &mut *fold,
        value: start..next,
        name_len,
    };
    let kind = check
        .field(
            record::Span::from_slice(field.name()),
            record::Span::from_slice(field.value()),
            prefix.indexing(),
        )
        .map_err(Failure::Timeout)?;
    let seal = &mut check.seal;
    if seal.has_fault() {
        field.fold.read = next;
        return Ok(());
    }
    let result = match kind {
        Field::Ignore | Field::GrpcStatus | Field::Encoding => Ok(()),
        Field::Text | Field::ContentType => match metadata::Metadata::check_ascii(field.value()) {
            Ok(()) => field.copy(Capture::None, seal),
            Err(
                metadata::Error::EmptyName
                | metadata::Error::InvalidName { .. }
                | metadata::Error::ReservedName
                | metadata::Error::EmptyValue
                | metadata::Error::InvalidValue { .. }
                | metadata::Error::Pack(_),
            ) => Ok(()),
        },
        Field::Binary => field.binary(Binary::Metadata, Capture::None, seal),
        Field::Details => field.binary(Binary::Details, Capture::Second, seal),
        Field::Message => field.message(seal),
        Field::Path => field.copy(Capture::First, seal),
        Field::Authority => field.copy(Capture::Second, seal),
        Field::Copy | Field::Method | Field::Scheme | Field::HttpStatus | Field::Te => {
            field.copy(Capture::None, seal)
        }
    };
    fold.read = next;
    result
}

impl Current<'_, '_> {
    fn name(&self) -> &[u8] {
        let name = self.fold.read + Prefix::LEN;
        // SAFETY: admission proves read + Prefix::LEN <= value.start <= lease.len().
        // The exclusive owner cannot resize or change read during this dispatch.
        unsafe {
            self.fold
                .lease
                .as_slice()
                .get_unchecked(name..self.value.start)
        }
    }

    fn value(&self) -> &[u8] {
        // SAFETY: admission retains this exact range in the unchanged exclusive lease.
        unsafe { self.fold.lease.as_slice().get_unchecked(self.value.clone()) }
    }

    fn copy(mut self, capture: Capture, seal: &mut Seal) -> Result<(), Failure> {
        let head_len = Prefix::LEN + self.name_len.into_usize();
        let end = self.fold.written + (self.value.end - self.fold.read);
        self.compact(Extent::Field);
        let range = self.fold.written + head_len..end;
        self.capture(range, capture, seal)
    }

    fn binary(mut self, field: Binary, capture: Capture, seal: &mut Seal) -> Result<(), Failure> {
        if let Some(first) = Match::find(b',', self.value()) {
            match capture {
                Capture::None => {}
                Capture::First | Capture::Second => {
                    seal.fault(Fault::Details);
                    return Ok(());
                }
            }
            let first = first.end();
            self.joined(first).map_err(|error| match error {
                Join::Base64(error) => Failure::Binary { field, error },
                Join::Capacity => Failure::Capacity,
            })?;
            return Ok(());
        }
        let head_len = Prefix::LEN + self.name_len.into_usize();
        let dst = self.fold.written + head_len;
        self.compact(Extent::Header);
        let len = Decoder::new(self.fold.lease.as_mut_slice())
            .decode(self.value.clone(), dst)
            .map_err(|error| Failure::Binary { field, error })?;
        self.prefix(len, Packing::Plain)?;
        let range = dst..dst + len;
        self.capture(range, capture, seal)
    }

    fn joined(mut self, first: NonZero<usize>) -> Result<(), Join> {
        let head_len = Prefix::LEN + self.name_len.into_usize();
        let mut source = self.value.start;
        let mut split = Some(first);
        let mut output = self.fold.written + head_len;
        self.compact(Extent::Header);
        loop {
            let Some(at) = split else {
                let len = Decoder::new(self.fold.lease.as_mut_slice())
                    .decode(source..self.value.end, output)
                    .map_err(Join::Base64)?;
                output += len;
                break;
            };
            let next = source + at.get();
            let comma = next - 1;
            let bytes = self.fold.lease.as_slice();
            // SAFETY: Match admits 0 < at <= value.end - source on this exact tail.
            // The fixed admitted field extent gives source <= comma < next <= lease.len().
            let tail = unsafe { bytes.get_unchecked(next..self.value.end) };
            split = Match::find(b',', tail).map(Match::end);
            let end = match split {
                Some(_) => End::More,
                None => End::Last,
            };
            // SAFETY: the same matched boundary admits source..comma in this unchanged borrow.
            let input = unsafe { bytes.get_unchecked(source..comma) };
            let raw = Encoded::new(input).decoded_len().map_err(Join::Base64)?;
            let part = Part::try_new(raw, end).ok_or(Join::Capacity)?;
            let part_len = part.encoded_len();
            let dst = output + part_len;
            let len = Decoder::new(self.fold.lease.as_mut_slice())
                .decode(source..comma, dst)
                .map_err(Join::Base64)?;
            // SAFETY: successful decoding admits dst within the same exclusive lease,
            // including empty output; the preceding output..dst header therefore fits.
            let prefix = unsafe {
                self.fold
                    .lease
                    .as_mut_slice()
                    .get_unchecked_mut(output..dst)
            };
            part.write(prefix).ok_or(Join::Capacity)?;
            output += part_len + len;
            source = next;
        }
        let len = output
            .checked_sub(self.fold.written + head_len)
            .ok_or(Join::Capacity)?;
        if let Err(Capacity) = self.prefix(len, Packing::Joined) {
            return Err(Join::Capacity);
        }
        self.fold.written = output;
        Ok(())
    }

    fn message(mut self, seal: &mut Seal) -> Result<(), Failure> {
        let head_len = Prefix::LEN + self.name_len.into_usize();
        let dst = self.fold.written + head_len;
        self.compact(Extent::Header);
        let written =
            decoder::Decoder::new(self.fold.lease.as_mut_slice(), self.value.clone(), dst)
                .map(decoder::Decoder::decode)
                .map_err(Failure::Message)?
                .range();
        if let Err(Capacity) = self.prefix(written.len(), Packing::Plain) {
            seal.fault(Fault::Message);
            return Ok(());
        }
        self.capture(written, Capture::First, seal)
    }

    fn compact(&mut self, extent: Extent) {
        let end = match extent {
            Extent::Header => self.value.start,
            Extent::Field => self.value.end,
        };
        let len = end - self.fold.read;
        let bytes = self.fold.lease.as_mut_slice().as_mut_ptr();
        // SAFETY: field() admits read <= value.start <= value.end <= lease.len().
        // Each completed field preserves written <= next read: plain copies retain
        // length, base64 and percent repair shrink, and joined Part+payload fits
        // its source segment plus comma. This consuming dispatch compacts before
        // advancing written; its exclusive owner cannot resize or change read.
        // Hence both ranges fit the same allocation; ptr::copy permits overlap.
        unsafe {
            ptr::copy(bytes.add(self.fold.read), bytes.add(self.fold.written), len);
        }
    }

    fn prefix(&mut self, len: usize, packing: Packing) -> Result<(), Capacity> {
        let Ok(len) = u32::try_from(len) else {
            return Err(Capacity);
        };
        let mut name_len = self.name_len.get();
        match packing {
            Packing::Plain => {}
            Packing::Joined => name_len |= JOINED,
        }
        let bytes = self.fold.lease.as_mut_slice().as_mut_ptr();
        // SAFETY: written <= read and the admitted header has Prefix::LEN >= 8 bytes.
        // Decoding preserves this exclusive lease and writes the prefix before advancing written.
        let bytes = unsafe { &mut *bytes.add(self.fold.written).cast::<[u8; 8]>() };
        bytes[..4].copy_from_slice(&name_len.to_ne_bytes());
        bytes[4..].copy_from_slice(&len.to_ne_bytes());
        Ok(())
    }

    fn capture(
        self,
        range: ops::Range<usize>,
        capture: Capture,
        seal: &mut Seal,
    ) -> Result<(), Failure> {
        match capture {
            Capture::None => {}
            Capture::First => {
                let span = seal::Span::from_range(range.clone()).ok_or(Capacity)?;
                seal.set_first(span);
            }
            Capture::Second => {
                let span = seal::Span::from_range(range.clone()).ok_or(Capacity)?;
                seal.set_second(span);
            }
        }
        self.fold.written = range.end;
        Ok(())
    }
}
