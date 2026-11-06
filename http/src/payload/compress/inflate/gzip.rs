use std::{convert::Infallible, mem};

use miniz_oxide::inflate::{TINFLStatus, core::DecompressorOxide};
use o3::buffer::{
    resident,
    storage::Owned,
    write::{self, SpareExt as _},
};

use crate::payload::{
    compress::inflate::{
        Checked, DecodeError, Error, Failure, Part, Plan,
        checked::STEP,
        input::{Chunk, Input, Step},
    },
    crc32::Hasher,
};

#[derive(Default)]
pub struct Gzip(DecompressorOxide, Hasher);

const _: () = assert!(
    mem::size_of::<Gzip>() == mem::size_of::<DecompressorOxide>() + mem::size_of::<usize>()
);

impl Gzip {
    /// Validates a member header using this decoder's retained CPU admission.
    pub fn try_plan<'a, I: Iterator<Item = &'a [u8]>>(
        &self,
        segments: I,
        hint: usize,
    ) -> Result<Plan<'a, I>, Failure> {
        Plan::try_header::<false, Infallible>(segments, hint, Some(&self.1), &mut || Ok(()))
            .map_err(Checked::into_work)
    }

    /// Admits the header with checkpoints during bounded scans and CRC updates.
    pub fn plan_with<'a, I: Iterator<Item = &'a [u8]>, E>(
        &self,
        segments: I,
        hint: usize,
        mut check: impl FnMut() -> Result<(), E>,
    ) -> Result<Plan<'a, I>, Checked<Failure, E>> {
        check().map_err(Checked::Check)?;
        Plan::try_header::<true, E>(segments, hint, Some(&self.1), &mut check)
    }

    pub fn decode<'a, I>(&mut self, segments: I, output: write::Spare<'_>) -> Result<(), Failure>
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let plan = self.try_plan(segments, 0)?;
        self.decode_exact(plan.input, output)
    }

    pub fn decode_plan<'a, I: Iterator<Item = &'a [u8]>>(
        &mut self,
        plan: Plan<'a, I>,
        output: write::Spare<'_>,
    ) -> Result<(), Failure> {
        if output.len() < plan.size_hint {
            return Err(Failure::new(
                Error::Length {
                    expected: plan.size_hint,
                    actual: output.len(),
                },
                0,
            ));
        }
        self.decode_exact(plan.input, output)
    }

    fn decode_exact<'a, I>(
        &mut self,
        input: Input<'a, I>,
        mut output: write::Spare<'_>,
    ) -> Result<(), Failure>
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let written = self
            .decode_input::<false, _, _, Failure, Infallible, _>(
                input,
                &mut output.as_mut_slice(),
                |output, _| {
                    let capacity = output.len();
                    Err(Failure::new(
                        Error::Output {
                            capacity,
                            written: capacity,
                        },
                        capacity,
                    )
                    .into())
                },
                &mut || Ok(()),
            )
            .map_err(Checked::into_work)?;
        if written != output.len() {
            return Err(Failure::new(
                Error::Length {
                    expected: output.len(),
                    actual: written,
                },
                written,
            ));
        }
        output.commit();
        Ok(())
    }

    fn decode_input<'a, const CHECK: bool, I, B, D, E, F>(
        &mut self,
        input: Input<'a, I>,
        output: &mut B,
        mut grow: impl FnMut(&mut B, &mut F) -> Result<(), Checked<D, E>>,
        check: &mut F,
    ) -> Result<usize, Checked<D, E>>
    where
        I: Iterator<Item = &'a [u8]>,
        B: AsMut<[u8]>,
        D: From<Failure>,
        F: FnMut() -> Result<(), E>,
    {
        let (mut segments, mut input) = input.into_parts();
        let mut written = 0;
        let mut start = 0;
        loop {
            self.0.init();
            let mut checksum = self.1.clone();
            let mut trailer = {
                let mut cursor = input;
                loop {
                    if CHECK {
                        check().map_err(Checked::Check)?;
                    }
                    let step =
                        Chunk::next::<CHECK, E>(cursor, &mut segments, check).map_err(|error| {
                            error.map_work(|cause| D::from(Failure::new(cause, written)))
                        })?;
                    let chunk = match step {
                        Step::Chunk(chunk) => chunk,
                        Step::End(end) => {
                            return Err(Failure::new(end.error(Part::Deflate), written).into());
                        }
                    };
                    let bytes = output.as_mut();
                    // SAFETY: the two private callers retain a fixed slice (growth fails)
                    // or grow its owner. Admitted writes and completed members preserve
                    // start <= written <= bytes.len().
                    let member = unsafe { bytes.get_unchecked_mut(start..) };
                    let (next, status, read, wrote) =
                        chunk.inflate::<CHECK>(&mut self.0, member, written - start);
                    cursor = next;
                    let end = written + wrote;
                    // SAFETY: inflate admits wrote <= bytes.len() - written for this owner.
                    checksum.update(unsafe { bytes.get_unchecked(written..end) });
                    written = end;

                    match status {
                        TINFLStatus::Done => break cursor,
                        TINFLStatus::NeedsMoreInput if read != 0 || wrote != 0 => {}
                        TINFLStatus::HasMoreOutput if read != 0 || wrote != 0 => {}
                        TINFLStatus::HasMoreOutput => {
                            grow(output, check)?;
                        }
                        cause => {
                            let error = Error::Inflate {
                                offset: cursor.consumed,
                                output: written,
                                cause,
                            };
                            return Err(Failure::new(error, written).into());
                        }
                    }
                }
            };

            let crc = trailer
                .u32::<CHECK, E>(&mut segments, Part::Trailer, check)
                .map_err(|error| error.map_work(|cause| D::from(Failure::new(cause, written))))?;
            let actual = checksum.finalize();
            if actual != crc {
                let error = Error::Checksum {
                    expected: crc,
                    actual,
                };
                return Err(Failure::new(error, written).into());
            }

            let expected = trailer
                .u32::<CHECK, E>(&mut segments, Part::Trailer, check)
                .map_err(|error| error.map_work(|cause| D::from(Failure::new(cause, written))))?;
            let expected = expected as usize;
            let length = written - start;
            if length as u32 != expected as u32 {
                let error = Error::Length {
                    expected,
                    actual: length,
                };
                return Err(Failure::new(error, written).into());
            }

            let step = Chunk::next::<CHECK, E>(trailer, &mut segments, check)
                .map_err(|error| error.map_work(|cause| D::from(Failure::new(cause, written))))?;
            match step {
                Step::End(_) => return Ok(written),
                Step::Chunk(chunk) => {
                    let first = chunk.first();
                    input = chunk.into_source();
                    if first != 0x1f {
                        let bytes = input
                            .remaining::<CHECK, E>(&mut segments, check)
                            .map_err(Checked::Check)?;
                        return Err(Failure::new(Error::Trailing { bytes }, written).into());
                    }
                    if CHECK {
                        check().map_err(Checked::Check)?;
                    }
                    input
                        .header::<CHECK, E>(&mut segments, Some(&self.1), check)
                        .map_err(|error| {
                            error.map_work(|cause| D::from(Failure::new(cause, written)))
                        })?;
                }
            }
            start = written;
        }
    }

    /// Decodes all members into one exclusively owned, bounded resident allocation.
    pub fn decode_owned<'a, 'd, I: Iterator<Item = &'a [u8]>>(
        &mut self,
        plan: Plan<'a, I>,
        limit: usize,
        budget: &resident::Budget<'d>,
    ) -> Result<Owned<resident::Budget<'d>>, DecodeError> {
        let plan = plan.try_limit(limit)?;
        self.owned::<false, _, Infallible>(plan, limit, budget, &mut || Ok(()))
            .map_err(Checked::into_work)
    }

    /// Bounds each input, output, CRC, and initialization chunk to 64 KiB.
    /// Allocation and reallocation remain indivisible. A checkpoint may interrupt
    /// collection of a trailing-byte count; completed work errors take precedence.
    pub fn owned_with<'a, 'd, I: Iterator<Item = &'a [u8]>, E>(
        &mut self,
        plan: Plan<'a, I>,
        limit: usize,
        budget: &resident::Budget<'d>,
        mut check: impl FnMut() -> Result<(), E>,
    ) -> Result<Owned<resident::Budget<'d>>, Checked<DecodeError, E>> {
        let plan = plan.try_limit(limit).map_err(Checked::Work)?;
        check().map_err(Checked::Check)?;
        self.owned::<true, _, E>(plan, limit, budget, &mut check)
    }

    fn owned<'a, 'd, const CHECK: bool, I: Iterator<Item = &'a [u8]>, E>(
        &mut self,
        plan: Plan<'a, I>,
        limit: usize,
        budget: &resident::Budget<'d>,
        check: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Owned<resident::Budget<'d>>, Checked<DecodeError, E>> {
        let free = budget.available();
        let seed = plan.size_hint.min(STEP);
        let mut output = Owned::try_allocate(budget, seed).map_err(|cause| {
            Checked::Work(DecodeError::Allocation {
                requested: seed,
                available: free,
                cause,
            })
        })?;
        fill::<CHECK, E>(&mut output, check).map_err(Checked::Check)?;
        let written = self.decode_input::<CHECK, _, _, DecodeError, E, _>(
            plan.input,
            &mut output,
            |output, check| {
                let current = output.len();
                if current == limit {
                    return Err(Checked::Work(DecodeError::SizeLimit {
                        actual: current.saturating_add(1),
                        limit,
                    }));
                }
                if current == output.capacity() {
                    if CHECK {
                        check().map_err(Checked::Check)?;
                    }
                    let free = budget.available();
                    let target = current
                        .saturating_mul(2)
                        .min(current.saturating_add(free))
                        .max(current + 1)
                        .min(limit);
                    output.try_reserve(budget, target).map_err(|cause| {
                        Checked::Work(DecodeError::Allocation {
                            requested: target,
                            available: free,
                            cause,
                        })
                    })?;
                }
                fill::<CHECK, E>(output, check).map_err(Checked::Check)
            },
            check,
        )?;
        output.truncate(written);
        if CHECK {
            check().map_err(Checked::Check)?;
        }
        Ok(output)
    }
}

fn fill<const CHECK: bool, E>(
    output: &mut Owned<resident::Budget<'_>>,
    check: &mut impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    static ZERO: [u8; STEP] = [0; STEP];
    let mut spare = output.spare();
    if CHECK && spare.remaining() != 0 {
        check()?;
    }
    spare.write(&ZERO);
    spare.commit();
    Ok(())
}
