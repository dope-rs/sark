use o3::buffer::storage::inline;
use sark_http::field::metadata::integer;

use crate::qpack::stream;

pub(crate) const CAPACITY: usize = inline::CAPACITY;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Status {
    #[default]
    Clear,
    Blocked(stream::Capacity),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    Capacity(stream::Capacity),
    Increment { pending: u64, additional: u64 },
}

pub(crate) struct State {
    bytes: inline::Bytes<CAPACITY>,
    tail_start: u8,
    tail_increment: u64,
}

pub(crate) struct Candidate<'a> {
    owner: &'a mut State,
    next: State,
    growth: usize,
}

struct Push {
    start: u8,
    instruction: stream::Instruction,
}

impl Push {
    fn try_new(state: &State, inst: stream::Instruction) -> Result<Self, Error> {
        let (start, inst) = match (state.tail_start, inst) {
            (start, stream::Instruction::InsertCountIncrement { increment: amount })
                if start != State::NO_TAIL =>
            {
                let amount = amount
                    .checked_add(state.tail_increment)
                    .ok_or(Error::Increment {
                        pending: state.tail_increment,
                        additional: amount.get(),
                    })?;
                (
                    start,
                    stream::Instruction::InsertCountIncrement { increment: amount },
                )
            }
            (_, inst) => (state.bytes.len() as u8, inst),
        };
        Ok(Self {
            start,
            instruction: inst,
        })
    }

    /// Room this instruction retains under the QPACK limit and the inline owner.
    ///
    /// `None` means the instruction's retained start is already past the limit, so
    /// its encoding can never fit; the caller must reject rather than clamp.
    fn room(&self, limit: usize) -> Option<usize> {
        limit.min(CAPACITY).checked_sub(usize::from(self.start))
    }

    /// Reports whether the retained room admits this instruction's encoding.
    fn fits(&self, limit: usize, len: usize) -> bool {
        match self.room(limit) {
            Some(room) => len <= room,
            None => false,
        }
    }
}

impl State {
    const NO_TAIL: u8 = u8::MAX;

    pub(crate) const fn new() -> Self {
        Self {
            bytes: inline::Bytes::new(),
            tail_start: Self::NO_TAIL,
            tail_increment: 0,
        }
    }

    fn try_stage(
        &mut self,
        update: impl FnOnce(&mut Self) -> Result<(), Error>,
    ) -> Result<Candidate<'_>, Error> {
        let before = self.len();
        let mut next = Self {
            bytes: self.bytes,
            tail_start: self.tail_start,
            tail_increment: self.tail_increment,
        };
        update(&mut next)?;
        let growth = next.len() - before;
        Ok(Candidate {
            owner: self,
            next,
            growth,
        })
    }

    pub(crate) fn try_prepare(
        &mut self,
        inst: stream::Instruction,
        limit: usize,
    ) -> Result<Candidate<'_>, Error> {
        self.try_stage(|next| next.try_push(inst, limit))
    }

    pub(crate) fn can_prepare(&self, inst: stream::Instruction, limit: usize) -> bool {
        let Ok(push) = Push::try_new(self, inst) else {
            return false;
        };
        push.fits(limit, push.instruction.encoded_len())
    }

    pub(crate) fn try_prefix(&mut self, prefix: u8, limit: usize) -> Result<Candidate<'_>, Error> {
        self.try_stage(|next| next.try_prepend(prefix, limit))
    }

    pub(crate) fn try_push(
        &mut self,
        inst: stream::Instruction,
        limit: usize,
    ) -> Result<(), Error> {
        let push = Push::try_new(self, inst)?;
        let encoded: inline::Bytes<MAX_INSTRUCTION> = match push.instruction {
            stream::Instruction::SectionAcknowledgment { stream_id: id } => {
                integer::PrefixedInt::<7>::new(id).pack(0x80)
            }
            stream::Instruction::StreamCancellation { stream_id: id } => {
                integer::PrefixedInt::<6>::new(id).pack(0x40)
            }
            stream::Instruction::InsertCountIncrement { increment: amount } => {
                integer::PrefixedInt::<6>::new(amount.get()).pack(0)
            }
        };
        self.bytes
            .try_replace(usize::from(push.start), limit, [encoded.as_ref()])
            .map_err(|_| {
                Error::Capacity(stream::Capacity {
                    len: push.start,
                    additional: encoded.len() as u8,
                    limit: limit.min(CAPACITY) as u8,
                })
            })?;
        let (tail, amount) = match push.instruction {
            stream::Instruction::InsertCountIncrement { increment: amount } => {
                (push.start, amount.get())
            }
            _ => (Self::NO_TAIL, 0),
        };
        self.tail_start = tail;
        self.tail_increment = amount;
        Ok(())
    }

    fn try_prepend(&mut self, prefix: u8, limit: usize) -> Result<(), Error> {
        let mut bytes = inline::Bytes::new();
        bytes
            .try_replace(0, limit, [&[prefix], self.bytes.as_ref()])
            .map_err(|_| {
                Error::Capacity(stream::Capacity {
                    len: self.bytes.len() as u8,
                    additional: 1,
                    limit: limit.min(CAPACITY) as u8,
                })
            })?;
        let tail = if self.tail_start == Self::NO_TAIL {
            Self::NO_TAIL
        } else {
            self.tail_start + 1
        };
        self.bytes = bytes;
        self.tail_start = tail;
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) const fn bytes(&self) -> inline::Bytes<CAPACITY> {
        self.bytes
    }

    pub(crate) fn clear(&mut self) {
        self.bytes.clear();
        self.tail_start = Self::NO_TAIL;
        self.tail_increment = 0;
    }
}

impl Candidate<'_> {
    pub(crate) const fn growth(&self) -> usize {
        self.growth
    }

    pub(crate) fn commit(self) {
        *self.owner = self.next;
    }
}

const _: () = {
    assert!(CAPACITY < u8::MAX as usize);
    assert!(core::mem::size_of::<State>() <= 6 * core::mem::size_of::<usize>());
};

const MAX_INSTRUCTION: usize = {
    let six = integer::PrefixedInt::<6>::new(u64::MAX).encoded_len().get();
    let seven = integer::PrefixedInt::<7>::new(u64::MAX).encoded_len().get();
    if six > seven { six } else { seven }
};

const _: () = assert!(MAX_INSTRUCTION <= u8::MAX as usize);
