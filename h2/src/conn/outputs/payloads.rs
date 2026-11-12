use dope::core::io::transfer;
use o3::{
    alloc,
    buffer::{hold, storage},
    depot::{
        array::Index,
        queue::{
            group,
            group::{capacity, vacant},
        },
    },
};
use sark_core::life::session;

use crate::{flow, frame, stream};

/// gRPC's fixed message prefix is the only copied prefix on the retained DATA
/// path. Keeping this exact avoids inflating every staged record.
pub(crate) const INLINE_PREFIX: usize = 5;
pub(crate) const MAX_PREFIX: usize = transfer::MAX_BYTES;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(in crate::conn) struct Mark(usize);

pub(crate) enum Inline {
    Empty([u8; 0]),
    One([u8; 1]),
    Two([u8; 2]),
    Three([u8; 3]),
    Four([u8; 4]),
    Five([u8; INLINE_PREFIX]),
}

pub(in crate::conn) enum Shared<C: session::Storage> {
    Data(Data<Option<C>>),
    Wire(Wire<C>),
}

pub(in crate::conn) struct Data<B> {
    pub(in crate::conn) credit: flow::Reserved,
    mark: Mark,
    pub(super) header: [u8; frame::HEADER_LEN],
    pub(super) inline: Inline,
    pub(in crate::conn) body: B,
}

pub(in crate::conn) struct Wire<C> {
    pub(super) stream: stream::Key,
    mark: Mark,
    pub(super) body: C,
}

impl<B> Data<B> {
    pub(super) fn into_parts(self) -> (Data<()>, B) {
        let Self {
            credit,
            mark,
            header,
            inline,
            body,
        } = self;
        (
            Data {
                credit,
                mark,
                header,
                inline,
                body: (),
            },
            body,
        )
    }
}

impl Data<()> {
    pub(super) fn with<B>(self, body: B) -> Data<B> {
        let Self {
            credit,
            mark,
            header,
            inline,
            body: (),
        } = self;
        Data {
            credit,
            mark,
            header,
            inline,
            body,
        }
    }
}

impl<C> Wire<C> {
    pub(super) fn into_parts(self) -> (Wire<()>, C) {
        let Self { stream, mark, body } = self;
        (
            Wire {
                stream,
                mark,
                body: (),
            },
            body,
        )
    }
}

impl Wire<()> {
    pub(super) fn with<C>(self, body: C) -> Wire<C> {
        Wire {
            stream: self.stream,
            mark: self.mark,
            body,
        }
    }
}

pub(super) type Vacancy<'a, C> = group::Vacancy<'a, Shared<C>, 1>;
pub(super) type Restorable<'a, C, T = Shared<C>> = (T, Vacancy<'a, C>);

pub(crate) struct Ledger<C: session::Storage> {
    records: group::Ledger<Shared<C>, 1>,
    bytes: usize,
    resident: usize,
}

/// One admitted destination and the byte account of that exact ledger.
pub(in crate::conn) struct Entry<'a, C: session::Storage> {
    slot: vacant::Entry<'a, Shared<C>, 1>,
    bytes: &'a mut usize,
    resident: &'a mut usize,
}

/// The selected native head and its ledger remain borrowed together.
pub(super) struct Front<'a, C: session::Storage> {
    slot: Option<group::Front<'a, Shared<C>, 1>>,
    bytes: &'a mut usize,
    resident: &'a mut usize,
}

/// The exact charge extracted from a native head, awaiting commit or restore.
pub(super) struct Taken<'a> {
    bytes: &'a mut usize,
    resident: &'a mut usize,
    charge: usize,
    retained: usize,
}

pub(crate) enum Write<C: session::Storage> {
    Buffered {
        written: usize,
        accepted: Option<flow::Reserved>,
    },
    Split {
        prefix_len: usize,
        credit: flow::Reserved,
        body: C,
    },
    Owned {
        stream: stream::Key,
        prefix_len: usize,
        body: C,
    },
}

impl Inline {
    pub(crate) const EMPTY: Self = Self::Empty([]);

    pub(crate) fn prefix(slice: &[u8], max: usize) -> Self {
        match (slice, max) {
            ([], _) | (_, 0) => Self::Empty([]),
            ([a], _) | ([a, ..], 1) => Self::One([*a]),
            ([a, b], _) | ([a, b, ..], 2) => Self::Two([*a, *b]),
            ([a, b, c], _) | ([a, b, c, ..], 3) => Self::Three([*a, *b, *c]),
            ([a, b, c, d], _) | ([a, b, c, d, ..], 4) => Self::Four([*a, *b, *c, *d]),
            ([a, b, c, d, e, ..], _) => Self::Five([*a, *b, *c, *d, *e]),
        }
    }

    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Empty(bytes) => bytes,
            Self::One(bytes) => bytes,
            Self::Two(bytes) => bytes,
            Self::Three(bytes) => bytes,
            Self::Four(bytes) => bytes,
            Self::Five(bytes) => bytes,
        }
    }
}

impl Mark {
    pub(super) const ZERO: Self = Self(0);

    pub(in crate::conn) const fn after(self, len: usize) -> Self {
        Self(self.0.wrapping_add(len))
    }

    pub(super) const fn distance(self, head: Self) -> usize {
        self.0.wrapping_sub(head.0)
    }
}

impl<C: session::Storage> Shared<C> {
    pub(super) fn head_len(&self) -> transfer::Len {
        match self {
            Self::Data(data) => match &data.inline {
                Inline::Empty(_) => transfer::Len::fixed::<{ frame::HEADER_LEN }>(),
                Inline::One(_) => transfer::Len::fixed::<{ frame::HEADER_LEN + 1 }>(),
                Inline::Two(_) => transfer::Len::fixed::<{ frame::HEADER_LEN + 2 }>(),
                Inline::Three(_) => transfer::Len::fixed::<{ frame::HEADER_LEN + 3 }>(),
                Inline::Four(_) => transfer::Len::fixed::<{ frame::HEADER_LEN + 4 }>(),
                Inline::Five(_) => transfer::Len::fixed::<{ frame::HEADER_LEN + INLINE_PREFIX }>(),
            },
            Self::Wire(_) => transfer::Len::ZERO,
        }
    }

    pub(super) fn body_len(&self) -> usize {
        self.body().map_or(0, |body| body.as_ref().len())
    }

    pub(in crate::conn) fn data_len(&self) -> usize {
        self.head_len().into_raw() + self.body_len()
    }

    fn resident(&self) -> usize {
        match self {
            Self::Data(data) => data.body.as_ref().map_or(0, |body| {
                hold::Charge::from_owner(body.as_ref().len(), body.resident()).resident_bytes
            }),
            Self::Wire(_) => 0,
        }
    }

    pub(super) const fn prefix_len(&self, head: Mark) -> usize {
        match self {
            Self::Data(data) => data.mark.distance(head),
            Self::Wire(wire) => wire.mark.distance(head),
        }
    }

    pub(super) fn body(&self) -> Option<&C> {
        match self {
            Self::Data(data) => data.body.as_ref(),
            Self::Wire(wire) => Some(&wire.body),
        }
    }

    pub(in crate::conn) fn data(
        mark: Mark,
        header: [u8; frame::HEADER_LEN],
        inline: Inline,
        body: Option<C>,
        credit: flow::Reserved,
    ) -> Self {
        Self::Data(Data {
            credit,
            mark,
            header,
            inline,
            body,
        })
    }

    pub(super) fn wire(stream: stream::Key, mark: Mark, body: C) -> Self {
        Self::Wire(Wire { stream, mark, body })
    }
}

impl<C: session::Storage> Ledger<C> {
    pub(crate) const MAX_MATERIALIZED_TAIL: usize = group::Ledger::<Shared<C>, 1>::record_bytes();

    pub(super) fn try_new(capacity: capacity::Count<1>) -> Result<Self, alloc::Error> {
        Ok(Self {
            records: group::Ledger::try_new(capacity)?,
            bytes: 0,
            resident: 0,
        })
    }

    pub(super) const fn bytes(&self) -> usize {
        self.bytes
    }

    pub(in crate::conn) fn check_charge(&self, added: usize) -> Result<(), hold::Overflow> {
        hold::Charge::from_commit(0, self.resident, 0)
            .try_add(hold::Charge::from_commit(0, added, 0))
            .map(|_| ())
    }

    /// The caller retains its admitted source and exclusive outbound borrow
    /// until insertion, so the prepared payload cannot outgrow its byte room
    /// or checked retained DATA resident charge. Wire records carry no such charge.
    pub(in crate::conn) fn try_reserve(
        &mut self,
        owner: u64,
    ) -> Result<Entry<'_, C>, group::Error<1>> {
        let slot = group::Batch::new(&mut self.records)?.bind(Index::at::<0>(), owner)?;
        Ok(Entry {
            slot,
            bytes: &mut self.bytes,
            resident: &mut self.resident,
        })
    }

    pub(in crate::conn) fn clear(&mut self, release: impl FnMut(Shared<C>)) {
        self.records.clear(release);
        self.bytes = 0;
        self.resident = 0;
    }

    pub(in crate::conn) fn drain(
        &mut self,
        id: stream::Id,
        mut release: impl FnMut(Shared<C>, usize),
    ) {
        let bytes = &mut self.bytes;
        let resident = &mut self.resident;
        self.records
            .drain_owner(u64::from(id.into_u32()), Index::at::<0>(), |payload| {
                let charge = payload.data_len();
                let retained = payload.resident();
                Taken {
                    bytes: &mut *bytes,
                    resident: &mut *resident,
                    charge,
                    retained,
                }
                .commit();
                release(payload, retained);
            });
    }

    pub(super) fn is_full(&self) -> bool {
        self.records.is_full()
    }

    pub(super) fn bounds(&self) -> (usize, usize) {
        let view = &self.records;
        let used = view.lane_len(Index::at::<0>());
        (used, used + view.available())
    }

    pub(super) fn front(&self) -> Option<&Shared<C>> {
        self.records.front(Index::at::<0>())
    }

    pub(super) fn bind_front(&mut self) -> Front<'_, C> {
        Front {
            slot: self.records.bind_front(Index::at::<0>()),
            bytes: &mut self.bytes,
            resident: &mut self.resident,
        }
    }

    pub(in crate::conn) fn front_prefix(&self, head: Mark) -> Option<usize> {
        self.front().map(|payload| payload.prefix_len(head))
    }
}

impl<C: session::Storage> Entry<'_, C> {
    pub(in crate::conn) fn insert(self, payload: Shared<C>) {
        *self.bytes += payload.data_len();
        *self.resident += payload.resident();
        self.slot.insert(payload);
    }
}

impl<'a, C: session::Storage> Front<'a, C> {
    pub(super) fn bytes(&self) -> usize {
        *self.bytes
    }

    pub(super) fn get(&self) -> Option<&Shared<C>> {
        self.slot.as_ref().map(group::Front::get)
    }

    pub(super) fn defer(&mut self) {
        self.slot = None;
    }

    pub(super) fn remove(self) -> (Option<Shared<C>>, &'a usize) {
        let payload = self.slot.map(group::Front::remove);
        if let Some(payload) = &payload {
            Taken {
                bytes: &mut *self.bytes,
                resident: &mut *self.resident,
                charge: payload.data_len(),
                retained: payload.resident(),
            }
            .commit();
        }
        (payload, self.bytes)
    }

    pub(super) fn take(self) -> (Option<Restorable<'a, C>>, Taken<'a>) {
        let payload = self.slot.map(group::Front::take);
        let charge = payload
            .as_ref()
            .map_or(0, |(payload, _)| payload.data_len());
        let retained = payload
            .as_ref()
            .map_or(0, |(payload, _)| payload.resident());
        (
            payload,
            Taken {
                bytes: self.bytes,
                resident: self.resident,
                charge,
                retained,
            },
        )
    }
}

impl<'a> Taken<'a> {
    /// Releases the immutable charge selected with this ledger's native entry.
    /// The exclusive account borrow survives extraction until commit; restoring
    /// the native vacancy instead leaves the admitted sum unchanged.
    pub(super) fn commit(self) -> &'a usize {
        *self.bytes = self.bytes.wrapping_sub(self.charge);
        *self.resident = self.resident.wrapping_sub(self.retained);
        self.bytes
    }
}

/// Fixed metadata; payload and its lifetime owner stay external.
const _: () = {
    assert!(core::mem::size_of::<Inline>() == INLINE_PREFIX + 1);
    assert!(core::mem::align_of::<Inline>() == 1);
};
const _: () = {
    assert!(core::mem::size_of::<Data<Option<storage::Shared>>>() == 64);
    assert!(core::mem::size_of::<Shared<storage::Shared>>() == 64);
};
const _: () = assert!(
    core::mem::size_of::<Option<storage::Shared>>() == core::mem::size_of::<storage::Shared>()
);

const _: () = {
    use core::mem::size_of;
    type Payload = Shared<storage::Shared>;

    assert!(
        size_of::<Ledger<storage::Shared>>()
            == size_of::<(group::Ledger<Payload, 1>, usize, usize)>()
    );
    assert!(
        size_of::<Entry<'static, storage::Shared>>()
            == size_of::<(vacant::Entry<'static, Payload, 1>, &mut usize, &mut usize)>()
    );
    assert!(
        size_of::<Front<'static, storage::Shared>>()
            == size_of::<(
                Option<group::Front<'static, Payload, 1>>,
                &mut usize,
                &mut usize,
            )>()
    );
    assert!(size_of::<Taken<'static>>() == 4 * size_of::<usize>());
    assert!(size_of::<Option<Payload>>() == size_of::<Payload>());
};
