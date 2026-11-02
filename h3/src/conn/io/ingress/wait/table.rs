use dope_quic::wire::stream::Id;
use o3::{
    alloc,
    depot::queue::{group::capacity, indexed::slab, tagged},
};

use crate::conn::{
    self,
    io::ingress::{
        receive::Recv,
        stream::table,
        wait::{Error, Lane},
    },
};

type Entries = slab::Slab<Id, (), { u32::MAX }, tagged::Hook, 2, capacity::Count<2>>;

pub(in crate::conn) struct Member {
    key: slab::Handle,
}

pub(in crate::conn) struct Table {
    entries: Entries,
}

impl Table {
    pub(in crate::conn) fn try_new(capacity: capacity::Count<2>) -> Result<Self, alloc::Error> {
        Ok(Self {
            entries: Entries::try_from(capacity)?,
        })
    }

    pub(in crate::conn) fn push_ready(&mut self, id: Id, recv: &mut Recv<'_>) -> Result<(), Error> {
        self.push(id, recv, Lane::Ready)
    }

    pub(in crate::conn) fn defer(&mut self, id: Id, recv: &mut Recv<'_>) -> Result<(), Error> {
        self.push(id, recv, Lane::Feedback)
    }

    pub(in crate::conn) fn pop_ready(
        &mut self,
        streams: &mut table::Table<'_>,
    ) -> Result<Option<Id>, conn::Error> {
        let Some(((), entry)) = self.entries.select([(Lane::Ready.index(), ())]) else {
            return Ok(None);
        };
        let id = *entry.as_ref();
        let recv = streams.recv_mut(id).ok_or(conn::Error::StreamMissing {
            stream_id: id.get(),
        })?;
        recv.wait = None;
        entry.remove();
        Ok(Some(id))
    }

    pub(in crate::conn) fn has_ready(&self) -> bool {
        self.entries.front_pair(Lane::Ready.index()).is_some()
    }

    pub(in crate::conn) fn promote(
        &mut self,
        expected: Id,
        recv: &mut Recv<'_>,
    ) -> Result<(), Error> {
        let Some(((), entry)) = self.entries.select([(Lane::Feedback.index(), ())]) else {
            return Err(Error::Missing { expected });
        };
        let actual = *entry.as_ref();
        if actual != expected {
            return Err(Error::Mismatch { expected, actual });
        }
        if recv.wait.is_none() {
            return Err(Error::Missing { expected });
        }
        entry.move_back(Lane::Ready.index());
        recv.wait_lane = Lane::Ready;
        Ok(())
    }

    pub(in crate::conn) fn feedback(&self) -> Option<Id> {
        self.entries
            .front_pair(Lane::Feedback.index())
            .map(|(_, id)| *id)
    }

    pub(in crate::conn) fn remove(&mut self, member: &mut Option<Member>) -> Result<(), Error> {
        if let Some(queued) = member.as_ref() {
            self.entries
                .try_entry(queued.key)
                .map_err(Error::Slot)?
                .remove();
            *member = None;
        }
        Ok(())
    }

    fn push(&mut self, id: Id, recv: &mut Recv<'_>, lane: Lane) -> Result<(), Error> {
        if recv.wait.is_some() {
            return Err(Error::Present {
                id,
                queued: recv.wait_lane,
                requested: lane,
            });
        }
        let capacity = self.entries.capacity().get();
        let vacant = self
            .entries
            .vacant()
            .ok_or(Error::Full { id, lane, capacity })?;
        let key = vacant.select(lane.index()).insert(id).key();
        recv.wait = Some(Member { key });
        recv.wait_lane = lane;
        Ok(())
    }
}

const _: () = assert!(size_of::<Option<Member>>() == size_of::<usize>());
