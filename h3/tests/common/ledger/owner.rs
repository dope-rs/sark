use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZero,
};

use dope_quic::wire::stream::Id;
use o3::{
    alloc,
    buffer::{resident, storage},
};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::Error,
    link::{Receive, Recv},
};

#[derive(Default)]
pub struct Held<'d> {
    pub incoming: BTreeMap<u64, VecDeque<resident::Shared<'d>>>,
    pub finished: BTreeSet<u64>,
    pub accepted: Vec<(u64, usize)>,
    pub rejected: Vec<(u64, usize)>,
    pub released: Vec<(u64, usize)>,
}

impl Held<'_> {
    pub fn try_push(
        &mut self,
        stream: u64,
        mut bytes: Vec<u8>,
    ) -> Result<(), (Vec<u8>, alloc::Exhausted)> {
        let owner = match storage::Shared::try_from(&mut bytes) {
            Ok(owner) => owner,
            Err(error) => return Err((bytes, error)),
        };
        self.incoming
            .entry(stream)
            .or_default()
            .push_back(resident::Shared::from(owner));
        Ok(())
    }
}

impl<'d> Recv<'d> for Held<'d> {
    type Credit = (Id, usize);
    type Error = Error;

    fn recv_stream(&mut self, id: Id) -> Result<Receive<'d, Self::Credit>, Self::Error> {
        let stream = id.get();
        if let Some(bytes) = self.incoming.get_mut(&stream).and_then(VecDeque::pop_front) {
            let boundary = if self.finished.contains(&stream)
                && self.incoming.get(&stream).is_none_or(VecDeque::is_empty)
            {
                Boundary::End
            } else {
                Boundary::More
            };
            let credit = (id, bytes.len());
            return Ok(Receive::Segment {
                bytes,
                boundary,
                credit,
            });
        }
        Ok(if self.finished.contains(&stream) {
            Receive::Finished
        } else {
            Receive::Pending
        })
    }

    fn accept(&mut self, (id, bytes): Self::Credit) -> Result<(), Self::Error> {
        self.accepted.push((id.get(), bytes));
        Ok(())
    }

    fn reject(&mut self, (id, bytes): Self::Credit) {
        self.rejected.push((id.get(), bytes));
    }

    fn release(&mut self, id: Id, bytes: NonZero<usize>) -> Result<(), Self::Error> {
        self.released.push((id.get(), bytes.get()));
        Ok(())
    }
}
