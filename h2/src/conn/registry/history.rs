use core::marker;

use o3::base::marker::ThreadBound;

use crate::{Endpoint, conn::registry::Past, stream};

#[must_use]
pub(crate) struct Peer<'a, R: crate::Role> {
    history: &'a mut History<R>,
    id: stream::Nonzero,
}

pub(crate) struct History<R: crate::Role> {
    role: marker::PhantomData<R>,
    next_local_id: stream::IdGen,
    last_peer_id: stream::Id,
    _thread: ThreadBound,
}

impl<R: crate::Role> History<R> {
    pub(crate) fn new() -> Self {
        let first = match R::ENDPOINT {
            Endpoint::Server => stream::Nonzero::FIRST_SERVER,
            Endpoint::Client => stream::Nonzero::FIRST_CLIENT,
        };
        Self {
            role: marker::PhantomData,
            next_local_id: stream::IdGen::new(first),
            last_peer_id: stream::Id::CONNECTION,
            _thread: ThreadBound::NEW,
        }
    }

    pub(crate) fn is_peer(id: stream::Id) -> bool {
        match R::ENDPOINT {
            Endpoint::Server => id.is_client(),
            Endpoint::Client => id.is_server(),
        }
    }

    pub(crate) fn is_local(id: stream::Id) -> bool {
        !Self::is_peer(id)
    }

    pub(crate) fn classify(&self, stream: stream::Nonzero) -> Past {
        let id = stream.get();
        let opened = if Self::is_peer(id) {
            id <= self.last_peer_id
        } else {
            self.next_local_id.peek().is_none_or(|next| id < next)
        };
        if opened { Past::Closed } else { Past::Idle }
    }

    pub(crate) fn peer(&mut self, proof: stream::Nonzero) -> Option<Peer<'_, R>> {
        let id = proof.get();
        if !Self::is_peer(id) || id <= self.last_peer_id {
            return None;
        }
        Some(Peer {
            history: self,
            id: proof,
        })
    }

    pub(crate) fn last_peer_id(&self) -> stream::Id {
        self.last_peer_id
    }

    pub(crate) fn next_local(&mut self) -> Option<stream::Next<'_>> {
        self.next_local_id.claim()
    }
}

impl<R: crate::Role> Peer<'_, R> {
    pub(crate) fn commit(self) {
        self.history.last_peer_id = self.id.get();
    }
}
