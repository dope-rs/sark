use sark_http::field::metadata::section::rules;
mod commit;
mod egress;
mod ingress;
pub(in crate::manifold) mod report;

use std::{num::NonZero, ops::ControlFlow};

use conn::{
    scope,
    stream::{recv::credit, resident},
};
use dope_quic::{conn, stream, wire::stream::Id};
pub use egress::Egress;
pub use ingress::Ingress;
use o3::buffer::resident::Shared;
use resident::{Handle, Local, Owner};
use sark_core::{life, manifold::stream::Boundary};
use sark_http;
use send::admit;
use stream::send;

use crate::{
    conn::io::write::{self, Gate},
    link::{self, Flush, Receive},
    manifold::{self, credits},
};

pub struct Transport<
    'd,
    'a,
    'scope,
    const ID: u8,
    B: stream::Recv = Vec<u8>,
    const RECONCILE: bool = false,
> {
    pub(super) scope: &'a mut scope::Access<'scope, ID, B, RECONCILE, Shared<'d>>,
    credits: &'a mut credits::Ledger,
    owner: Owner<'scope>,
    receive_handle: Option<Handle<'scope>>,
}

pub struct Credit<'scope> {
    id: Id,
    inner: credit::Credit<'scope>,
}

impl<'d, 'a, 'scope, const ID: u8, B: stream::Recv, const RECONCILE: bool>
    Transport<'d, 'a, 'scope, ID, B, RECONCILE>
{
    pub(super) fn new(
        scope: &'a mut scope::Access<'scope, ID, B, RECONCILE, Shared<'d>>,
        ledger: &'a mut credits::Ledger,
        owner: Owner<'scope>,
        recv: Option<Handle<'scope>>,
    ) -> Self {
        Self {
            scope,
            credits: ledger,
            owner,
            receive_handle: recv,
        }
    }

    fn pump<P, C, O>(
        &mut self,
        h3: &mut crate::conn::Connection<'d, P>,
        life: &mut O,
        handle: C,
    ) -> Result<report::Report, manifold::Error>
    where
        P: rules::Policy,
        C: Copy,
        O: life::Mode<C, Id>,
        B: stream::Retain<'d>,
    {
        let mut done = false;
        for _ in 0..link::WRITE_QUANTUM {
            let feedback = h3.session.qpack_decoder;
            let Some(taken) = h3.queues.take(&mut h3.qpack.feedback, feedback) else {
                return Self::finish::<P>(h3, Flush::Drained, done);
            };
            let sent = taken.with(|write, prepared| -> Result<_, manifold::Error> {
                let charge = if O::ENABLED {
                    write.retained()
                } else {
                    Default::default()
                };
                let commit = commit::Commit::new(prepared, &mut *life, handle, charge);
                match self.send(write, commit) {
                    admit::Outcome::Retained(done) | admit::Outcome::Discarded(done) => {
                        Ok(ControlFlow::Continue(done))
                    }
                    admit::Outcome::Blocked { owner, commit } => {
                        commit.restore(owner);
                        Ok(ControlFlow::Break(()))
                    }
                    admit::Outcome::Failed {
                        owner,
                        commit,
                        error,
                    } => {
                        commit.restore(owner);
                        Err(error.into())
                    }
                }
            })??;
            let complete = match sent {
                ControlFlow::Continue(done) => done,
                ControlFlow::Break(()) => return Self::finish::<P>(h3, Flush::Blocked, done),
            };
            match complete {
                write::Completion::Queued => done = true,
                write::Completion::Feedback => h3.events().resume()?,
            }
        }
        let flush = if h3.queues.has_write(&h3.qpack.feedback) {
            Flush::Pending
        } else {
            Flush::Drained
        };
        Self::finish::<P>(h3, flush, done)
    }

    fn finish<P: rules::Policy>(
        h3: &mut crate::conn::Connection<'d, P>,
        flush: Flush,
        done: bool,
    ) -> Result<report::Report, manifold::Error> {
        let gate = match flush {
            Flush::Blocked => Gate::Held,
            Flush::Drained | Flush::Pending => Gate::Open,
        };
        h3.session.set_gate(gate)?;
        Ok(report::Report::new(flush, done))
    }

    fn send<'id, C: admit::Commit<Shared<'d>, write::Flight<'id, 'd>>>(
        &mut self,
        write: write::Flight<'id, 'd>,
        commit: C,
    ) -> admit::Outcome<Shared<'d>, C, resident::Error, write::Flight<'id, 'd>> {
        let target = write.target();
        let id = target.stream_id();
        let handle = match target.key() {
            Some(key) => {
                let local = Local::new(id, key);
                let handle = match self.scope.streams().resident().bind(self.owner, local) {
                    Ok(handle) => handle,
                    Err(error) => {
                        return admit::Outcome::Failed {
                            owner: write,
                            commit,
                            error: resident::Error::Stale(error),
                        };
                    }
                };
                Some(handle)
            }
            None => None,
        };
        let mut table = self.scope.streams();
        if let Some(handle) = handle {
            table.try_write(handle, write, commit)
        } else {
            let id = match table.view().resolve_send(id) {
                Ok(id) => id,
                Err(error) => {
                    return admit::Outcome::Failed {
                        owner: write,
                        commit,
                        error: resident::Error::Stream(error),
                    };
                }
            };
            match table.send().try_admit(id, write, commit) {
                admit::Outcome::Retained(held) => admit::Outcome::Retained(held),
                admit::Outcome::Discarded(gone) => admit::Outcome::Discarded(gone),
                admit::Outcome::Blocked { owner, commit } => {
                    admit::Outcome::Blocked { owner, commit }
                }
                admit::Outcome::Failed {
                    owner,
                    commit,
                    error,
                } => admit::Outcome::Failed {
                    owner,
                    commit,
                    error: resident::Error::Stream(error),
                },
            }
        }
    }
}

impl<'d, 'a, 'scope, const ID: u8, B, const RECONCILE: bool> link::Recv<'d>
    for Transport<'d, 'a, 'scope, ID, B, RECONCILE>
where
    B: stream::Retain<'d>,
{
    type Credit = Credit<'scope>;
    type Error = manifold::Error;

    fn recv_stream(&mut self, id: Id) -> Result<Receive<'d, Self::Credit>, Self::Error> {
        let (held, fin) = {
            let mut table = self.scope.streams();
            if let Some(handle) = self.receive_handle.filter(|handle| handle.id() == id) {
                table.try_hold_eof(handle)?
            } else {
                let recv = table.view().resolve_recv(id)?;
                table.receive().hold_eof(recv)
            }
        };
        let Some(held) = held else {
            return Ok(if fin {
                Receive::Finished
            } else {
                Receive::Pending
            });
        };
        let (mut buffer, credit) = held.into_parts();
        let credit = Credit { id, inner: credit };
        let boundary = if fin { Boundary::End } else { Boundary::More };
        match stream::Retain::try_retain(&mut buffer) {
            Ok(bytes) => Ok(Receive::Segment {
                bytes,
                boundary,
                credit,
            }),
            Err(error) => {
                self.reject(credit);
                Err(manifold::Error::Retain(error))
            }
        }
    }

    fn accept(&mut self, credit: Self::Credit) -> Result<(), Self::Error> {
        let Credit { id, inner } = credit;
        let mut table = self.scope.streams();
        let mut recv = table.receive();
        match self.credits.try_hold(id, inner, &mut recv) {
            Ok(hold) => {
                hold.commit(&mut recv);
                Ok(())
            }
            Err(rejected) => {
                let (credit, cause) = rejected.into_parts();
                let credit = credit.release_conn(&mut recv).detach(&mut recv);
                Err(match cause {
                    credits::Rejection::Capacity => manifold::Credit::Capacity {
                        stream_id: id,
                        credit,
                    },
                    credits::Rejection::Mismatch { held } => manifold::Credit::Mismatch {
                        stream_id: id,
                        held,
                        credit,
                    },
                }
                .into())
            }
        }
    }

    fn reject(&mut self, credit: Self::Credit) {
        let mut table = self.scope.streams();
        credit.inner.release(&mut table.receive());
    }

    fn release(&mut self, id: Id, bytes: NonZero<usize>) -> Result<(), Self::Error> {
        let credit = self.credits.try_take(id, bytes)?;
        let mut table = self.scope.streams();
        Ok(credit.release(&mut table.receive())?)
    }
}
