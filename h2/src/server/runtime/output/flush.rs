use core::mem;

use conn::write;
use data::compose;
use dope::{
    core::driver::{retained, route::Identity},
    manifold::network::listener,
    net::{
        link::egress::{self, data},
        wire,
    },
};
use listener::conn;
use o3::buffer::{
    hold,
    resident::{self, bound},
};
use output::{
    Error, Failure,
    track::{Progress, Proof, Sink, Track},
};
use sark_core::life;
use write::error;

use crate::{conn::event, frame::numbers, server::runtime::output, stream};

type Admission<'d, P, G> = Result<
    (
        egress::Load<data::Bound<P, G>>,
        <P as Evidence<'d>>::Receipt,
    ),
    (egress::Excess<P>, <P as Evidence<'d>>::Receipt),
>;

enum Cycle<R> {
    Empty,
    Write(R),
}

pub(in crate::server) enum End {
    Close,
    Keep,
}

const _: () = {
    assert!(mem::size_of::<End>() == mem::size_of::<bool>());
    assert!(mem::align_of::<End>() == mem::align_of::<bool>());
};

pub(in crate::server) trait Evidence<'d>: data::raw::Payload<'d> + Sized {
    type Write;
    type Receipt: Default;
    type Track<'a, C: Copy + 'a, M: life::Mode<C, stream::Key> + 'a>: Sink<Self::Receipt>;

    fn track<'a, C: Copy + 'a, M: life::Mode<C, stream::Key> + 'a>(
        mode: &'a mut M,
        conn: C,
    ) -> Self::Track<'a, C, M>;

    fn retain(
        stream: stream::Key,
        bytes: usize,
        body: resident::Shared<'d>,
    ) -> (resident::Shared<'d>, Self::Write);

    fn admit<G: bound::Bound>(
        write: Self::Write,
        body: resident::Shared<'d>,
    ) -> Admission<'d, Self, G>;

    fn admit_owned<G: bound::Bound>(
        stream: stream::Key,
        body: resident::Shared<'d>,
    ) -> Admission<'d, Self, G>;
}

impl<'d> Evidence<'d> for resident::Shared<'d> {
    type Write = ();
    type Receipt = ();
    type Track<'a, C: Copy + 'a, M: life::Mode<C, stream::Key> + 'a> = Track<()>;

    fn track<'a, C: Copy + 'a, M: life::Mode<C, stream::Key> + 'a>(
        _: &'a mut M,
        _: C,
    ) -> Self::Track<'a, C, M> {
        Track::new(())
    }

    fn retain(_: stream::Key, _: usize, body: resident::Shared<'d>) -> (resident::Shared<'d>, ()) {
        (body, ())
    }

    fn admit<G: bound::Bound>(
        _: Self::Write,
        body: resident::Shared<'d>,
    ) -> Admission<'d, Self, G> {
        let body = egress::Load::new(body)
            .try_bound::<G>()
            .map_err(|cause| (cause, ()))?;
        Ok((body, ()))
    }

    fn admit_owned<G: bound::Bound>(
        _: stream::Key,
        body: resident::Shared<'d>,
    ) -> Admission<'d, Self, G> {
        Self::admit::<G>((), body)
    }
}

impl<'d> Evidence<'d> for compose::Tagged<resident::Shared<'d>, stream::Key> {
    type Write = Proof;
    type Receipt = Option<Proof>;
    type Track<'a, C: Copy + 'a, M: life::Mode<C, stream::Key> + 'a> = Track<(C, &'a mut M)>;

    fn track<'a, C: Copy + 'a, M: life::Mode<C, stream::Key> + 'a>(
        mode: &'a mut M,
        conn: C,
    ) -> Self::Track<'a, C, M> {
        Track::new((conn, mode))
    }

    fn retain(
        stream: stream::Key,
        bytes: usize,
        body: resident::Shared<'d>,
    ) -> (resident::Shared<'d>, Proof) {
        let charge = data::raw::Payload::charge(&body);
        (
            body,
            Proof {
                stream,
                charge: hold::Charge::from_owner(bytes, charge.resident_bytes),
            },
        )
    }

    fn admit<G: bound::Bound>(
        write: Self::Write,
        body: resident::Shared<'d>,
    ) -> Admission<'d, Self, G> {
        match egress::Load::new(body)
            .tagged(write.stream)
            .try_bound::<G>()
        {
            Ok(body) => Ok((body, Some(write))),
            Err(cause) => Err((cause, Some(write))),
        }
    }

    fn admit_owned<G: bound::Bound>(
        stream: stream::Key,
        body: resident::Shared<'d>,
    ) -> Admission<'d, Self, G> {
        let body = egress::Load::new(body)
            .tagged(stream)
            .try_bound::<G>()
            .map_err(|cause| (cause, None))?;
        Ok((body, None))
    }
}

fn flush_into<'d, ID: Identity, W, C, E, G, S>(
    conn: &mut conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, W, C, data::Bound<E, G>>,
    driver: &mut retained::Context<'_, '_, 'd>,
    end: End,
    accepted: &mut S,
) -> Result<(), Failure>
where
    W: wire::Spec,
    C: output::State<'d>,
    E: Evidence<'d>,
    G: bound::Bound,
    S: Sink<E::Receipt>,
{
    loop {
        if !conn.state_mut().protocol().transfer().has_write() {
            if matches!(end, End::Close) {
                conn.close_after();
            }
            return Ok(());
        }
        let outcome = conn.write().try_commit(driver, |state, mut prefix| {
            let bound = match state
                .protocol()
                .transfer()
                .bind_link::<{ write::Prefix::CAPACITY }>()
            {
                Ok(Some(bound)) => bound,
                Ok(None) => return prefix.commit(None, Cycle::Empty).map_err(Error::Commit),
                Err(cause) => return Err(Error::Input(cause)),
            };
            let drained = bound.is_drained();
            let output = bound
                .commit_with(&mut prefix, E::retain)
                .map_err(|cause| Error::Commit(error::Build::prefix(cause, None, Cycle::Empty)))?;
            let (body, receipt) = match output {
                event::Write::Buffered(_) => Ok((None, E::Receipt::default())),
                event::Write::Owned {
                    stream_id: stream,
                    prefix_len: _,
                    body,
                } => E::admit_owned::<G>(stream, body).map(|(body, receipt)| (Some(body), receipt)),
                event::Write::Split {
                    write,
                    prefix_len: _,
                    body,
                } => E::admit::<G>(write, body).map(|(body, receipt)| (Some(body), receipt)),
            }
            .map_err(|(cause, receipt)| Error::Bound {
                cause,
                receipt: Cycle::Write(receipt),
            })?;
            let commit = prefix
                .commit(body, Cycle::Write(receipt))
                .map_err(Error::Commit)?;
            let end = match (&end, drained) {
                (End::Close, true) => End::Close,
                (End::Close, false) | (End::Keep, _) => End::Keep,
            };
            Ok(match end {
                End::Close => commit.close_after(),
                End::Keep => commit,
            })
        });
        match outcome {
            Ok(write::Outcome::Committed(Cycle::Write(receipt))) => {
                accepted.accept(receipt);
            }
            Ok(write::Outcome::Committed(Cycle::Empty)) => return Ok(()),
            Ok(write::Outcome::Deferred(_) | write::Outcome::Closed) => {
                return Ok(());
            }
            Err(cause) => {
                let (failure, receipt) = match cause {
                    error::Error::Capacity(cause) => (Ok(Failure::Capacity(cause)), Cycle::Empty),
                    error::Error::Build(Error::Input(cause)) => {
                        (Ok(Failure::Input(cause)), Cycle::Empty)
                    }
                    error::Error::Build(Error::Bound { cause, receipt }) => {
                        (Err(cause.into()), receipt)
                    }
                    error::Error::Build(Error::Commit(error::Build::Prefix {
                        cause,
                        receipt,
                        ..
                    })) => (Ok(Failure::Prefix(cause)), receipt),
                    error::Error::Build(Error::Commit(error::Build::Rejected {
                        cause,
                        receipt,
                        ..
                    })) => {
                        let (_, cause) = cause.into_parts();
                        (Ok(Failure::Rejected(cause)), receipt)
                    }
                    error::Error::Build(Error::Commit(error::Build::Overflow {
                        cause,
                        receipt,
                        ..
                    })) => (Ok(Failure::Overflow(cause)), receipt),
                    error::Error::Mismatch(cause) => {
                        let (cause, receipt) = cause.into_parts();
                        (Ok(Failure::Mismatch(cause)), receipt)
                    }
                };
                match receipt {
                    Cycle::Write(receipt) => accepted.reject(receipt),
                    Cycle::Empty => {}
                }
                match failure {
                    Ok(failure) => {
                        conn.close_after();
                        return Err(failure);
                    }
                    Err(failure) => conn.write().reject(failure),
                }
                return Ok(());
            }
        }
    }
}

pub(in crate::server) struct Flush<'a, T> {
    conn: &'a mut T,
}

impl<'a, T> Flush<'a, T> {
    pub(in crate::server) fn new(conn: &'a mut T) -> Self {
        Self { conn }
    }
}

impl<'a, 'e, 'n, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, E, G>
    Flush<'a, conn::Ctx<'e, 'n, 'p, 's, 'w, 'd, ID, W, C, data::Bound<E, G>>>
where
    W: wire::Spec,
    C: output::State<'d>,
    E: Evidence<'d>,
    G: bound::Bound,
{
    pub(in crate::server) fn observe<M: life::Mode<conn::Id<'d, ID>, stream::Key>>(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
        error: Option<numbers::ErrorCode>,
        mode: &mut M,
    ) {
        if let Err((cause, _)) = self.finish(driver, error, mode) {
            mode.diagnostic(&cause);
        }
    }

    pub(in crate::server) fn finish<M: life::Mode<conn::Id<'d, ID>, stream::Key>>(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
        error: Option<numbers::ErrorCode>,
        mode: &mut M,
    ) -> Result<Progress, (Failure, Progress)> {
        let end = match error {
            Some(error) => {
                self.conn.state_mut().protocol().control().goaway(error);
                End::Close
            }
            None => self.conn.state_mut().settle(),
        };
        let mut accepted = E::track(mode, self.conn.id());
        flush_into(self.conn, driver, end, &mut accepted)
            .map_err(|failure| (failure, accepted.progress()))?;
        Ok(accepted.progress())
    }
}

const _: () = {
    assert!(
        core::mem::size_of::<data::Bound<resident::Shared<'_>, bound::Unbounded>>()
            == core::mem::size_of::<resident::Shared<'_>>()
    );
    assert!(
        core::mem::size_of::<
            data::Bound<compose::Tagged<resident::Shared<'_>, stream::Key>, bound::Unbounded>,
        >() == core::mem::size_of::<compose::Tagged<resident::Shared<'_>, stream::Key>>()
    );
};
