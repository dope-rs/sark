use core::mem;

use conn::write;
use data::compose;
use dope::{
    core::driver::{self, route::Identity},
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
use output::{Error, Failure};
use sark_core::{
    life::{self, exchange, owners},
    manifold::Body,
};
use sark_http::owner::{self, cause};
use write::error;

use crate::{conn::event, frame::numbers, server::runtime::output, stream};

type Admission<'d, P, G> = Result<
    (
        egress::Load<data::Bound<P, G>>,
        <P as Evidence<'d>>::Receipt,
    ),
    (egress::Excess<P>, <P as Evidence<'d>>::Receipt),
>;

pub(in crate::server) struct Proof {
    stream: stream::Key,
    charge: hold::Charge,
}

#[derive(Clone, Copy)]
pub(in crate::server) enum Progress {
    Blocked,
    Committed,
}

enum Cycle<R> {
    Empty,
    Write(R),
}

pub(in crate::server) enum End {
    Close,
    Keep,
}

const _: () = {
    assert!(mem::size_of::<Progress>() == 1);
    assert!(mem::size_of::<End>() == mem::size_of::<bool>());
    assert!(mem::align_of::<End>() == mem::align_of::<bool>());
};

pub(in crate::server) trait Evidence<'d>: data::raw::Payload<'d> + Sized {
    type Write;
    type Receipt: Default;
    fn observe<C: Copy, M: life::Mode<C, stream::Key>>(
        _: Self::Receipt,
        _: &mut M,
        _: C,
        _: owner::Change,
    ) {
    }

    fn retain(stream: stream::Key, bytes: usize) -> Self::Write;

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
    fn retain(_: stream::Key, _: usize) {}

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

impl<'d, S: data::raw::Payload<'d>> Evidence<'d> for compose::Either<Body<'d, ()>, S> {
    type Write = ();
    type Receipt = ();
    fn retain(_: stream::Key, _: usize) {}

    fn admit<G: bound::Bound>(
        _: Self::Write,
        body: resident::Shared<'d>,
    ) -> Admission<'d, Self, G> {
        let body = egress::Load::new(Self::Left(Body::Shared {
            owner: body,
            tag: (),
        }))
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
    type Write = (stream::Key, usize);
    type Receipt = Option<Proof>;

    fn observe<C: Copy, M: life::Mode<C, stream::Key>>(
        receipt: Self::Receipt,
        mode: &mut M,
        conn: C,
        change: owner::Change,
    ) {
        if let Some(receipt) = receipt {
            owners::Projection::new(mode).apply(owner::Effect::new(
                exchange::Id::new(conn, receipt.stream),
                receipt.charge,
                change,
            ));
        }
    }

    fn retain(stream: stream::Key, bytes: usize) -> Self::Write {
        (stream, bytes)
    }

    fn admit<G: bound::Bound>(
        (stream, bytes): Self::Write,
        body: resident::Shared<'d>,
    ) -> Admission<'d, Self, G> {
        let body = egress::Load::new(body).tagged(stream);
        let write = Proof {
            stream,
            charge: hold::Charge::from_owner(bytes, body.charge().resident().get()),
        };
        match body.try_bound::<G>() {
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

pub(in crate::server) fn finish<'d, ID: Identity, W, C, E, G, M>(
    conn: &mut conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, W, C, data::Bound<E, G>>,
    driver: &mut driver::Context<'_, 'd>,
    error: Option<numbers::ErrorCode>,
    mode: &mut M,
) -> Result<Progress, (Failure, Progress)>
where
    W: wire::Spec,
    C: output::State<'d>,
    E: Evidence<'d>,
    G: bound::Bound,
    M: life::Mode<conn::Id<'d, ID>, stream::Key>,
{
    let end = match error {
        Some(error) => {
            conn.state_mut().protocol().control().goaway(error);
            End::Close
        }
        None => conn.state_mut().settle(),
    };
    let owner = conn.id();
    let mut progress = Progress::Blocked;
    loop {
        if !conn.state_mut().protocol().transfer().has_write() {
            if matches!(end, End::Close) {
                conn.close_after();
            }
            return Ok(progress);
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
                progress = Progress::Committed;
                E::observe(receipt, mode, owner, owner::Change::Handoff);
            }
            Ok(write::Outcome::Committed(Cycle::Empty)) => return Ok(progress),
            Ok(write::Outcome::Deferred(_) | write::Outcome::Closed) => {
                return Ok(progress);
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
                    Cycle::Write(receipt) => {
                        progress = Progress::Committed;
                        E::observe(
                            receipt,
                            mode,
                            owner,
                            owner::Change::StagingReleased(cause::Staging::Closed),
                        );
                    }
                    Cycle::Empty => {}
                }
                match failure {
                    Ok(failure) => {
                        conn.close_after();
                        return Err((failure, progress));
                    }
                    Err(failure) => conn.write().reject(failure),
                }
                return Ok(progress);
            }
        }
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
