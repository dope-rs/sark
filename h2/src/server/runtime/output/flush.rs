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
        let outcome = 'write: {
            let mut native_slot = mem::MaybeUninit::uninit();
            let mut native = match conn.write().try_open(driver, &mut native_slot) {
                Ok(native) => native,
                Err(error::Admission::Deferred(_) | error::Admission::Closed) => {
                    return Ok(progress);
                }
                Err(error::Admission::Rejected(cause)) => {
                    break 'write Err((Ok(Failure::Capacity(cause)), None));
                }
            };
            let prepared = 'prepare: {
                let (state, mut prefix) = native.get_mut().parts();
                let bound = match state
                    .protocol()
                    .transfer()
                    .bind_link::<{ write::HEADER_CAPACITY }>()
                {
                    Ok(Some(bound)) => bound,
                    Ok(None) => break 'prepare Ok((None, None, write::End::Open)),
                    Err(cause) => break 'prepare Err(Error::Input(cause)),
                };
                let drained = bound.is_drained();
                let output = match bound.commit_with(&mut prefix, E::retain) {
                    Ok(output) => output,
                    Err(cause) => break 'prepare Err(Error::Prefix(cause)),
                };
                let admitted = match output {
                    event::Write::Buffered(_) => Ok((None, E::Receipt::default())),
                    event::Write::Owned {
                        stream_id: stream,
                        prefix_len: _,
                        body,
                    } => E::admit_owned::<G>(stream, body)
                        .map(|(body, receipt)| (Some(body), receipt)),
                    event::Write::Split {
                        write,
                        prefix_len: _,
                        body,
                    } => E::admit::<G>(write, body).map(|(body, receipt)| (Some(body), receipt)),
                };
                let (body, receipt) = match admitted {
                    Ok(output) => output,
                    Err((cause, receipt)) => {
                        break 'prepare Err(Error::Bound {
                            cause,
                            receipt: Some(receipt),
                        });
                    }
                };
                let end = match (&end, drained) {
                    (End::Close, true) => write::End::Close,
                    (End::Close, false) | (End::Keep, _) => write::End::Open,
                };
                Ok((body.map(egress::load::Value::Unchecked), Some(receipt), end))
            };
            let (body, receipt, end) = match prepared {
                Ok(prepared) => prepared,
                Err(cause) => {
                    drop(native);
                    break 'write Err(match cause {
                        Error::Input(cause) => (Ok(Failure::Input(cause)), None),
                        Error::Prefix(cause) => (Ok(Failure::Prefix(cause)), None),
                        Error::Bound { cause, receipt } => (Err(cause.into()), receipt),
                    });
                }
            };
            let published = match body {
                Some(body) => write::Output::try_publish(native, body, end),
                None => write::Output::finish(native, end),
            };
            match published {
                Ok(()) => Ok(receipt),
                Err(cause) => Err((Ok(Failure::Mismatch(cause.into_cause())), receipt)),
            }
        };
        match outcome {
            Ok(Some(receipt)) => {
                progress = Progress::Committed;
                E::observe(receipt, mode, owner, owner::Change::Handoff);
            }
            Ok(None) => return Ok(progress),
            Err((failure, receipt)) => {
                match receipt {
                    Some(receipt) => {
                        progress = Progress::Committed;
                        E::observe(
                            receipt,
                            mode,
                            owner,
                            owner::Change::StagingReleased(cause::Staging::Closed),
                        );
                    }
                    None => {}
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
