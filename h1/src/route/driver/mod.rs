//! Mutable resources for direct, synchronous HTTP/1 routing.

use core::pin;
use o3::buffer::write::Slice;
use std::{convert::Infallible, ops::ControlFlow};

use o3::{alloc, mem::Rejected};
use sark_http::{
    h1::{codec::canned, head::line::error},
    payload::{compress, fixed, owned},
};

use crate::{
    pump,
    route::{Router, set},
    session, switch,
    wire::{Failure, cache, head, parsing},
};

mod routes;

type Connection<'io, S, T> = session::Conn<
    'io,
    (),
    <T as set::Table<S>>::Pending<'io>,
    switch::Disabled,
    Infallible,
    <T as set::Table<S>>::Head,
>;

type Outcome<'io, S, T> = session::RouteOutcome<
    'io,
    <T as set::Table<S>>::Pending<'io>,
    Infallible,
    <T as set::Table<S>>::Head,
    parsing::Rejected<'io>,
>;

/// Executor-local HTTP/1 policy state for one protocol-neutral router.
pub struct Driver<'router, R: Router> {
    response_cache: Box<[Option<cache::Entry>]>,
    gzip: <R::Table as set::Table<R::State>>::Compression,
    date: [u8; 29],
    router: &'router R,
}

impl<'router, R: Router> Driver<'router, R> {
    /// Reserves every route cache before the listener accepts connections.
    pub fn try_new(router: &'router R, date: [u8; 29]) -> Result<Self, alloc::Error> {
        let count = if <R::Table as set::Table<R::State>>::HAS_CACHE {
            <R::Table as set::Table<R::State>>::LEN
        } else {
            0
        };
        Ok(Self {
            response_cache: alloc::BoxSliceExt::try_default(count)?,
            gzip: compress::Compression::try_new()?,
            date,
            router,
        })
    }

    /// Drives one raw transport chunk through the fiber-free connection.
    pub fn ingest<'io>(
        &mut self,
        conn: &mut Connection<'io, R::State, R::Table>,
        input: &[u8],
        write: Slice<'_, &mut usize>,
        prior: head::Connection,
    ) -> pump::Cycle<'io> {
        let input = match pump::Ingress::preflight(conn, input) {
            pump::Preflight::Drive => input,
            pump::Preflight::Resume => &[],
            pump::Preflight::Settled | pump::Preflight::Finished => {
                return pump::Cycle::settled(conn.ingress.discard_remaining);
            }
            pump::Preflight::Rejected(error) => {
                return pump::Cycle::failed(error, conn.ingress.discard_remaining);
            }
        };
        let mut direct = &mut *self;
        pump::Ingress::drive(conn, pin::Pin::new(&mut direct), input, write, prior)
    }

    /// Retains a chunk when a transport cannot currently lend a write buffer.
    pub fn retain<'io>(
        &self,
        conn: &mut Connection<'io, R::State, R::Table>,
        input: &[u8],
    ) -> pump::Cycle<'io> {
        match pump::Ingress::preflight(conn, input) {
            pump::Preflight::Drive => pump::Cycle::retained(pump::Ingress::retain(conn, input)),
            pump::Preflight::Resume => pump::Cycle::retained(pump::Ingress::retain(conn, &[])),
            pump::Preflight::Settled | pump::Preflight::Finished => {
                pump::Cycle::settled(conn.ingress.discard_remaining)
            }
            pump::Preflight::Rejected(error) => {
                pump::Cycle::failed(error, conn.ingress.discard_remaining)
            }
        }
    }
    fn select<'turn, 'io>(
        &'turn mut self,
        permit: session::Permit,
        write: Slice<'turn, &'turn mut usize>,
        conn: &'turn mut Connection<'io, R::State, R::Table>,
    ) -> routes::Select<'turn, 'io, R> {
        routes::Select {
            router: self.router,
            egress: routes::Egress {
                response_cache: &mut self.response_cache,
                gzip: &mut self.gzip,
                date: &self.date,
                write,
            },
            permit,
            conn,
        }
    }
}

impl<'io, R: Router> session::Dispatch<'io, (), <R::Table as set::Table<R::State>>::Pending<'io>>
    for &mut Driver<'_, R>
{
    type Ready = Infallible;
    type Head = <R::Table as set::Table<R::State>>::Head;

    fn failure(self: pin::Pin<&mut Self>, cause: &crate::wire::Failure) -> fixed::Body<'io> {
        self.router.failure(cause)
    }

    fn try_consume(
        mut self: pin::Pin<&mut Self>,
        permit: session::Permit,
        input: crate::Input<'_>,
        write: Slice<'_, &mut usize>,
        conn: &mut Connection<'io, R::State, R::Table>,
    ) -> Outcome<'io, R::State, R::Table> {
        let methods = <R::Table as set::Table<R::State>>::METHODS;
        let parsed = match parsing::Head::parse(input, methods) {
            Ok(Some(parsed)) => parsed,
            Ok(None) => {
                return session::Outcome::NeedMore {
                    permit,
                    state: session::NeedMore::Line,
                }
                .into();
            }
            Err(error::Request::Syntax | error::Request::Target(..) | error::Request::Span(..)) => {
                return session::Outcome::Close(canned::BAD_REQUEST).into();
            }
        };
        let select = self.select(permit, write, conn);
        match <R::Table as set::Table<R::State>>::find(&parsed, select) {
            Ok(route) => route,
            Err(select) => parsing::Rejected::start(&parsed, select.permit, canned::NOT_FOUND),
        }
    }

    fn consume_line(
        mut self: pin::Pin<&mut Self>,
        permit: session::Permit,
        source: parsing::Source,
        write: Slice<'_, &mut usize>,
        conn: &mut Connection<'io, R::State, R::Table>,
    ) -> Outcome<'io, R::State, R::Table> {
        let methods = <R::Table as set::Table<R::State>>::METHODS;
        let owner = match parsing::Owned::try_parse(source, methods).map_err(Rejected::discard) {
            Ok(ControlFlow::Continue(owner)) => owner,
            Ok(ControlFlow::Break(_)) => {
                return session::Outcome::NeedMore {
                    permit,
                    state: session::NeedMore::Line,
                }
                .into();
            }
            Err(error::Request::Syntax | error::Request::Target(..) | error::Request::Span(..)) => {
                return session::Outcome::Close(canned::BAD_REQUEST).into();
            }
        };
        let select = self.select(permit, write, conn);
        match <R::Table as set::Table<R::State>>::find_owned(owner, select) {
            Ok(route) => route,
            Err((owner, select)) => {
                parsing::Rejected::start_owned(owner, select.permit, canned::NOT_FOUND)
            }
        }
    }

    fn resume_head(
        mut self: pin::Pin<&mut Self>,
        pending: Self::Head,
        permit: session::Permit,
        input: crate::Input<'_>,
        write: Slice<'_, &mut usize>,
        conn: &mut Connection<'io, R::State, R::Table>,
    ) -> Outcome<'io, R::State, R::Table> {
        <R::Table as set::Table<R::State>>::resume_head(
            pending,
            routes::Resume {
                input,
                select: self.select(permit, write, conn),
            },
        )
    }

    fn body_data(
        self: pin::Pin<&mut Self>,
        mut wait: <R::Table as set::Table<R::State>>::Pending<'io>,
        body: owned::Segments<'io>,
        rt: &mut (),
    ) -> Result<<R::Table as set::Table<R::State>>::Pending<'io>, fixed::Body<'io>> {
        let () = *rt;
        <R::Table as set::Table<R::State>>::edit(&mut wait, body)
            .map_err(|error| self.router.failure(&Failure::Assembly(error)))?;
        Ok(wait)
    }

    fn has_body_work(_: &<R::Table as set::Table<R::State>>::Pending<'io>) -> bool {
        false
    }

    fn resume_pending(
        self: pin::Pin<&mut Self>,
        wait: <R::Table as set::Table<R::State>>::Pending<'io>,
        _: &mut (),
    ) -> Result<<R::Table as set::Table<R::State>>::Pending<'io>, fixed::Body<'io>> {
        Ok(wait)
    }

    fn finish_body(
        mut self: pin::Pin<&mut Self>,
        wait: <R::Table as set::Table<R::State>>::Pending<'io>,
        body: session::Complete,
        permit: session::Permit,
        write: Slice<'_, &mut usize>,
        rt: &mut (),
    ) -> session::BodyOutcome<'io, Self::Ready> {
        let () = *rt;
        let this = &mut *self;
        session::BodyOutcome::Complete(<R::Table as set::Table<R::State>>::take(
            wait,
            routes::Finish {
                router: this.router,
                egress: routes::Egress {
                    response_cache: &mut this.response_cache,
                    gzip: &mut this.gzip,
                    date: &this.date,
                    write,
                },
                body,
                permit,
            },
        ))
    }

    fn resume_body(
        self: pin::Pin<&mut Self>,
        ready: Self::Ready,
        _: session::Permit,
        _: Slice<'_, &mut usize>,
        _: &mut (),
    ) -> session::BodyOutcome<'io, Self::Ready> {
        match ready {}
    }
}
