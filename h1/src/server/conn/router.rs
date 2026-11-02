use std::pin;

use dope::{core::driver::route::Identity, manifold::network::listener::conn};
use o3::buffer::{hold, resident::bound};
use sark_http::payload::{fixed, owned};

use crate::{
    Input,
    server::date,
    session, switch,
    wire::{Failure, parsing},
};

pub trait Routes<'d, Runtime, ID: Identity, Transitions: switch::Engine = switch::Disabled> {
    type Pending;
    type Ready;
    type Head;
    type TransportTag: Copy + Default + 'd;
    type Egress: bound::Bound + 'd;

    /// Receives exact terminal failures without changing response ownership.
    fn failure(self: pin::Pin<&mut Self>, cause: &crate::wire::Failure) -> fixed::Body<'d> {
        cause.body()
    }

    fn reject_line(
        self: pin::Pin<&mut Self>,
        _: conn::Id<'d, ID>,
        _: session::Permit,
        _: &mut session::Conn<'d, Runtime, Self::Pending, Transitions, Self::Ready, Self::Head>,
        error: parsing::LineError<'d>,
    ) -> fixed::Body<'d> {
        error.response(|cause| self.failure(cause))
    }

    fn abort_body(
        self: pin::Pin<&mut Self>,
        conn: &mut session::Conn<'d, Runtime, Self::Pending, Transitions, Self::Ready, Self::Head>,
        error: Failure,
    ) -> fixed::Body<'d>;

    fn response_tag(
        self: pin::Pin<&mut Self>,
        id: conn::Id<'d, ID>,
        runtime: &mut Runtime,
        charge: hold::Charge,
    ) -> Self::TransportTag;

    fn accept(
        self: pin::Pin<&mut Self>,
        id: conn::Id<'d, ID>,
        runtime: &mut Runtime,
        tag: Self::TransportTag,
    );

    fn cancel(
        self: pin::Pin<&mut Self>,
        id: conn::Id<'d, ID>,
        runtime: &mut Runtime,
        tag: Self::TransportTag,
    );

    fn try_consume(
        self: pin::Pin<&mut Self>,
        id: conn::Id<'d, ID>,
        date: &date::Clock,
        permit: session::Permit,
        input: Input<'_>,
        write: &mut [u8],
        conn: &mut session::Conn<'d, Runtime, Self::Pending, Transitions, Self::Ready, Self::Head>,
    ) -> session::RouteOutcome<'d, Self::Pending, Transitions::Raw, Self::Head, parsing::Rejected<'d>>;

    fn consume_line(
        self: pin::Pin<&mut Self>,
        id: conn::Id<'d, ID>,
        date: &date::Clock,
        permit: session::Permit,
        source: parsing::Source,
        write: &mut [u8],
        conn: &mut session::Conn<'d, Runtime, Self::Pending, Transitions, Self::Ready, Self::Head>,
    ) -> session::RouteOutcome<'d, Self::Pending, Transitions::Raw, Self::Head, parsing::Rejected<'d>>;

    fn resume_head(
        routes: Dispatch<'_, 'd, Self, ID>,
        pending: Self::Head,
        permit: session::Permit,
        input: Input<'_>,
        write: &mut [u8],
        conn: &mut session::Conn<'d, Runtime, Self::Pending, Transitions, Self::Ready, Self::Head>,
    ) -> session::RouteOutcome<'d, Self::Pending, Transitions::Raw, Self::Head, parsing::Rejected<'d>>;

    fn resume_error(
        self: pin::Pin<&mut Self>,
        _: conn::Id<'d, ID>,
        pending: parsing::Rejected<'d>,
        permit: session::Permit,
        input: Input<'_>,
        _: &mut session::Conn<'d, Runtime, Self::Pending, Transitions, Self::Ready, Self::Head>,
    ) -> session::RouteOutcome<'d, Self::Pending, Transitions::Raw, Self::Head, parsing::Rejected<'d>>
    {
        pending.resume(input, permit)
    }

    fn body_data(
        self: pin::Pin<&mut Self>,
        id: conn::Id<'d, ID>,
        wait: Self::Pending,
        body: owned::Segments<'d>,
        rt: &mut Runtime,
    ) -> Result<Self::Pending, fixed::Body<'d>>;

    fn has_body_work(pending: &Self::Pending) -> bool;

    fn resume_pending(
        self: pin::Pin<&mut Self>,
        id: conn::Id<'d, ID>,
        pending: Self::Pending,
        runtime: &mut Runtime,
    ) -> Result<Self::Pending, fixed::Body<'d>>;

    fn finish_body(
        self: pin::Pin<&mut Self>,
        wait: Self::Pending,
        body: session::Complete,
        permit: session::Permit,
        write: &mut [u8],
        date: &[u8; 29],
        rt: &mut Runtime,
    ) -> session::BodyOutcome<'d, Self::Ready, Transitions::Raw>;

    fn resume_body(
        self: pin::Pin<&mut Self>,
        id: conn::Id<'d, ID>,
        ready: Self::Ready,
        permit: session::Permit,
        write: &mut [u8],
        date: &[u8; 29],
        rt: &mut Runtime,
    ) -> session::BodyOutcome<'d, Self::Ready, Transitions::Raw>;
}

/// Exact router borrow, connection identity and worker-local date source.
pub struct Dispatch<'a, 'd, H: ?Sized, ID: Identity> {
    pub app: pin::Pin<&'a mut H>,
    pub id: conn::Id<'d, ID>,
    pub date: &'a date::Clock,
}

impl<'d, H, Runtime, ID: Identity, Transitions>
    session::Dispatch<'d, Runtime, H::Pending, Transitions> for Dispatch<'_, 'd, H, ID>
where
    H: Routes<'d, Runtime, ID, Transitions>,
    Transitions: switch::Engine,
{
    type Ready = H::Ready;
    type Head = H::Head;

    fn failure(self: pin::Pin<&mut Self>, cause: &crate::wire::Failure) -> fixed::Body<'d> {
        Routes::failure(self.get_mut().app.as_mut(), cause)
    }

    fn reject_line(
        self: pin::Pin<&mut Self>,
        permit: session::Permit,
        conn: &mut session::Conn<'d, Runtime, H::Pending, Transitions, H::Ready, H::Head>,
        error: parsing::LineError<'d>,
    ) -> fixed::Body<'d> {
        let this = self.get_mut();
        Routes::reject_line(this.app.as_mut(), this.id, permit, conn, error)
    }

    fn abort_body(
        self: pin::Pin<&mut Self>,
        conn: &mut session::Conn<'d, Runtime, H::Pending, Transitions, H::Ready, H::Head>,
        error: Failure,
    ) -> fixed::Body<'d> {
        Routes::abort_body(self.get_mut().app.as_mut(), conn, error)
    }

    fn try_consume(
        self: pin::Pin<&mut Self>,
        permit: session::Permit,
        input: Input<'_>,
        write: &mut [u8],
        conn: &mut session::Conn<'d, Runtime, H::Pending, Transitions, H::Ready, H::Head>,
    ) -> session::RouteOutcome<'d, H::Pending, Transitions::Raw, Self::Head, parsing::Rejected<'d>>
    {
        let this = self.get_mut();
        Routes::try_consume(
            this.app.as_mut(),
            this.id,
            this.date,
            permit,
            input,
            write,
            conn,
        )
    }

    fn consume_line(
        self: pin::Pin<&mut Self>,
        permit: session::Permit,
        source: parsing::Source,
        write: &mut [u8],
        conn: &mut session::Conn<'d, Runtime, H::Pending, Transitions, H::Ready, H::Head>,
    ) -> session::RouteOutcome<'d, H::Pending, Transitions::Raw, Self::Head, parsing::Rejected<'d>>
    {
        let this = self.get_mut();
        Routes::consume_line(
            this.app.as_mut(),
            this.id,
            this.date,
            permit,
            source,
            write,
            conn,
        )
    }

    fn resume_head(
        self: pin::Pin<&mut Self>,
        pending: Self::Head,
        permit: session::Permit,
        input: Input<'_>,
        write: &mut [u8],
        conn: &mut session::Conn<'d, Runtime, H::Pending, Transitions, Self::Ready, Self::Head>,
    ) -> session::RouteOutcome<'d, H::Pending, Transitions::Raw, Self::Head, parsing::Rejected<'d>>
    {
        let this = self.get_mut();
        H::resume_head(
            Dispatch {
                app: this.app.as_mut(),
                id: this.id,
                date: this.date,
            },
            pending,
            permit,
            input,
            write,
            conn,
        )
    }

    fn resume_error(
        self: pin::Pin<&mut Self>,
        pending: parsing::Rejected<'d>,
        permit: session::Permit,
        input: Input<'_>,
        _: &mut [u8],
        conn: &mut session::Conn<'d, Runtime, H::Pending, Transitions, Self::Ready, Self::Head>,
    ) -> session::RouteOutcome<'d, H::Pending, Transitions::Raw, Self::Head, parsing::Rejected<'d>>
    {
        let this = self.get_mut();
        Routes::resume_error(this.app.as_mut(), this.id, pending, permit, input, conn)
    }

    fn body_data(
        self: pin::Pin<&mut Self>,
        wait: H::Pending,
        body: owned::Segments<'d>,
        rt: &mut Runtime,
    ) -> Result<H::Pending, fixed::Body<'d>> {
        let this = self.get_mut();
        Routes::body_data(this.app.as_mut(), this.id, wait, body, rt)
    }

    fn has_body_work(pending: &H::Pending) -> bool {
        H::has_body_work(pending)
    }

    fn resume_pending(
        self: pin::Pin<&mut Self>,
        pending: H::Pending,
        runtime: &mut Runtime,
    ) -> Result<H::Pending, fixed::Body<'d>> {
        let this = self.get_mut();
        Routes::resume_pending(this.app.as_mut(), this.id, pending, runtime)
    }

    fn finish_body(
        self: pin::Pin<&mut Self>,
        wait: H::Pending,
        body: session::Complete,
        permit: session::Permit,
        write: &mut [u8],
        rt: &mut Runtime,
    ) -> session::BodyOutcome<'d, Self::Ready, Transitions::Raw> {
        let this = self.get_mut();
        let date = this.date.load();
        Routes::finish_body(this.app.as_mut(), wait, body, permit, write, &date, rt)
    }

    fn resume_body(
        self: pin::Pin<&mut Self>,
        ready: Self::Ready,
        permit: session::Permit,
        write: &mut [u8],
        rt: &mut Runtime,
    ) -> session::BodyOutcome<'d, Self::Ready, Transitions::Raw> {
        let this = self.get_mut();
        let date = this.date.load();
        Routes::resume_body(this.app.as_mut(), this.id, ready, permit, write, &date, rt)
    }
}
