use dope::core::driver::schedule::app;
use o3::buffer::resident;
use sark_core::{
    decode::{self, request},
    manifold::{
        schedule,
        server::{
            Connection as _, Event,
            drain::{self, Events, Progress},
        },
    },
};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{self, event, machine},
    server::{
        self,
        state::exchange::{Deferred, Routed},
        sync::{Book, book::ingress},
    },
};

type Connection<'d, R> = machine::Connection<
    crate::ServerRole,
    <R as decode::Decode>::Plan,
    resident::Shared<'d>,
    rules::RejectPush,
    event::Ignore,
>;
type Incoming<'d, R> = Event<Connection<'d, R>>;

/// Executes the selected synchronous request family on its connection.
pub trait Dispatch<'d, S = <<Self as decode::Decode>::Head as request::Head>::Start>:
    decode::Decode<Prepared: request::Synchronous, Egress<'d>: 'd> + Sized
{
    fn ingress(
        &self,
        book: &mut Book<'d, Self>,
        conn: &mut Connection<'d, Self>,
        limits: server::Config,
        next: impl FnMut(&mut Connection<'d, Self>) -> Option<Incoming<'d, Self>>,
        report: impl FnMut(conn::Error),
    );

    fn events(
        &self,
        book: &mut Book<'d, Self>,
        conn: &mut Connection<'d, Self>,
        limits: server::Config,
    ) {
        self.ingress(
            book,
            conn,
            limits,
            |conn| conn.ready().map(drain::Ready::take),
            drop,
        );
        self.pump::<false, _>(
            book,
            conn,
            limits,
            schedule::Egress::Unchanged,
            || Some(()),
            drop,
        );
    }

    fn drive(
        &self,
        book: &mut Book<'d, Self>,
        conn: &mut Connection<'d, Self>,
        limits: server::Config,
        work: app::Work<'_, '_>,
        mut report: impl FnMut(conn::Error),
    ) -> Progress {
        let drain = Events::fair(work, |mut drain| {
            self.ingress(book, conn, limits, |conn| drain.next(conn), &mut report);
            drain.finish()
        });
        self.pump::<true, _>(
            book,
            conn,
            limits,
            schedule::Egress::Unchanged,
            || work.permit(),
            &mut report,
        );
        if conn.ingress().is_blocked() && book.has_ready() {
            self.pump::<false, _>(
                book,
                conn,
                limits,
                schedule::Egress::Unchanged,
                || work.permit(),
                &mut report,
            );
        }
        drain
    }

    fn pump<const ROUND: bool, P>(
        &self,
        book: &mut Book<'d, Self>,
        conn: &mut Connection<'d, Self>,
        limits: server::Config,
        egress: schedule::Egress,
        permit: impl FnMut() -> Option<P>,
        report: impl FnMut(conn::Error),
    ) -> schedule::Report;
    fn clear(&self, book: &mut Book<'d, Self>, conn: &mut Connection<'d, Self>);
}

impl<'d, R, S> Dispatch<'d, S> for R
where
    R: decode::Decode<Egress<'d>: 'd>,
    R::Head: request::Head<Start = S>,
    R::Prepared: request::Synchronous,
    S: ingress::head::Begin<'d, R>,
    <S as request::Selection<R::Prepared>>::Deferred<Routed<'d, R::Prepared, R::Routes>>:
        Deferred<'d, R::Prepared, R::Routes>,
{
    fn ingress(
        &self,
        book: &mut Book<'d, Self>,
        conn: &mut Connection<'d, Self>,
        limits: server::Config,
        next: impl FnMut(&mut Connection<'d, Self>) -> Option<Incoming<'d, Self>>,
        report: impl FnMut(conn::Error),
    ) {
        ingress::Ingress::new(book, conn).run(self, limits, next, report);
    }

    fn pump<const ROUND: bool, P>(
        &self,
        book: &mut Book<'d, Self>,
        conn: &mut Connection<'d, Self>,
        limits: server::Config,
        egress: schedule::Egress,
        permit: impl FnMut() -> Option<P>,
        report: impl FnMut(conn::Error),
    ) -> schedule::Report {
        book.pump::<ROUND, _, _, _>(conn, self, limits, egress, permit, report)
    }

    fn clear(&self, book: &mut Book<'d, Self>, conn: &mut Connection<'d, Self>) {
        book.clear(conn);
    }
}
