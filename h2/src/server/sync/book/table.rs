use o3::buffer::resident;
use sark_core::{
    decode::{self, request},
    manifold::schedule::{self, typed},
};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{self, event, machine},
    server,
    server::{
        response,
        state::exchange::Deferred,
        sync::book::{self, Book, Delayed, Receiving, driver, phase::Phase},
    },
    stream,
};

impl<'d, R> Book<'d, R>
where
    R: decode::Decode,
    R::Prepared: request::Synchronous,
{
    pub(in crate::server::sync) fn pump<'a, const ROUND: bool, B, P, D>(
        &'a mut self,
        conn: &'a mut machine::Connection<
            crate::ServerRole,
            R::Plan,
            resident::Shared<'d>,
            rules::RejectPush,
            event::Ignore,
        >,
        router: &'a R,
        limits: server::Config,
        egress: schedule::Egress,
        permit: B,
        report: D,
    ) -> schedule::Report
    where
        'd: 'a,
        B: FnMut() -> Option<P>,
        D: FnMut(conn::Error),
        driver::Driver<'a, 'd, R, B, D>: typed::Driver<
                Phase<'d, R>,
                response::Pending<'d>,
                Receiving<'d, R>,
                Key = stream::Key,
                Block = conn::DataBlock,
            >,
    {
        self.runtime.resume(|| conn.flow().poll_wake(), egress);
        let driver = &mut driver::Driver {
            connection: conn,
            router,
            limits,
            credit: &mut self.credit,
            permit,
            report,
        };
        match ROUND {
            true => self.runtime.drive_round(driver),
            false => self.runtime.drive(driver),
        }
    }

    pub(in crate::server::sync) fn clear(
        &mut self,
        conn: &mut machine::Connection<
            crate::ServerRole,
            R::Plan,
            resident::Shared<'d>,
            rules::RejectPush,
            event::Ignore,
        >,
    ) where
        Delayed<'d, R>: Deferred<'d, R::Prepared, R::Routes>,
    {
        if !self.runtime.is_empty() {
            for index in 0..self.runtime.capacity() {
                let Some(stream) = self.runtime.key_at(index) else {
                    continue;
                };
                if let Some(phase) = self.runtime.remove(stream) {
                    // SAFETY: this phase was removed from this account's own runtime.
                    match unsafe { book::raw::try_close(phase, &mut self.credit) } {
                        Ok(Some(head)) => head.discard(conn),
                        Ok(None) => {}
                        Err(error) => conn.streams().cancel(stream, error.code()),
                    }
                }
            }
        }
        self.runtime.clear();
    }
}
