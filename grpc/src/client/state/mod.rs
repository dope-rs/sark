use sark_core::{
    life::session,
    manifold::{
        client::{Connection, domain::Plan, event::Reset},
        stream::Boundary,
    },
};
use sark_h2::{conn, stream};

use crate::{
    Reason,
    client::{
        call,
        receive::{self, policies},
        session::Create,
    },
    metadata,
    protocol::{request, section},
};

pub(super) mod calls;
pub(crate) mod output;
pub(in crate::client) mod plan;
mod ticket;

pub(super) struct State<P, C>
where
    C: session::Storage,
    P: Plan<C, Connection: Connection<Error = conn::Error>>,
{
    pub(in crate::client) scheme: request::Scheme,
    pub(super) connection: P::Connection,
    pub(super) calls: calls::Store<P, C>,
    pub(super) egress: output::Output,
    pub(super) ingress: receive::Assembler<P, C>,
}

pub(super) struct Started<K, T> {
    pub(super) stream: K,
    pub(super) completion: T,
}

impl<P, C> State<P, C>
where
    C: session::Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
{
    pub(super) fn try_new(
        plan: plan::Plan<P>,
        conn: P::Connection,
    ) -> Result<Self, Create<P::Build, P::Storage>> {
        let limits = plan.limits;
        Ok(Self {
            scheme: plan.scheme,
            connection: conn,
            calls: calls::Store::try_new(
                limits.call_count(),
                limits.output().owner_count(),
                limits.bytes(),
            )
            .map_err(|error| match error {
                calls::Build::Domain(error) => Create::CallDomain(error),
                calls::Build::Requests(error) => Create::CallRequests(error),
            })?,
            egress: output::Output::try_new(limits).map_err(Create::Output)?,
            ingress: receive::Assembler::try_new(limits).map_err(Create::Ingress)?,
        })
    }

    pub(super) fn start_reply<T>(
        &mut self,
        path: &[u8],
        peer: Option<&[u8]>,
        metadata: &metadata::Metadata,
    ) -> Result<Started<P::Key, T>, crate::Status>
    where
        T: ticket::Ticket<P, C>,
    {
        if !self.ingress.phase.is_open() {
            return Err(crate::Status::new(
                crate::Code::Unavailable,
                Reason::ConnectionDraining,
            ));
        }
        let headers = request::Fields::new(path, peer, metadata)?.with_scheme(self.scheme);
        let (key, target) = T::reserve(&mut self.ingress)?;
        let stream = match self.connection.start(headers, Boundary::More) {
            Ok(stream) => stream,
            Err(error) => {
                self.release(target);
                return Err(crate::Status::from(error));
            }
        };
        if let Err(error) = self.calls.insert(stream, target) {
            let reset = self.connection.reset(stream, Reset::Refused);
            self.release(target);
            return Err(crate::Status::insert(error, reset));
        }
        Ok(Started {
            stream,
            completion: key,
        })
    }

    fn release(&mut self, target: calls::Target) {
        match target {
            calls::Target::Unary(key) => {
                self.ingress.cancel_unary(key);
            }
            calls::Target::Streaming(key) => {
                self.ingress.cancel_feed(key);
            }
        }
    }

    pub(super) fn poll_unary<R: policies::Policy<P, C>>(
        &mut self,
        key: receive::UnaryKey,
        policy: &mut R,
    ) -> Result<receive::Progress<receive::UnaryResult<C>>, crate::Status> {
        let result = match self.ingress.poll_unary(key)? {
            receive::Progress::Ready(result) => result,
            receive::Progress::Pending(index) => return Ok(receive::Progress::Pending(index)),
        };
        self.refill(policy);
        Ok(receive::Progress::Ready(result?))
    }

    pub(super) fn cancel_unary<R: policies::Policy<P, C>>(
        &mut self,
        stream: P::Key,
        key: receive::UnaryKey,
        policy: &mut R,
    ) {
        if self.cancel(stream, policy).is_none() {
            self.ingress.cancel_unary(key);
        }
        self.refill(policy);
    }

    pub(super) fn poll_stream<R: policies::Policy<P, C>>(
        &mut self,
        key: receive::StreamKey,
        policy: &mut R,
    ) -> Result<receive::Progress<receive::Response<C>>, crate::Status> {
        match self.ingress.poll_stream(key)? {
            receive::Progress::Ready(result) => {
                self.refill(policy);
                Ok(receive::Progress::Ready(result?))
            }
            receive::Progress::Pending(index) => {
                self.ingress.take_error()?;
                Ok(receive::Progress::Pending(index))
            }
        }
    }

    pub(super) fn cancel_feed<R: policies::Policy<P, C>>(
        &mut self,
        stream: P::Key,
        key: receive::StreamKey,
        policy: &mut R,
    ) {
        if self.cancel(stream, policy).is_none() {
            self.ingress.cancel_feed(key);
        }
        self.refill(policy);
    }

    pub(super) fn refill<R: policies::Policy<P, C>>(&mut self, policy: &mut R) {
        self.ingress.refill(
            &mut self.connection,
            &mut self.calls,
            &mut self.egress,
            policy,
        );
    }

    pub(super) fn cancel<R: policies::Policy<P, C>>(
        &mut self,
        stream: P::Key,
        policy: &mut R,
    ) -> Option<calls::Target> {
        self.ingress.discard(stream);
        let record = self
            .egress
            .cancel(&mut self.calls, &mut self.connection, stream)?;
        policy.cancel(&mut self.ingress, record.response)
    }

    pub(super) fn send_bytes<R: policies::Policy<P, C>>(
        &mut self,
        stream: P::Key,
        payload: &[u8],
        boundary: Boundary,
        policy: &mut R,
    ) -> Result<(), crate::Status> {
        let result = self.egress.send_bytes(
            &mut self.calls,
            &mut self.connection,
            stream,
            payload,
            boundary,
            &mut |record| {
                let _ = policy.cancel(&mut self.ingress, record.response);
            },
        );
        result.inspect_err(|_| {
            let _ = self.cancel(stream, policy);
        })
    }

    pub(super) fn start_single<
        T: ticket::Ticket<P, C>,
        D: crate::Codec,
        R: policies::Policy<P, C>,
    >(
        &mut self,
        path: &[u8],
        peer: Option<&[u8]>,
        metadata: &metadata::Metadata,
        codec: &mut D,
        message: &D::Encode,
        policy: &mut R,
    ) -> call::Admission<Started<P::Key, T>> {
        let payload = match self.egress.reserve(codec, message) {
            call::Admission::Ready(request) => request,
            call::Admission::Pending => return call::Admission::Pending,
            call::Admission::Rejected(status) => {
                return call::Admission::Rejected(status);
            }
        };
        let started = match self.start_reply::<T>(path, peer, metadata) {
            Ok(started) => started,
            Err(status) => return call::Admission::Rejected(status),
        };
        let result = self.egress.send(
            &mut self.calls,
            &mut self.connection,
            started.stream,
            payload,
            Boundary::End,
            &mut |record| {
                let _ = policy.cancel(&mut self.ingress, record.response);
            },
        );
        if let Err(status) = result {
            let _ = self.cancel(started.stream, policy);
            return call::Admission::Rejected(status);
        }
        call::Admission::Ready(started)
    }

    pub(super) fn send<D: crate::Codec, R: policies::Policy<P, C>>(
        &mut self,
        stream: P::Key,
        codec: &mut D,
        message: &D::Encode,
        policy: &mut R,
    ) -> call::Admission<()> {
        let payload = match self.egress.reserve(codec, message) {
            call::Admission::Ready(request) => request,
            call::Admission::Pending => return call::Admission::Pending,
            call::Admission::Rejected(status) => {
                return call::Admission::Rejected(status);
            }
        };
        let result = self.egress.send(
            &mut self.calls,
            &mut self.connection,
            stream,
            payload,
            Boundary::More,
            &mut |record| {
                let _ = policy.cancel(&mut self.ingress, record.response);
            },
        );
        if let Err(status) = result {
            let _ = self.cancel(stream, policy);
            return call::Admission::Rejected(status);
        }
        call::Admission::Ready(())
    }

    pub(super) fn finish<R: policies::Policy<P, C>>(
        &mut self,
        stream: P::Key,
        policy: &mut R,
    ) -> Result<(), crate::Status> {
        let ingress = &mut self.ingress;
        let result = self.egress.finish(
            &mut self.calls,
            &mut self.connection,
            stream,
            &mut |record| {
                let _ = policy.cancel(ingress, record.response);
            },
        );
        result.inspect_err(|_| {
            let _ = self.cancel(stream, policy);
        })
    }
}
