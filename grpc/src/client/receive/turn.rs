use sark_core::{
    life::session,
    manifold::{
        client::{
            self, Connection,
            domain::Plan,
            event::{self, Classify as _, Response, Shutdown},
            io::Read as _,
        },
        schedule,
        stream::{Boundary, Message as _, Section},
    },
};
use sark_h2::{conn::Error, stream};
use state::{calls, output};

use crate::{
    Reason,
    client::{
        receive::{self, data, policies},
        state,
    },
    message,
    protocol::{response, section},
};

pub(super) enum Pending<P, C>
where
    C: session::Storage,
    P: Plan<C, Connection: Connection<Error = Error>>,
{
    Head {
        stream: P::Key,
        fields: section::Owner,
        section: Section,
        boundary: Boundary,
    },
    Data {
        stream: P::Key,
        data: message::DataChunk<C, P::Permit>,
        boundary: Boundary,
        ready: Option<message::Frame<C>>,
    },
    Reset {
        stream: P::Key,
        status: crate::Status,
    },
    GoAway {
        boundary: <P::Connection as client::Connection>::GoAway,
        shutdown: Shutdown,
        next: usize,
    },
}

pub(super) enum Step<P, C>
where
    C: session::Storage,
    P: Plan<C, Connection: Connection<Error = Error>>,
{
    Complete,
    Blocked(Pending<P, C>),
}

#[repr(u8)]
enum Drain {
    Idle,
    Progress,
    Blocked,
}

const _: () = assert!(core::mem::size_of::<Drain>() == core::mem::size_of::<u8>());

pub(super) struct Turn<'a, P, C, R>
where
    C: session::Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = Error>,
        >,
    R: policies::Policy<P, C>,
{
    pub(super) assembler: &'a mut receive::Assembler<P, C>,
    pub(super) connection: &'a mut P::Connection,
    pub(super) calls: &'a mut calls::Store<P, C>,
    pub(super) egress: &'a mut output::Output,
    pub(super) policy: &'a mut R,
}

impl<'a, P, C, R> Turn<'a, P, C, R>
where
    C: session::Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = Error>,
        >,
    R: policies::Policy<P, C>,
{
    pub(super) fn new(
        assembly: &'a mut receive::Assembler<P, C>,
        conn: &'a mut P::Connection,
        calls: &'a mut calls::Store<P, C>,
        egress: &'a mut output::Output,
        policy: &'a mut R,
    ) -> Self {
        Self {
            assembler: assembly,
            connection: conn,
            calls,
            egress,
            policy,
        }
    }

    pub(super) fn run(mut self, mut result: Result<(), Error>) -> Result<(), crate::Status> {
        loop {
            let drain = self.drain()?;
            let assembly = &mut *self.assembler;
            let policy = &mut *self.policy;
            self.calls.drive(
                self.connection,
                schedule::Egress::Unchanged,
                &mut |record| {
                    let _ = policy.cancel(assembly, record.response);
                },
            )?;
            match (drain, &result) {
                (_, Ok(())) => return Ok(()),
                (Drain::Blocked, Err(error)) if error.kind() == event::Error::Blocked => {
                    return Ok(());
                }
                (Drain::Progress, Err(error)) if error.kind() == event::Error::Blocked => {
                    result = self.connection.resume();
                }
                (_, Err(_)) => return result.map_err(crate::Status::from),
            }
        }
    }

    fn drain(&mut self) -> Result<Drain, crate::Status> {
        let mut progress = Drain::Idle;
        loop {
            let pending = match self.assembler.pending.take() {
                Some(pending) => pending,
                None => {
                    let Some(event) = self.connection.poll() else {
                        return Ok(progress);
                    };
                    progress = Drain::Progress;
                    Self::pending(event)
                }
            };
            match self.step(pending)? {
                Step::Complete => {}
                Step::Blocked(pending) => {
                    self.assembler.pending = Some(pending);
                    return Ok(Drain::Blocked);
                }
            }
        }
    }

    fn pending(event: Response<P::Connection>) -> Pending<P, C> {
        match event {
            Response::Head {
                message,
                fields,
                selection: (),
                section,
            } => Pending::Head {
                stream: message.key(),
                fields,
                section,
                boundary: message.boundary(),
            },
            Response::Data { message, data } => Pending::Data {
                stream: message.key(),
                data: message::DataChunk::new(data),
                boundary: message.boundary(),
                ready: None,
            },
            Response::Reset { stream, reason } => Pending::Reset {
                stream,
                status: crate::Status::reset(reason),
            },
            Response::GoAway { boundary, shutdown } => Pending::GoAway {
                boundary,
                shutdown,
                next: 0,
            },
        }
    }

    fn step(&mut self, pending: Pending<P, C>) -> Result<Step<P, C>, crate::Status> {
        match pending {
            Pending::Head {
                stream,
                fields,
                section,
                boundary,
            } => self.head(stream, fields, section, boundary),
            Pending::Data {
                stream,
                data,
                boundary,
                ready,
            } => data::Data::new(self, stream, data, boundary, ready).run(),
            Pending::Reset { stream, status } => self.reset(stream, status),
            Pending::GoAway {
                boundary,
                shutdown,
                next,
            } => self.goaway(boundary, shutdown, next),
        }
    }

    fn head(
        &mut self,
        stream: P::Key,
        fields: section::Owner,
        section: Section,
        boundary: Boundary,
    ) -> Result<Step<P, C>, crate::Status> {
        if matches!(section, Section::Trailing) {
            if matches!(boundary, Boundary::More) {
                return Err(crate::Status::new(
                    crate::Code::Internal,
                    Reason::TrailersWithoutEnd,
                ));
            }
            let response = self
                .response(stream)
                .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::EventUnknown))?;
            let mode = response.target.mode();
            let http = response.http_status;
            if !self.assembler.claims.try_ready(mode)? {
                return Ok(Step::Blocked(Pending::Head {
                    stream,
                    fields,
                    section,
                    boundary,
                }));
            }
            let status = match crate::Status::from_owner(fields, section, http) {
                Ok(status) => status,
                Err(status) => return self.reset(stream, status),
            };
            self.finish(stream, status)?;
            return Ok(Step::Complete);
        }

        let response = self
            .calls
            .response_mut(stream)
            .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::EventUnknown))?;
        let mode = response.target.mode();
        if !self.assembler.claims.try_ready(mode)?
            && (mode == calls::ResponseMode::Streaming || matches!(boundary, Boundary::End))
        {
            return Ok(Step::Blocked(Pending::Head {
                stream,
                fields,
                section,
                boundary,
            }));
        }
        if matches!(boundary, Boundary::End) {
            let status = match crate::Status::from_owner(fields, section, None) {
                Ok(status) => status,
                Err(status) => return self.reset(stream, status),
            };
            self.finish(stream, status)?;
            return Ok(Step::Complete);
        }
        let head = match response::Head::from_owner(fields) {
            Ok(head) => head,
            Err(status) => return self.reset(stream, status),
        };
        let status = head.http_status();
        if status.is_info() {
            return Ok(Step::Complete);
        }
        response.http_status = Some(status);
        response.grpc = head.is_grpc();
        let metadata = head.into_inner();
        match response.target {
            calls::Target::Unary(_) => {
                response.metadata = metadata;
            }
            calls::Target::Streaming(target) => {
                self.policy.push_event(
                    self.assembler,
                    target,
                    Ok(receive::Response::Headers { metadata }),
                    None,
                )?;
            }
        }
        Ok(Step::Complete)
    }

    fn reset(
        &mut self,
        stream: P::Key,
        status: crate::Status,
    ) -> Result<Step<P, C>, crate::Status> {
        let record = self
            .calls
            .get(stream)
            .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::ResetUnknown))?;
        let Some(response) = record.response.as_ref() else {
            self.cancel(stream);
            return Ok(Step::Complete);
        };
        let mode = response.target.mode();
        if !self.assembler.claims.try_ready(mode)? {
            return Ok(Step::Blocked(Pending::Reset { stream, status }));
        }
        self.finish_reset(stream, status)?;
        Ok(Step::Complete)
    }

    /// Closed admission keeps the visited prefix complete; cancellation only removes slots.
    fn goaway(
        &mut self,
        boundary: <P::Connection as client::Connection>::GoAway,
        shutdown: Shutdown,
        next: usize,
    ) -> Result<Step<P, C>, crate::Status> {
        self.assembler.phase.observe(shutdown);
        for index in next..self.calls.capacity() {
            let Some(stream) = self.calls.key_at(index) else {
                continue;
            };
            if matches!(shutdown, Shutdown::Clean) && self.connection.is_accepted(&boundary, stream)
            {
                continue;
            }
            let Some(response) = self.response(stream) else {
                continue;
            };
            if !self.assembler.claims.try_ready(response.target.mode())? {
                return Ok(Step::Blocked(Pending::GoAway {
                    boundary,
                    shutdown,
                    next: index,
                }));
            }
            let reason = match shutdown {
                Shutdown::Clean => Reason::ShutdownPending,
                Shutdown::Failed => Reason::ShutdownFailed,
            };
            self.finish_reset(stream, crate::Status::new(crate::Code::Unavailable, reason))?;
        }
        Ok(Step::Complete)
    }

    pub(super) fn finish(
        &mut self,
        stream: P::Key,
        status: crate::Status,
    ) -> Result<(), crate::Status> {
        let response = self
            .calls
            .finish(stream)
            .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::TerminalUnknown))?;
        let result = if response.deframer.is_idle() {
            Ok(status)
        } else {
            Err(message::Error::Incomplete.into())
        };
        self.publish(response, result)
    }

    pub(super) fn finish_reset(
        &mut self,
        stream: P::Key,
        status: crate::Status,
    ) -> Result<(), crate::Status> {
        let mut record = self
            .egress
            .cancel(self.calls, self.connection, stream)
            .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::ResetUnknown))?;
        let response = record
            .response
            .take()
            .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::TerminalUnknown))?;
        self.publish(response, Err(status))
    }

    fn publish(
        &mut self,
        response: calls::Response<C>,
        result: Result<crate::Status, crate::Status>,
    ) -> Result<(), crate::Status> {
        let calls::Response {
            target,
            metadata,
            message,
            ..
        } = response;
        let (message, charge) = match message {
            Some(calls::Buffered { frame, charge }) => (Some(frame), Some(charge)),
            None => (None, None),
        };
        match target {
            calls::Target::Unary(target) => {
                let value = match result {
                    Ok(status) => Ok(receive::UnaryResult {
                        metadata,
                        message,
                        status,
                    }),
                    Err(status) => Err(status),
                };
                self.policy
                    .push_unary(self.assembler, target, value, charge)?;
            }
            calls::Target::Streaming(target) => {
                let value = match result {
                    Ok(status) => Ok(receive::Response::Trailers { status }),
                    Err(status) => Err(status),
                };
                self.policy
                    .push_event(self.assembler, target, value, charge)?;
            }
        }
        Ok(())
    }

    fn response(&self, stream: P::Key) -> Option<&calls::Response<C>> {
        self.calls
            .get(stream)
            .and_then(|record| record.response.as_ref())
    }

    pub(super) fn cancel(&mut self, stream: P::Key) {
        if let Some(record) = self.egress.cancel(self.calls, self.connection, stream) {
            let _ = self.policy.cancel(self.assembler, record.response);
        }
    }
}
