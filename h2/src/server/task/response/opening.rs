use std::ops;

use dope::{core::driver::route::Identity, manifold::network::listener::conn::Id};
use sark_core::{
    decode::request,
    duplex, life,
    manifold::{schedule, schedule::typed, stream::Boundary, task::app::Runtime},
};
use sark_http::{field::metadata::section::rules, payload::response::status::Code};

use crate::{
    conn,
    server::{
        response::{Handoff, Head, Status},
        state,
        task::{
            self, response,
            response::{Progress, input},
        },
    },
    stream,
};

impl<'d, ID: Identity + 'd, R, M, F, P> response::Driver<'_, 'd, ID, R, M, F, P>
where
    R: Runtime<'d, Target = task::Target<'d, ID>>,
    M: state::Transitions<Id<'d, ID>>,
    M::Retention: Handoff,
    F: rules::Policy,
    P: FnMut(Id<'d, ID>, stream::Key, &dyn std::fmt::Debug),
{
    pub(super) fn open(
        &mut self,
        stream: stream::Key,
        request: input::Request<'d, R::Prepared>,
    ) -> Progress<'d, R> {
        let app = self.router.as_ref().get_ref();
        let input = match request.into_body() {
            request::Body::Open(request) => match duplex::Application::try_open(app, request) {
                Ok((session, body)) => input::Input::open(session, input::Buf::new(body)),
                Err((error, ..)) => return self.reject_open(stream, error),
            },
            request::Body::Finished(request) => match duplex::Application::try_open(app, request) {
                Ok((session, body)) => input::Input::finished(session, input::Buf::new(body)),
                Err((error, ..)) => return self.reject_open(stream, error),
            },
        };
        let head = Status::new(Code::OK)
            .prepare(self.connection, stream, Boundary::More)
            .map_err(|error| self.error(stream, error))?;
        self.connection.hold_duplex(stream);
        self.resume_open(stream, head, input)
    }

    pub(super) fn resume_open(
        &mut self,
        stream: stream::Key,
        head: Head<'d>,
        input: input::Input<'d, R::Duplex>,
    ) -> Progress<'d, R> {
        let progress = match head.drive(self.connection) {
            Ok(progress) => progress,
            Err(error) => {
                drop(input);
                self.connection.release_duplex(stream);
                return Err(self.error(stream, error));
            }
        };
        match progress {
            ops::ControlFlow::Break(head) => Ok(typed::Control::Control {
                value: response::Response::DuplexHead { head, input },
                turn: schedule::Turn::Blocked(conn::DataBlock::Egress),
            }),
            ops::ControlFlow::Continue(receipt) => {
                drop(receipt.observe(self.mode, task::identify(self.connection_id, stream)));
                Ok(typed::Control::Data {
                    value: response::Data::Duplex { input },
                    turn: schedule::Turn::Progress,
                })
            }
        }
    }

    fn reject_open(
        &mut self,
        stream: stream::Key,
        error: duplex::OpenError<R::Error<'d>>,
    ) -> Progress<'d, R> {
        let status = match error {
            duplex::OpenError::InvalidRequest(error) => R::reject(error).status(),
            duplex::OpenError::Length => Code::BAD_REQUEST,
            duplex::OpenError::Unsupported => Code::INTERNAL_SERVER_ERROR,
        };
        let head = Status::new(status)
            .prepare(self.connection, stream, Boundary::End)
            .map_err(|error| self.error(stream, error))?;
        match head
            .drive(self.connection)
            .map_err(|error| self.error(stream, error))?
        {
            ops::ControlFlow::Break(head) => Ok(typed::Control::Control {
                value: response::Response::Head {
                    head,
                    state: sark_core::manifold::task::response::State::Complete(
                        life::Outcome::Rejected,
                    ),
                    count: response::Count::Pending,
                },
                turn: schedule::Turn::Blocked(conn::DataBlock::Egress),
            }),
            ops::ControlFlow::Continue(receipt) => {
                let exchange = task::identify(self.connection_id, stream);
                drop(receipt.observe(self.mode, exchange));
                exchange.retire(self.mode, life::Outcome::Rejected);
                Ok(typed::Control::Complete)
            }
        }
    }
}
