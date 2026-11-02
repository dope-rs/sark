use std::ops;

use dope::{core::driver::route::Identity, manifold::network::listener::conn::Id};
use o3::buffer::hold;
use sark_core::{
    decode::request,
    duplex, life,
    manifold::{schedule, schedule::typed, stream::Boundary, task::app::Runtime},
};
use sark_http::payload::response::status::Code;

use crate::{
    conn,
    server::{
        response::{Head, Receipt, Status},
        state,
        task::{
            self, response,
            response::{Progress, input},
        },
    },
    stream,
};

pub(super) struct Opening<'a, 'b, 'd, ID: Identity, R, M>
where
    R: Runtime<'d>,
{
    pub(super) driver: &'a mut response::Driver<'b, 'd, ID, R, M>,
}

impl<'d, ID: Identity + 'd, R, M> Opening<'_, '_, 'd, ID, R, M>
where
    R: Runtime<'d, Target = task::Target<'d, ID>>,
    M: state::Transitions<Id<'d, ID>>,
{
    pub(super) fn open(
        self,
        stream: stream::Key,
        request: input::Request<'d, R::Prepared>,
    ) -> Progress<'d, R> {
        let app = self.driver.router.as_ref().get_ref();
        let input = match request.into_body() {
            request::Body::Open(request) => match duplex::Application::try_open(app, request) {
                Ok((session, body)) => input::Input::open(session, body),
                Err((error, ..)) => return self.reject(stream, error),
            },
            request::Body::Finished(request) => match duplex::Application::try_open(app, request) {
                Ok((session, body)) => input::Input::finished(session, body),
                Err((error, ..)) => return self.reject(stream, error),
            },
        };
        let head = Status::new(Code::OK)
            .prepare(self.driver.connection, stream, Boundary::More)
            .map_err(conn::Error::code)?;
        self.resume(stream, head, input)
    }

    pub(super) fn resume(
        mut self,
        stream: stream::Key,
        head: Head<'d>,
        input: input::Input<'d, R::Duplex>,
    ) -> Progress<'d, R> {
        match head
            .drive(self.driver.connection)
            .map_err(conn::Error::code)?
        {
            ops::ControlFlow::Break(head) => Ok(typed::Control::Control {
                value: response::Response::DuplexHead { head, input },
                turn: Self::blocked(),
            }),
            ops::ControlFlow::Continue(receipt) => {
                self.committed(stream, receipt);
                Ok(typed::Control::Data {
                    value: response::Data::Duplex { input },
                    turn: Self::progress(),
                })
            }
        }
    }

    fn reject(
        mut self,
        stream: stream::Key,
        error: duplex::OpenError<R::Error<'d>>,
    ) -> Progress<'d, R> {
        let status = match error {
            duplex::OpenError::InvalidRequest(error) => R::reject(error).status(),
            duplex::OpenError::Length => Code::BAD_REQUEST,
            duplex::OpenError::Unsupported => Code::INTERNAL_SERVER_ERROR,
        };
        let head = Status::new(status)
            .prepare(self.driver.connection, stream, Boundary::End)
            .map_err(conn::Error::code)?;
        match head
            .drive(self.driver.connection)
            .map_err(conn::Error::code)?
        {
            ops::ControlFlow::Break(head) => Ok(typed::Control::Control {
                value: response::Response::Head {
                    head,
                    outcome: life::Outcome::Rejected,
                    count: response::Count::Pending,
                },
                turn: Self::blocked(),
            }),
            ops::ControlFlow::Continue(receipt) => {
                self.committed(stream, receipt);
                task::identify(self.driver.connection_id, stream)
                    .retire(self.driver.mode, life::Outcome::Rejected);
                Ok(typed::Control::Complete)
            }
        }
    }

    fn committed(&mut self, stream: stream::Key, receipt: Receipt<'d>) {
        let (_, linked, copied) = receipt.into_parts();
        let exchange = task::identify(self.driver.connection_id, stream);
        if let Some(copied) = copied {
            self.driver.mode.observe(life::Event::ResponseBodyCopied {
                exchange,
                read: copied.read,
                written: copied.written,
                cause: copied.cause,
            });
        }
        self.driver.mode.observe(life::Event::ResponseCommitted {
            exchange,
            body_bytes: 0,
            charge: hold::Charge::default(),
            linked,
        });
    }

    fn blocked() -> schedule::Turn<conn::DataBlock> {
        schedule::Turn::Blocked(conn::DataBlock::Egress)
    }

    fn progress() -> schedule::Turn<conn::DataBlock> {
        schedule::Turn::Progress
    }
}
