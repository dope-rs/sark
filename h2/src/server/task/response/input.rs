use o3::{
    buffer::{resident, segments},
    mem,
};
use sark_core::{
    body,
    decode::{exec, request},
    duplex::{self, admit, input, machine},
    manifold::stream::Boundary,
};
use sark_http::payload::owned;

use crate::{conn::event, frame::numbers, server::payload};

type Admitted<'d, P> =
    request::Admitted<exec::Duplex, P, Option<segments::Chain<resident::Shared<'d>>>>;
type Body<'d, P> = request::Body<Admitted<'d, P>>;

#[repr(transparent)]
pub(in crate::server::task) struct Request<'d, P>(Body<'d, P>);

pub(in crate::server::task) enum Error {
    Input(payload::Error),
    Machine(machine::FinishError),
}

impl Error {
    pub(in crate::server::task) const fn code(&self) -> numbers::ErrorCode {
        match self {
            Self::Input(error) => error.code(),
            Self::Machine(machine::FinishError::Finished | machine::FinishError::Closed) => {
                numbers::ErrorCode::ProtocolError
            }
        }
    }
}

impl<'d, P> Request<'d, P> {
    pub(in crate::server::task) const fn open(req: Admitted<'d, P>) -> Self {
        Self(request::Body::Open(req))
    }

    pub(in crate::server::task) const fn finished(req: Admitted<'d, P>) -> Self {
        Self(request::Body::Finished(req))
    }

    pub(super) fn append(
        &mut self,
        data: event::DataPayload<resident::Shared<'d>>,
    ) -> Result<(), mem::Rejected<event::DataPayload<resident::Shared<'d>>, payload::Error>> {
        let req = match &mut self.0 {
            request::Body::Open(req) => req,
            request::Body::Finished(_) => {
                return Err(mem::Rejected::new(
                    data,
                    payload::Error::Body(body::Error::InputFinished),
                ));
            }
        };
        data.try_append(req.body_mut()).map_err(|rejected| {
            let (input, error) = rejected.into_parts();
            mem::Rejected::new(input, payload::Error::Segments(error))
        })
    }

    pub(super) fn try_finish(self) -> Result<Self, mem::Rejected<Self, payload::Error>> {
        match self.0 {
            request::Body::Open(req) => Ok(Self(request::Body::Finished(req))),
            request::Body::Finished(req) => Err(mem::Rejected::new(
                Self(request::Body::Finished(req)),
                payload::Error::Body(body::Error::InputFinished),
            )),
        }
    }

    pub(super) fn try_end(
        self,
        data: event::DataPayload<resident::Shared<'d>>,
    ) -> Result<
        Self,
        (
            mem::Rejected<event::DataPayload<resident::Shared<'d>>, payload::Error>,
            Self,
        ),
    > {
        match self.0 {
            request::Body::Open(mut req) => {
                let result = data.try_append(req.body_mut()).map_err(|rejected| {
                    let (input, error) = rejected.into_parts();
                    mem::Rejected::new(input, payload::Error::Segments(error))
                });
                match result {
                    Ok(()) => Ok(Self(request::Body::Finished(req))),
                    Err(fail) => Err((fail, Self(request::Body::Open(req)))),
                }
            }
            request::Body::Finished(req) => Err((
                mem::Rejected::new(data, payload::Error::Body(body::Error::InputFinished)),
                Self(request::Body::Finished(req)),
            )),
        }
    }

    pub(super) fn into_body(self) -> Body<'d, P> {
        self.0
    }
}

struct Buf<'d> {
    input: Option<segments::Chain<resident::Shared<'d>>>,
}

impl<'d> Buf<'d> {
    const fn new(input: Option<segments::Chain<resident::Shared<'d>>>) -> Self {
        Self { input }
    }
}

impl<'d> input::Buf<'d> for Buf<'d> {
    type Input = event::DataPayload<resident::Shared<'d>>;
    type Error = payload::Error;
    type Receipt = ();

    fn state(&self) -> input::Buffer {
        match self.input.as_ref() {
            None => input::Buffer::Empty,
            Some(value) if value.is_empty() => input::Buffer::Empty,
            Some(_) => input::Buffer::Queued,
        }
    }

    fn push(&mut self, input: Self::Input) -> Result<(), mem::Rejected<Self::Input, Self::Error>> {
        input.try_append(&mut self.input).map_err(|rejected| {
            let (input, error) = rejected.into_parts();
            mem::Rejected::new(input, payload::Error::Segments(error))
        })
    }

    fn take(&mut self) -> input::Take<'d, Self::Receipt> {
        let Some(source) = self.input.take() else {
            return input::Take::Empty;
        };
        if source.is_empty() {
            return input::Take::Empty;
        }
        input::Take::Item {
            input: owned::Segments::from(source),
            receipt: (),
        }
    }
}

/// H2 carrier storage around the protocol-neutral input reducer.
#[repr(transparent)]
pub(in crate::server::task) struct Input<'d, D>(input::State<'d, D, Buf<'d>>)
where
    D: duplex::Session<'d>;

impl<'d, D> Input<'d, D>
where
    D: duplex::Session<'d>,
{
    pub(super) fn open(session: D, input: Option<segments::Chain<resident::Shared<'d>>>) -> Self {
        Self(input::State::open(session, Buf::new(input)))
    }

    pub(super) fn finished(
        session: D,
        input: Option<segments::Chain<resident::Shared<'d>>>,
    ) -> Self {
        Self(input::State::finished(session, Buf::new(input)))
    }

    pub(super) const fn status(&self) -> input::Status {
        self.0.status()
    }

    pub(super) fn push(
        &mut self,
        data: event::DataPayload<resident::Shared<'d>>,
        boundary: Boundary,
    ) -> Result<(), input::PushError<event::DataPayload<resident::Shared<'d>>, payload::Error>>
    {
        self.0.push(data, boundary)
    }

    pub(super) fn finish(&mut self) -> Result<(), machine::FinishError> {
        self.0.finish()
    }

    pub(super) fn work(&self) -> input::Work {
        self.0.work()
    }

    pub(super) fn transact<R, E>(
        &mut self,
        admit: impl for<'turn> FnOnce(
            admit::Emission<'turn, 'd, D>,
        ) -> admit::Admission<'turn, 'd, D, R, E>,
    ) -> input::Turn<machine::Transaction<R, E>, ()> {
        self.0.transact(admit)
    }
}

const _: () = {
    assert!(size_of::<Request<'static, ()>>() == size_of::<Body<'static, ()>>());
    assert!(align_of::<Request<'static, ()>>() == align_of::<Body<'static, ()>>());
    assert!(
        size_of::<Input<'static, duplex::Never>>()
            <= size_of::<Option<segments::Chain<resident::Shared<'static>>>>()
                + size_of::<machine::Machine<'static, duplex::Never>>()
                + 8
    );
};
