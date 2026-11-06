use o3::{
    buffer::{resident, segments},
    mem,
};
use sark_core::{
    body,
    decode::{exec, request},
    duplex::{self, input, machine},
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

pub(in crate::server::task) struct Buf<'d> {
    input: Option<segments::Chain<resident::Shared<'d>>>,
}

impl<'d> Buf<'d> {
    pub(super) const fn new(input: Option<segments::Chain<resident::Shared<'d>>>) -> Self {
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

/// H2 carrier storage using the protocol-neutral input reducer.
pub(in crate::server::task) type Input<'d, D> = input::State<'d, D, Buf<'d>>;

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
