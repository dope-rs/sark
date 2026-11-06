use core::mem;

use o3::{
    alloc,
    buffer::{
        hold::ranges,
        segments::assembly::{self, total},
    },
};
use ranges::retain;
use sark_core::{
    body::{self, admit},
    request, route,
};
use sark_http::{
    h1::{chunk, codec::canned},
    payload::fixed::Body,
    request::body::Error,
};

use crate::wire::{
    InputError,
    body::{View, chunked, fixed, ingress},
    output,
};

#[derive(Debug)]
pub enum Failure {
    BadRequest,
    PayloadTooLarge,
    Admission(body::Error),
    Assembly(assembly::Error),
    Projection(Error),
    Bind(request::BindError),
    Route(route::Reject),
    Upgrade(sark_ws::upgrade::Error),
    Fixed(fixed::Error),
    Chunk(chunked::Decode),
    View(retain::Error),
    Ingress(ingress::Error),
    Input(InputError),
    Output(output::Error),
    Pump(crate::pump::Reject),
}

impl Failure {
    pub(crate) fn reject(error: admit::Error<View<'_, '_>>) -> Self {
        let cause = match &error {
            admit::Error::Admission(failure) => failure.error(),
            admit::Error::Storage(failure) => failure.error(),
        };
        Self::Admission(*cause)
    }

    pub fn body<'d>(&self) -> Body<'d> {
        match self {
            Self::BadRequest | Self::Bind(_) => canned::BAD_REQUEST,
            Self::Route(route::Reject::BadRequest) => canned::BAD_REQUEST,
            Self::Route(route::Reject::Internal) => canned::INTERNAL_SERVER_ERROR,
            Self::Route(route::Reject::Unavailable) => canned::SERVICE_UNAVAILABLE,
            Self::Upgrade(_) => Body::new(sark_ws::server::Response::REJECTION),
            Self::PayloadTooLarge => canned::PAYLOAD_TOO_LARGE,
            Self::Projection(error) => match error {
                Error::Borrowed { .. } | Error::Retention { .. } => canned::INTERNAL_SERVER_ERROR,
                Error::Owned(_) => canned::SERVICE_UNAVAILABLE,
            },
            Self::Admission(error) => match error {
                body::Error::TooLarge { .. } | body::Error::Overflow { .. } => {
                    canned::PAYLOAD_TOO_LARGE
                }
                body::Error::LengthMismatch { .. }
                | body::Error::InputIncomplete
                | body::Error::InputFinished
                | body::Error::SegmentMismatch
                | body::Error::Credit(_) => canned::BAD_REQUEST,
            },
            Self::Fixed(error) => match error {
                fixed::Error::Bad => canned::BAD_REQUEST,
                fixed::Error::Ingress(error) => Self::ingress(error),
            },
            Self::Chunk(error) => Self::chunk(error),
            Self::Assembly(error) => Self::assembly(error),
            Self::View(error) => Self::view(error),
            Self::Ingress(error) => Self::ingress(error),
            Self::Input(error) => Self::input(error),
            Self::Output(error) => error.body(),
            Self::Pump(_) => canned::INTERNAL_SERVER_ERROR,
        }
    }

    fn assembly<'d>(error: &assembly::Error) -> Body<'d> {
        match error {
            assembly::Error::Metadata(alloc::Error::Exhausted)
            | assembly::Error::Nodes {
                allocation: alloc::Error::Exhausted,
                ..
            }
            | assembly::Error::Descriptors {
                allocation: alloc::Error::Exhausted,
                ..
            }
            | assembly::Error::Total(total::Error::Resident(_)) => canned::SERVICE_UNAVAILABLE,
            assembly::Error::Metadata(alloc::Error::Overflow)
            | assembly::Error::Nodes {
                allocation: alloc::Error::Overflow,
                ..
            }
            | assembly::Error::Descriptors {
                allocation: alloc::Error::Overflow,
                ..
            } => canned::INTERNAL_SERVER_ERROR,
            assembly::Error::Total(total::Error::Count(_) | total::Error::Logical(_)) => {
                canned::PAYLOAD_TOO_LARGE
            }
        }
    }

    fn chunk<'d>(error: &chunked::Decode) -> Body<'d> {
        match error {
            chunked::Decode::Wire(
                chunk::Error::Limit {
                    axis: chunk::Axis::Body,
                    ..
                }
                | chunk::Error::Overflow {
                    axis: chunk::Axis::Body | chunk::Axis::Size,
                    ..
                },
            )
            | chunked::Decode::Range { .. } => canned::PAYLOAD_TOO_LARGE,
            chunked::Decode::Framing => canned::BAD_REQUEST,
            chunked::Decode::Wire(
                chunk::Error::Syntax { .. }
                | chunk::Error::Forbidden { .. }
                | chunk::Error::Limit { .. }
                | chunk::Error::Overflow { .. },
            ) => canned::BAD_REQUEST,
            chunked::Decode::Ingress(error) => Self::ingress(error),
            chunked::Decode::Reserve(_) => canned::SERVICE_UNAVAILABLE,
        }
    }

    fn view<'d>(error: &retain::Error) -> Body<'d> {
        match error {
            retain::Error::Metadata(_) | retain::Error::Retain { .. } => {
                canned::SERVICE_UNAVAILABLE
            }
            retain::Error::Resident { .. } => canned::PAYLOAD_TOO_LARGE,
        }
    }

    fn ingress<'d>(error: &ingress::Error) -> Body<'d> {
        match error {
            ingress::Error::Range(_) | ingress::Error::Ranges(_) => canned::BAD_REQUEST,
            ingress::Error::Storage(_)
            | ingress::Error::Assembly(_)
            | ingress::Error::Metadata(_)
            | ingress::Error::Retain { .. }
            | ingress::Error::Resident { .. }
            | ingress::Error::Reserve(_) => canned::SERVICE_UNAVAILABLE,
        }
    }

    fn input<'d>(error: &InputError) -> Body<'d> {
        match error {
            InputError::Range(_) => canned::BAD_REQUEST,
            InputError::Storage(_) | InputError::Share(_) => canned::SERVICE_UNAVAILABLE,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Failure>() <= 6 * mem::size_of::<usize>());
    assert!(!mem::needs_drop::<Failure>());
};
