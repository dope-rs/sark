//! Fiber-free HTTP/3 server planning and execution binding.

use std::mem;

use book::input;
use dope::core::driver::schedule::ready::task::owner;
use dope_quic::{conn::id, wire::stream::Id};
use o3::{
    buffer::{segments, segments::assembly},
    depot::{capacity, map},
};
use requests::{book, entries};
use sark_core::{
    body,
    body::{buffered, credit, flow},
    duplex::machine,
    manifold,
    manifold::task::{Target, app::Runtime},
    route,
};
use startup::setup::Setup;

use crate::{conn::options, frame, manifold::requests};

pub mod deadline;
pub mod failure;
mod handler;
pub mod rotation;
mod session;
pub mod startup;
/// Server task typestates and runtime contracts.
pub mod task;

pub use handler::{Handler, mode::Mode, tasks::Tasks};
pub use session::Session;

fn credit_error_code(error: credit::Error) -> frame::ErrorCode {
    match error {
        credit::Error::ByteOverflow { .. }
        | credit::Error::ChunkOverflow { .. }
        | credit::Error::ByteLimit { .. }
        | credit::Error::ChunkLimit { .. } => frame::ErrorCode::ExcessiveLoad,
        credit::Error::ByteUnderflow { .. } | credit::Error::ChunkUnderflow { .. } => {
            frame::ErrorCode::Internal
        }
    }
}

fn body_error_code(error: &body::Error) -> frame::ErrorCode {
    match error {
        body::Error::TooLarge { .. } | body::Error::Overflow { .. } => {
            frame::ErrorCode::ExcessiveLoad
        }
        body::Error::LengthMismatch { .. }
        | body::Error::InputIncomplete
        | body::Error::InputFinished => frame::ErrorCode::Message,
        body::Error::SegmentMismatch => frame::ErrorCode::Internal,
        body::Error::Credit(error) => credit_error_code(*error),
    }
}

fn reject_error_code(error: route::Reject) -> frame::ErrorCode {
    match error {
        route::Reject::BadRequest => frame::ErrorCode::Message,
        route::Reject::Internal => frame::ErrorCode::Internal,
        route::Reject::Unavailable => frame::ErrorCode::RequestRejected,
    }
}

fn input_error_code<'d, R: Runtime<'d>>(
    error: input::Error<<R::Build as body::Push<'d>>::Error>,
) -> frame::ErrorCode {
    match error {
        input::Error::Request(error) => request_error_code(error),
        input::Error::Build(error) => reject_error_code(R::reject_push(error)),
    }
}

fn payload_error_code(error: buffered::Error<assembly::Error>) -> frame::ErrorCode {
    match error {
        buffered::Error::Body(error) => body_error_code(&error),
        buffered::Error::Retain(_) => frame::ErrorCode::ExcessiveLoad,
    }
}

fn flow_error_code<'d, R: Runtime<'d>>(
    error: flow::Error<<R::Build as body::Build<'d>>::StepError>,
) -> frame::ErrorCode {
    match error {
        flow::Error::LengthMismatch => frame::ErrorCode::Message,
        flow::Error::Credit(error) => credit_error_code(error),
        flow::Error::Build(error) => reject_error_code(R::reject_step(error)),
        flow::Error::Release { .. } => frame::ErrorCode::Internal,
    }
}

fn build_error_code<'d, R: Runtime<'d>>(
    error: entries::Failure<<R::Build as body::Build<'d>>::StepError>,
) -> frame::ErrorCode {
    match error {
        entries::Failure::Step(error) => flow_error_code::<R>(error),
        entries::Failure::Admit(error) => body_error_code(&error),
        entries::Failure::StepRelease { .. } | entries::Failure::AdmitRelease { .. } => {
            frame::ErrorCode::Internal
        }
    }
}

fn request_error_code(error: requests::Error) -> frame::ErrorCode {
    match error {
        requests::Error::Segments(
            segments::Error::SegmentCapacity(_)
            | segments::Error::ChainCapacity(_)
            | segments::Error::Retain(_),
        ) => frame::ErrorCode::ExcessiveLoad,
        requests::Error::Body(error) => body_error_code(&error),
        requests::Error::Payload(error) => payload_error_code(error),
        requests::Error::Malformed
        | requests::Error::Unsupported
        | requests::Error::Duplex(machine::FinishError::Finished | machine::FinishError::Closed) => {
            frame::ErrorCode::Message
        }
        requests::Error::Segments(
            segments::Error::DifferentPool
            | segments::Error::Count(_)
            | segments::Error::Logical(_)
            | segments::Error::Resident(_)
            | segments::Error::Total(_)
            | segments::Error::Boundary(_),
        )
        | requests::Error::Lookup(_)
        | requests::Error::Identity
        | requests::Error::ReadyStatus
        | requests::Error::Resume
        | requests::Error::Build
        | requests::Error::Dispatch => frame::ErrorCode::Internal,
    }
}

const DEFAULT_MAX_CONNECTION_BODY_BYTES: usize = 64 * 1024 * 1024;
/// One request may retain at most a quarter of the default connection budget.
const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = DEFAULT_MAX_CONNECTION_BODY_BYTES / 4;

/// Resource bounds for the built-in HTTP/3 manifold adapter.
#[derive(Clone)]
pub struct Config {
    /// Maximum retained body bytes of one request, mirroring h2's bound.
    pub max_request_body_bytes: usize,
    pub max_body_chunks: usize,
    pub max_connection_body_bytes: usize,
    pub max_connection_body_chunks: usize,
    pub protocol: options::Config,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            max_body_chunks: body::DEFAULT_MAX_REQUEST_CHUNKS,
            max_connection_body_bytes: DEFAULT_MAX_CONNECTION_BODY_BYTES,
            max_connection_body_chunks: body::DEFAULT_MAX_CONNECTION_CHUNKS,
            protocol: options::Config::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ConfigError {
    Protocol(options::Error),
    Requests(map::Capacity),
    Chunks(capacity::Error),
}

/// Exact connection/stream capability retained by one multiplexed H3 task.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct Handle<'d, const ID: u8> {
    connection: id::Handle<id::Driver<'d>, ID>,
    request: requests::Id,
}

// SAFETY: every `Handle` is minted by `Handle::from_request` from one resident
// connection handle, so each target names exactly one protocol identity domain.
unsafe impl<const ID: u8> manifold::raw::Target for Handle<'_, ID> {}

impl<const ID: u8> Target for Handle<'_, ID> {}

impl<'d, const ID: u8> owner::Source for Handle<'d, ID> {
    type Identity = id::Handle<id::Driver<'d>, ID>;
}

impl<'d, const ID: u8> Handle<'d, ID> {
    pub(in crate::manifold) fn from_request(
        conn: id::Handle<id::Driver<'d>, ID>,
        req: requests::Id,
    ) -> Self {
        Self {
            connection: conn,
            request: req,
        }
    }

    pub const fn connection(self) -> id::Handle<id::Driver<'d>, ID> {
        self.connection
    }

    pub(in crate::manifold) const fn request(self) -> requests::Id {
        self.request
    }

    pub const fn stream(self) -> Id {
        self.request.stream_id()
    }
}

const _: () = {
    assert!(
        mem::size_of::<Handle<'static, 0>>()
            == mem::size_of::<id::Handle<id::Driver<'static>, 0>>()
                + mem::size_of::<requests::Id>()
    );
    assert!(mem::size_of::<Option<Handle<'static, 0>>>() == mem::size_of::<Handle<'static, 0>>());
};
