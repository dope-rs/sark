use std::ops;

use dope::{core::driver::route::Identity, manifold::network::listener::conn};
use o3::buffer::resident;
use sark_core::{
    body,
    duplex::input::PushError,
    life,
    manifold::{stream::Boundary, task::app::Runtime},
};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::event,
    frame::numbers,
    server::{
        state,
        task::{
            app::*,
            ingress::Progress,
            response::{self, input},
            *,
        },
    },
};

pub(super) fn apply<'d, ID: Identity + 'd, R, Q, F: rules::Policy>(
    mut drive: drive::Drive<'_, '_, 'd, ID, R, Q, F>,
    stream: crate::stream::Key,
    data: event::DataPayload<resident::Shared<'d>>,
    boundary: Boundary,
) where
    R: Runtime<'d, Target = Target<'d, ID>>,
    Q: state::Transitions<conn::Id<'d, ID>>,
{
    let call = identify(drive.conn, stream);
    let progress = |mode: &mut Q| {
        state::Transitions::progress(mode, call);
        if let Boundary::End = boundary {
            mode.observe(life::Event::InputFinished(call));
        }
    };
    let data = match drive
        .state
        .try_push(stream, data, boundary, || progress(drive.mode))
    {
        Ok(ops::ControlFlow::Break(())) => return,
        Ok(ops::ControlFlow::Continue(data)) => data,
        Err(error) => {
            let code = match error {
                PushError::Buffer(rejected) => {
                    let (data, error) = rejected.into_parts();
                    data.discard();
                    error.code()
                }
                PushError::Finished(data) => {
                    data.discard();
                    numbers::ErrorCode::ProtocolError
                }
                PushError::Finish { input: data, error } => {
                    data.discard();
                    input::Error::Machine(error).code()
                }
            };
            drive.reset(stream, code);
            return;
        }
    };
    let mode = &mut *drive.mode;
    match drive
        .state
        .data()
        .push(stream, data, boundary, drive.limits, move || {
            progress(mode);
            mode.body(call)
        }) {
        Err(error) => drive.reset(stream, error.code::<R>()),
        Ok(Progress::Idle) => {}
        Ok(Progress::Streaming) => {
            if let Boundary::More = boundary {
                return;
            }
            let turn = drive.state.data().build().drive_for(
                stream,
                body::DEFAULT_BUILD_STEP_BUDGET,
                drive.mode.body(identify(drive.conn, stream)),
            );
            drive.build(turn);
        }
        Ok(Progress::Request(request)) => {
            // SAFETY: push removed this request from the same Drive's exact
            // incoming Book for immediate consumption without another handoff.
            unsafe { response::raw::buffered(drive, stream, request) };
        }
    }
}
