use std::cell::Cell;

use o3::buffer;
use sark_grpc::{
    Status,
    server::service::{
        self,
        handler::{self, Handler},
        reply::Write,
        timers,
    },
};

pub struct Borrowed<'a>(pub &'a Cell<u32>);

impl Handler for Borrowed<'_> {
    type Call = u32;
    type Timer = timers::Off;

    fn request<P: buffer::raw::Segment, W: Write>(
        &mut self,
        &mut (): &mut (),
        call: &mut Self::Call,
        _: service::Request<'_, P>,
        output: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        *call += 1;
        self.0.set(self.0.get() + 1);
        output.finish_with(Status::ok());
        handler::RequestState::Complete
    }
}
