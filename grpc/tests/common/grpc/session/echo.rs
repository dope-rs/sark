use std::cell::Cell;

use dope_test::alloc::Tracker;
use o3::buffer;
use sark_grpc::server::service::{
    Request, Response,
    handler::{Handler, RequestState},
    reply::Write,
    timers,
};

pub struct Echo<'a>(pub &'a Cell<Option<usize>>);

impl Handler for Echo<'_> {
    type Call = ();
    type Timer = timers::Off;

    fn request<P: buffer::raw::Segment, W: Write>(
        &mut self,
        &mut (): &mut (),
        &mut (): &mut (),
        request: Request<'_, P>,
        response: &mut Response<'_, W>,
    ) -> RequestState {
        for (index, message) in request.messages.iter().enumerate() {
            let mut payload = Vec::with_capacity(message.len());
            for segment in message.payload.segments() {
                payload.extend_from_slice(segment.as_ref());
            }
            let ((), used) = Tracker::measure(|| response.push_message(payload));
            if index != 0 {
                self.0.set(Some(used[0]));
            }
        }
        RequestState::Complete
    }
}
