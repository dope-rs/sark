use o3::buffer;
use sark_grpc::{
    Status,
    server::service::{
        Request, Response,
        handler::{Handler, RequestState},
        reply::Write,
        timers,
    },
};
use sark_h2::stream::Id;

pub struct Accept<'a>(pub &'a mut Vec<Id>);

impl Handler for Accept<'_> {
    type Call = ();
    type Timer = timers::Off;

    fn request<P: buffer::raw::Segment, W: Write>(
        &mut self,
        &mut (): &mut (),
        &mut (): &mut (),
        request: Request<'_, P>,
        response: &mut Response<'_, W>,
    ) -> RequestState {
        self.0.push(request.stream_id);
        response.finish_with(Status::ok());
        RequestState::Complete
    }
}
