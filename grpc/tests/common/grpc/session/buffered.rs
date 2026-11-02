use o3::buffer;
use sark_grpc::{
    message,
    server::service::{
        Request, Response,
        handler::{Handler, RequestState},
        reply::Write,
        timers,
    },
};

pub struct Join;

impl Handler for Join {
    type Call = ();
    type Timer = timers::Off;

    fn request<P: buffer::raw::Segment, W: Write>(
        &mut self,
        &mut (): &mut (),
        &mut (): &mut (),
        request: Request<'_, P>,
        response: &mut Response<'_, W>,
    ) -> RequestState {
        let mut payload = Vec::new();
        if request.head.path() == b"/input/one" {
            match request.messages.try_one() {
                Ok(message) => append(message, &mut payload),
                Err(status) => {
                    response.finish_with(status);
                    return RequestState::Complete;
                }
            }
        } else {
            let mut messages = request.messages.into_iter();
            for message in messages.by_ref() {
                match message {
                    Ok(message) => append(message, &mut payload),
                    Err(status) => {
                        response.finish_with(status);
                        return RequestState::Complete;
                    }
                }
            }
            assert_eq!(
                (messages.size_hint(), messages.next().is_none()),
                ((0, Some(0)), true)
            );
        }
        response.push_message(payload);
        RequestState::Complete
    }
}

fn append<P: buffer::raw::Segment>(message: &message::Frame<P>, payload: &mut Vec<u8>) {
    for segment in message.payload.segments() {
        payload.extend_from_slice(segment.as_ref());
    }
}
