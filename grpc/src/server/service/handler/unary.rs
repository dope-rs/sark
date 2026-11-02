use o3::buffer;

use crate::server::service::{self, handler::modes};

pub(in crate::server) fn decode<'a, C: crate::Codec, P: buffer::raw::Segment + 'a>(
    codec: &mut C,
    request: service::Request<'a, P>,
) -> Result<modes::Request<C::Decode<'a, P>>, crate::Status> {
    let message = request.messages.try_one()?;
    Ok(modes::Request {
        stream_id: request.stream_id,
        head: request.head,
        body: crate::Codec::decode(codec, message.view())?,
        trailers: request.trailers,
    })
}

pub(in crate::server) fn own<C: crate::Codec, P: buffer::raw::Segment>(
    codec: &mut C,
    request: service::Request<'_, P>,
) -> Result<modes::Request<C::Owned<P>>, crate::Status> {
    let message = request.messages.into_frame()?;
    Ok(modes::Request {
        stream_id: request.stream_id,
        head: request.head,
        body: crate::Codec::decode_frame(codec, message)?,
        trailers: request.trailers,
    })
}
