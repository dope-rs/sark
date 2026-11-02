use o3::{buffer, buffer::storage};
use sark_h2::stream::Id;

use crate::{metadata, protocol::request, server::service::messages};

pub struct Request<'a, C: buffer::raw::Segment = storage::Shared> {
    pub stream_id: Id,
    pub head: request::Head,
    pub messages: messages::Messages<'a, C>,
    pub trailers: metadata::Decoded,
}
