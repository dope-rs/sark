use sark_h2::stream::Id;

use crate::common::drained::Chunk;

pub struct Segment<'a> {
    pub stream: Id,
    pub payload: &'a [u8],
    pub owner: Option<&'a Chunk>,
}
