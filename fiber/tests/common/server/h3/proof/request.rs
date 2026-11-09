use std::num::NonZero;

use o3::buffer::storage;
use sark_core::manifold::stream::Boundary;
use sark_h3::frame;

use crate::common::server::h3::proof::Dispatch;

#[derive(Clone, Copy)]
pub struct Spec<'a> {
    pub(crate) connection: usize,
    pub head: Head<'a>,
    pub data: Data<'a>,
    pub expect: Expected<'a>,
    pub dispatch: Dispatch,
    pub read_response: bool,
}

impl<'a> Spec<'a> {
    pub fn new(path: &'a [u8], body: &'a [u8]) -> Self {
        Self {
            connection: 0,
            head: Head {
                method: b"POST",
                protocol: None,
                path,
                content_type: None,
                wire: None,
            },
            data: Data {
                bytes: body,
                chunk_size: None,
                split: None,
                defer_tail: false,
                declared_body_len: None,
                boundary: Boundary::End,
            },
            expect: Expected {
                status: b"200",
                error: None,
                input_finished: true,
                connection_closed: false,
            },
            dispatch: Dispatch::Initial,
            read_response: true,
        }
    }

    pub fn duplex(body: &'a [u8]) -> Self {
        let mut request = Self::new(b"/duplex", body);
        request.head.method = b"CONNECT";
        request.head.protocol = Some(b"sark-echo");
        request
    }

    pub fn reserved() -> Self {
        let mut request = Self::new(b"/body", b"");
        request.head.wire = Some(const { &[frame::TYPE_HEADERS.get() as u8] });
        request.data.boundary = Boundary::More;
        request.expect.input_finished = false;
        request
    }

    pub fn classic(body: &'a [u8]) -> Self {
        let mut request = Self::new(b"*", body);
        request.head.method = b"CONNECT";
        request
    }
}

#[derive(Clone, Copy)]
pub struct Head<'a> {
    pub(crate) method: &'a [u8],
    pub(crate) protocol: Option<&'a [u8]>,
    pub(crate) path: &'a [u8],
    pub content_type: Option<&'a [u8]>,
    pub wire: Option<&'a [u8]>,
}

#[derive(Clone, Copy)]
pub struct Data<'a> {
    pub bytes: &'a [u8],
    pub chunk_size: Option<NonZero<usize>>,
    pub split: Option<usize>,
    pub defer_tail: bool,
    pub declared_body_len: Option<usize>,
    pub boundary: Boundary,
}

#[derive(Clone, Copy)]
pub struct Expected<'a> {
    pub status: &'a [u8],
    pub error: Option<frame::ErrorCode>,
    pub input_finished: bool,
    pub connection_closed: bool,
}

#[sark_fiber::request]
pub(crate) struct BodyReq {
    #[raw_body]
    pub(crate) payload: storage::Shared,
}

#[sark_fiber::request]
pub(crate) struct StreamReq {
    #[path]
    pub(crate) kind: storage::Shared,
    #[raw_body]
    pub(crate) payload: storage::Shared,
}

#[sark_fiber::request]
pub(crate) struct SegmentReq {
    #[raw_body]
    pub(crate) payload: owned::Segments,
}

#[sark_fiber::request]
pub(crate) struct EmptyReq {}

#[sark_fiber::request]
pub(crate) struct NamedPathReq {
    /// Intentionally opposite to the route pattern.
    #[path]
    pub(crate) team_id: storage::Shared,
    #[path]
    pub(crate) user_id: storage::Shared,
}

#[sark_json::json(ordered, plain)]
pub(crate) struct JsonBody {
    pub(crate) value: storage::Shared,
}

#[sark_fiber::request(ordered)]
#[json_body(JsonBody)]
pub(crate) struct JsonReq {}

#[sark_json::json(ordered)]
pub(crate) struct LargeJson {
    pub(crate) value: u64,
}

#[sark_fiber::request(ordered)]
#[json_body(LargeJson)]
pub(crate) struct LargeJsonReq {}

#[sark_json::json(ordered)]
pub(crate) struct JsonItem {
    pub(crate) id: u64,
    pub(crate) value: storage::Shared,
    #[field(seq)]
    pub(crate) tags: Vec<storage::Shared>,
}

#[sark_json::json(ordered)]
pub(crate) struct JsonTree {
    #[field(seq, nested)]
    pub(crate) items: Vec<JsonItem>,
    #[field(nested)]
    pub(crate) featured: JsonItem,
}

#[sark_fiber::request(ordered)]
#[json_body(JsonTree)]
pub(crate) struct TreeReq {}
