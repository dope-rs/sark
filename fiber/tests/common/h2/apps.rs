use core::mem;

use dope::core::driver::schedule::clock::timer::Timer;
use o3::buffer::{resident::bound, segments, storage};
use sark_core::{decode, duplex, manifold::task::app::Bind};
use sark_http::{
    field::{self, Value},
    method::Method,
    payload::{
        fixed,
        response::{
            dispatch::{Stream, chunk::Chunk},
            status,
        },
    },
    request,
    request::body,
};

use crate::common::h2::{facts::*, *};
use crate::common::route::body::Chunks;

#[sark_json::json(ordered)]
pub struct Item {
    pub id: i64,
    pub value: storage::Shared,
    #[field(seq)]
    pub tags: Vec<storage::Shared>,
}

#[sark_json::json(ordered)]
pub struct Nested {
    #[field(seq, nested)]
    pub items: Vec<Item>,
    #[field(nested)]
    pub featured: Item,
}

#[sark_fiber::request(ordered)]
#[json_body(Nested)]
pub struct NestedReq {}

#[sark_fiber::request]
pub struct SegmentReq {
    #[raw_body]
    pub payload: request::Segments,
}

#[sark_json::json(ordered)]
pub struct LargeJson {
    pub value: u64,
}

#[sark_fiber::request(ordered)]
#[json_body(LargeJson)]
#[repr(align(64))]
pub struct LargeRequest {}

#[sark_fiber::request]
pub struct FloodReq {
    #[raw_body]
    pub payload: request::Segments,
}

#[sark_fiber::request]
pub struct HeaderReq {
    #[authority]
    pub authority: storage::Shared,
    #[header("x-name", default = "")]
    pub name: storage::Shared,
    #[query("q", default = "missing-query")]
    pub req: storage::Shared,
    /// Opposite to route position: the ABI is canonicalized by name.
    #[path]
    pub team_id: storage::Shared,
    #[path]
    pub user_id: storage::Shared,
}

#[sark_fiber::response(raw)]
pub struct Reply<'d> {
    pub status: status::Code,
    pub body: request::Bytes<'d>,
}

#[sark_fiber::response(raw)]
pub struct OwnedReply {
    pub status: status::Code,
    pub body: storage::Shared,
}

#[sark_fiber::response(raw)]
pub struct RefReply<'req, 'd> {
    pub status: status::Code,
    pub body: body::Ref<'req, 'd>,
}

#[sark_fiber::response(raw)]
pub struct StaticReply<'d> {
    pub status: status::Code,
    pub body: fixed::Body<'d>,
}

pub struct RejectHead;

#[sark_fiber::response(raw)]
pub struct GuardReply<'d> {
    pub status: status::Code,
    #[header("x-guard-padding")]
    pub padding: Value<&'d [u8]>,
    pub body: fixed::Body<'d>,
}

impl Guard<State> for RejectHead {
    type Egress<'d> = bound::Unbounded;

    type Break<'req, 'd>
        = Result<OwnedReply, GuardReply<'d>>
    where
        'd: 'req;

    fn check<'req, 'd>(head: Head<'req>, state: &'req State) -> ControlFlow<Self::Break<'req, 'd>>
    where
        'd: 'req,
    {
        if let Some(guard) = &state.guard {
            guard.checks.set(guard.checks.get().saturating_add(1));
        } else {
            state.invalid.set(true);
        }
        if head.method() != &Method::POST || head.path() != Some(b"/guarded/body".as_slice()) {
            state.invalid.set(true);
        }
        if let Some(body) = state.body.take() {
            return ControlFlow::Break(Ok(OwnedReply {
                status: status::Code::UNAUTHORIZED,
                body,
            }));
        }
        ControlFlow::Break(Err(GuardReply {
            status: status::Code::UNAUTHORIZED,
            padding: if state.guard.as_ref().is_some_and(|guard| guard.padded) {
                field::literal!(Value::try_bytes(&[b'x'; 8192]))
            } else {
                field::literal!(Value::try_bytes(b""))
            },
            body: fixed::Body::new(b"guarded"),
        }))
    }
}

#[sark_fiber::handler(pub)]
pub(crate) async fn nested<'d>(request: NestedReq, state: &State, timer: Timer<'_>) -> Reply<'d> {
    if !matches!(request.body.items.as_slice(), [first, second] if first.id == i64::MIN && second.tags.len() == 2)
        || request.body.featured.id != i64::MAX
    {
        state.invalid.set(true);
    }
    let pointer = request.body.featured.value.as_slice().as_ptr();
    state.delay(timer, Duration::from_millis(10)).await;
    if request.body.featured.value.as_slice().as_ptr() != pointer {
        state.invalid.set(true);
    }
    Reply {
        status: status::Code::OK,
        body: request.body.featured.value.into_inner(),
    }
}

#[sark_fiber::handler(pub)]
pub(crate) fn segmented(request: SegmentReq, state: &State) -> RefReply<'req, 'd> {
    if request.payload.count() != segments::Count::TWO
        || request.payload.segment(0) != Some(b"first-".as_slice())
        || request.payload.segment(1) != Some(b"second".as_slice())
        || request.payload.as_slice().is_some()
    {
        state.invalid.set(true);
    }
    RefReply {
        status: status::Code::OK,
        body: request.payload,
    }
}

#[sark_fiber::handler(pub)]
pub(crate) async fn large(request: LargeRequest, state: &State) -> StaticReply<'d> {
    if request.body.value != 1 {
        state.invalid.set(true);
    }
    StaticReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"large"),
    }
}

#[sark_fiber::handler(pub)]
pub(crate) async fn quick() -> StaticReply<'d> {
    StaticReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"quick"),
    }
}

#[sark_fiber::handler(pub)]
pub(crate) async fn stream_hold(state: &State) -> Stream<'static, Chunks<Chunk<'d>>> {
    dope_test::fibers::Yield::new().await;
    Stream::new(
        Chunks::new(
            [Chunk::from(b"held")],
            Some(std::rc::Rc::clone(&state.stream)),
        )
        .park(true),
    )
}

#[sark_fiber::handler(pub)]
pub(crate) fn guarded(state: &State) -> StaticReply<'d> {
    if let Some(guard) = &state.guard {
        guard.handlers.set(guard.handlers.get().saturating_add(1));
    } else {
        state.invalid.set(true);
    }
    StaticReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"unreachable"),
    }
}

pub const PHYSICAL_BODY: [u8; 256] = [b'p'; 256];

#[sark_fiber::handler(pub)]
pub(crate) async fn physical() -> StaticReply<'d> {
    StaticReply {
        status: status::Code::OK,
        body: fixed::Body::new(&PHYSICAL_BODY),
    }
}

#[sark_fiber::handler(pub)]
pub(crate) fn unbounded(state: &State) -> Result<OwnedReply, StaticReply<'d>> {
    let Some(body) = state.body.take() else {
        state.invalid.set(true);
        return Err(StaticReply {
            status: status::Code::INTERNAL_SERVER_ERROR,
            body: fixed::Body::new(b"missing body"),
        });
    };
    Ok(OwnedReply {
        status: status::Code::OK,
        body,
    })
}

#[sark_fiber::handler(pub)]
pub(crate) fn ingress_flood(request: FloodReq, state: &State) -> StaticReply<'d> {
    if request.payload.as_slice() != Some(b"flood".as_slice()) {
        state.invalid.set(true);
    }
    if let Some(fairness) = &state.ingress {
        fairness
            .flood_started
            .set(fairness.flood_started.get().saturating_add(1));
    } else {
        state.invalid.set(true);
    }
    StaticReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"flood"),
    }
}

#[sark_fiber::handler(pub)]
pub(crate) fn ingress_probe(state: &State) -> StaticReply<'d> {
    if let Some(fairness) = &state.ingress {
        fairness.probe_seen_at.set(fairness.flood_started.get());
    } else {
        state.invalid.set(true);
    }
    StaticReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"probe"),
    }
}

#[sark_fiber::handler(pub)]
pub(crate) async fn blocked(state: &State, timer: Timer<'_>) -> StaticReply<'d> {
    state.delay(timer, Duration::from_secs(5)).await;
    StaticReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"blocked"),
    }
}

#[sark_fiber::handler(pub)]
pub(crate) async fn repark(state: &State, timer: Timer<'_>) -> StaticReply<'d> {
    state.delay(timer, Duration::from_secs(1)).await;
    state.delay(timer, Duration::from_secs(5)).await;
    StaticReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"parked"),
    }
}

pub(crate) const RETAINED_NAME: &[u8] = &[b'h'; 1024];

#[sark_fiber::handler(pub)]
pub(crate) async fn retained_header<'d>(
    request: HeaderReq,
    state: &State,
    timer: Timer<'_>,
) -> Reply<'d> {
    let pointer = request.name.as_slice().as_ptr();
    let id = request.name.id();
    if request.authority.id() != id
        || request.req.id() != id
        || request.team_id.id() != id
        || request.user_id.id() != id
    {
        state.invalid.set(true);
    }
    let auth_ptr = request.authority.as_slice().as_ptr();
    let team_ptr = request.team_id.as_slice().as_ptr();
    let user_ptr = request.user_id.as_slice().as_ptr();
    state.delay(timer, Duration::from_millis(25)).await;
    if request.name.as_slice().as_ptr() != pointer
        || request.authority.as_slice().as_ptr() != auth_ptr
        || request.team_id.as_slice().as_ptr() != team_ptr
        || request.user_id.as_slice().as_ptr() != user_ptr
        || request.authority.as_slice() != b"localhost"
        || request.name.as_slice() != RETAINED_NAME
        || request.req.as_slice() != b"request-query"
        || request.team_id.as_slice() != b"alpha"
        || request.user_id.as_slice() != b"beta"
    {
        state.invalid.set(true);
    }
    Reply {
        status: status::Code::OK,
        body: request.name,
    }
}

pub struct DuplexEcho(pub(super) u8);

pub const DUPLEX_LAYOUT: (usize, usize) =
    (mem::size_of::<DuplexEcho>(), mem::align_of::<DuplexEcho>());

pub const STATIC_DUPLEX_INPUT: &[u8] = b"static-duplex";

pub const STATIC_DUPLEX_BODY: [u8; 256] = [b'd'; 256];

#[sark_fiber::request]
pub struct DuplexReq {
    #[header("x-duplex-header", default = "")]
    pub marker: storage::Shared,
}

#[sark_fiber::duplex(pub)]
pub(crate) fn duplex_echo(request: DuplexReq, state: &State) -> DuplexEcho {
    state.header.set(request.marker.try_into().ok());
    DuplexEcho(0)
}

sark_fiber::define_task_route! {
    pub H2TaskApp: State => {
        POST "/nested" => async(capacity = 8, body = json) nested,
        POST "/segmented" => segmented,
        POST "/large" => async(capacity = 8, body = json) large,
        POST "/quick" => async(capacity = 64, body = discarded) quick,
        POST "/physical" => async(capacity = 64, body = discarded) physical,
        POST "/unbounded" => unbounded,
        POST "/ingress-flood" => ingress_flood,
        POST "/ingress-probe" => ingress_probe,
        POST "/blocked" => async(capacity = 2, body = discarded) blocked,
        POST "/parked" => async(capacity = 8, body = discarded) repark,
        POST "/users/:user_id/teams/:team_id/retained-header" => request(path, header, query) async(capacity = 8, body = discarded) retained_header,
        POST "/tree0" => async(capacity = 1, body = discarded) quick,
        POST "/tree1" => async(capacity = 1, body = discarded) quick,
        POST "/tree2" => async(capacity = 1, body = discarded) quick,
        POST "/tree3" => async(capacity = 1, body = discarded) quick,
        POST "/tree4" => async(capacity = 1, body = discarded) quick,
        POST "/tree5" => async(capacity = 1, body = discarded) quick,
        POST "/tree6" => async(capacity = 1, body = discarded) quick,
        POST "/tree7" => async(capacity = 1, body = discarded) quick,
        scope "/guarded" guard (RejectHead,) => [
            POST "/body" => guarded,
        ],
        CONNECT "*" => request(header) duplex_echo,
        CONNECT("sark-echo") "/duplex" => request(header) duplex_echo,
        POST "/stream" => async stream(capacity = 2, body = discarded) stream_hold,
    }
}

pub trait Layout<'d>: Bind<'d>
where
    <Self::Runtime as decode::Decode>::Egress<'d>: 'd,
{
    fn layout(&self) -> (usize, usize);
}

impl<'d, F> Layout<'d> for F
where
    F: Bind<'d>,
    F::Runtime: duplex::Application<'d>,
    <F::Runtime as decode::Decode>::Egress<'d>: 'd,
{
    fn layout(&self) -> (usize, usize) {
        (
            mem::size_of::<<F::Runtime as duplex::Application<'d>>::Duplex>(),
            mem::align_of::<<F::Runtime as duplex::Application<'d>>::Duplex>(),
        )
    }
}

pub const LARGE_JSON_LEN: usize = 24 * 1024;

pub const LARGE_JSON: &[u8; LARGE_JSON_LEN] = &{
    let mut input = [b' '; LARGE_JSON_LEN];
    let prefix = b"{\"value\":1}";
    let mut index = 0;
    while index < prefix.len() {
        input[index] = prefix[index];
        index += 1;
    }
    input
};

pub const NESTED_JSON: &[u8] = br#"{"items":[{"id":-9223372036854775808,"value":"first","tags":["a"]},{"id":2,"value":"second","tags":["b","c"]}],"featured":{"id":9223372036854775807,"value":"retained","tags":[]}}"#;
