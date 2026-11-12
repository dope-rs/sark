use o3::buffer::write::Slice;
pub mod branches;
pub mod chunked;
pub mod fields;
mod layout;
pub mod terminal;

use core::marker::PhantomData;

pub use layout::Layout;
use sark_fiber::{
    driver::route::{Identity, KeyTag},
    sark_http::payload::{fixed, response::status::Code},
};

#[sark_fiber::request]
pub struct Request {
    #[path]
    req: usize,
}

#[sark_fiber::response(raw)]
pub struct Response<'d> {
    status: Code,
    body: fixed::Body<'d>,
}

#[sark_fiber::handler(pub)]
fn show<'d>(request: Request) -> Response<'d> {
    Response {
        status: if request.req == 0 {
            Code::NOT_FOUND
        } else {
            Code::OK
        },
        body: fixed::Body::new(b"ok"),
    }
}

#[sark_fiber::handler(pub)]
async fn show_async<'d>(request: Request) -> Response<'d> {
    Response {
        status: if request.req == 0 {
            Code::NOT_FOUND
        } else {
            Code::OK
        },
        body: fixed::Body::new(b"async"),
    }
}

#[sark_fiber::request]
pub struct Connect {}

pub struct DuplexState<C: Identity, const N: usize>(PhantomData<C>);

pub const DUPLEX_STATE: DuplexState<KeyTag<0>, 1> = DuplexState(PhantomData);

#[sark_fiber::duplex(pub)]
fn first_duplex<C: Identity, const N: usize>(_: Connect, _: &DuplexState<C, N>) -> Session<C> {
    Session(PhantomData)
}

#[sark_fiber::duplex(pub)]
fn second_duplex<C, const N: usize>(_: Connect, _: &DuplexState<C, N>) -> Session<KeyTag<1>>
where
    C: Identity,
{
    Session(PhantomData)
}

sark_fiber::define_router! {
    pub PortableRouter => {
        GET "/items/:req" => request(path) show,
    }
}

sark_fiber::define_route! {
    pub FiberApplication: sark_fiber::EmptyState => {
        GET "/items/:req" => request(path) show,
        GET "/async/:req" => request(path) async(capacity = 1, body = discarded) show_async,
    }
}

sark_fiber::define_route! {
    pub SingleDuplexApplication<C: Identity, const N: usize>: DuplexState<C, N> => {
        CONNECT "*" => first_duplex,
        CONNECT("sark-echo") "/duplex" => first_duplex,
    }
}

sark_fiber::define_route! {
    pub MultiDuplexApplication<C, const N: usize>: DuplexState<C, N>
    where C: Identity
    => {
        CONNECT "*" => first_duplex,
        CONNECT("sark-echo") "/duplex" => second_duplex,
    }
}

use sark_fiber::{sark_core::duplex, sark_http::payload::owned};

pub struct Session<ID: Identity>(PhantomData<fn() -> ID>);

impl<'d, ID: Identity> Session<ID> {
    const END: duplex::Action<'d> = duplex::Action::end();
    const IDLE: duplex::Action<'d> = duplex::Action::idle();
}

impl<'d, ID: Identity> duplex::Session<'d> for Session<ID> {
    fn receive(
        &mut self,
        _: owned::Segments<'d>,
        _: Slice<'_, &mut usize>,
        _: &mut Vec<u8>,
    ) -> duplex::Action<'d> {
        duplex::Action::close()
    }

    fn finish(&mut self, _: Slice<'_, &mut usize>, _: &mut Vec<u8>) -> duplex::Action<'d> {
        Self::END
    }

    fn activate(&mut self, _: Slice<'_, &mut usize>, _: &mut Vec<u8>) -> duplex::Action<'d> {
        Self::IDLE
    }
}
