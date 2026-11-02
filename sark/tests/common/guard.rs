use std::{
    cell::Cell,
    iter,
    ops::{ControlFlow, Deref},
};

use sark::{
    Guard, Head,
    http::{
        o3::buffer::resident::bound,
        payload::{fixed, response::status::Code},
    },
};

pub struct State<C, const N: usize> {
    pub calls: C,
}

#[sark::request]
pub struct Request {
    #[path]
    pub value: Shared,
}

#[sark::response(raw)]
pub struct Reply<'d> {
    pub(crate) status: Code,
    pub(crate) body: fixed::Body<'d>,
}

#[sark::response(raw)]
pub struct Rejected<'req> {
    pub(crate) status: Code,
    pub(crate) body: &'req [u8],
}

pub struct EchoPath;

impl<C: Deref<Target = Cell<usize>>, const N: usize> Guard<State<C, N>> for EchoPath {
    type Egress<'d> = bound::Unbounded;
    type Break<'req, 'd>
        = Rejected<'req>
    where
        'd: 'req,
        State<C, N>: 'req;

    fn check<'req, 'd>(head: Head<'req>, _: &'req State<C, N>) -> ControlFlow<Self::Break<'req, 'd>>
    where
        'd: 'req,
        State<C, N>: 'req,
    {
        let Some(path) = head.path() else {
            return ControlFlow::Continue(());
        };
        ControlFlow::Break(Rejected {
            status: Code::UNAUTHORIZED,
            body: path,
        })
    }
}

#[sark::handler(pub, request = Request)]
pub(crate) fn unreachable<C, const N: usize>(state: &State<C, N>) -> Reply<'d>
where
    C: Deref<Target = Cell<usize>>,
{
    state.calls.set(state.calls.get() + N);
    Reply {
        status: Code::OK,
        body: fixed::Body::new(b"unreachable"),
    }
}

sark::define_router! {
    pub Router<r#__V, const r#STATE: usize>: State<r#__V, r#STATE>
    where r#__V: Deref<Target = Cell<usize>>
    => {
        scope "" guard (EchoPath,) => [
            GET "/guard/:value" => request(path) unreachable,
        ],
    }
}

impl Request<'_> {
    pub fn wire() -> Vec<u8> {
        let mut req = Vec::from(&b"GET /guard/"[..]);
        req.extend(iter::repeat_n(b'a', 512));
        req.extend_from_slice(b" HTTP/1.1\r\nHost: origin.example\r\n\r\n");
        req
    }
}
