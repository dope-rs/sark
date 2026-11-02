use std::{cell::Cell, ops::ControlFlow};

use dope_test::harness::observe::VecCellExt as _;
use sark::{Guard, Head};
use sark_http::{
    o3::buffer::resident::bound,
    payload::{fixed, response::status},
};

#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    Access(Option<Vec<u8>>),
    LogPass,
    Secret,
    Nested,
    Open,
}

pub struct State<'a>(pub &'a Cell<Vec<Event>>);
pub struct AccessLog;

impl<'a> Guard<State<'a>> for AccessLog {
    type Egress<'d> = bound::Unbounded;
    type Break<'req, 'd>
        = Rejected<'d>
    where
        'd: 'req,
        State<'a>: 'req;

    fn check<'req, 'd>(
        head: Head<'req>,
        state: &'req State<'a>,
    ) -> ControlFlow<Self::Break<'req, 'd>>
    where
        'd: 'req,
        State<'a>: 'req,
    {
        state.0.push(Event::Access(head.path().map(<[u8]>::to_vec)));
        ControlFlow::Continue(())
    }
}

pub struct Deny;

impl<'a> Guard<State<'a>> for Deny {
    type Egress<'d> = bound::Unbounded;
    type Break<'req, 'd>
        = Rejected<'d>
    where
        'd: 'req,
        State<'a>: 'req;

    fn check<'req, 'd>(_: Head<'req>, _: &'req State<'a>) -> ControlFlow<Self::Break<'req, 'd>>
    where
        'd: 'req,
        State<'a>: 'req,
    {
        ControlFlow::Break(Rejected {
            status: status::Code::UNAUTHORIZED,
            body: fixed::Body::new(b""),
        })
    }
}

#[sark_fiber::response(raw)]
pub struct Resp {
    status: status::Code,
    body: Vec<u8>,
}

#[sark_fiber::response(raw)]
pub struct Rejected<'d> {
    status: status::Code,
    body: fixed::Body<'d>,
}

fn ok_body(body: &[u8], state: &State<'_>, event: Event) -> Resp {
    state.0.push(event);
    let mut buf = Vec::new();
    buf.extend_from_slice(body);
    Resp {
        status: status::Code::OK,
        body: buf,
    }
}

#[sark_fiber::handler(pub)]
fn log_pass<'a>(state: &State<'a>) -> Resp {
    ok_body(b"log-pass", state, Event::LogPass)
}

#[sark_fiber::handler(pub)]
fn secret<'a>(state: &State<'a>) -> Resp {
    ok_body(b"secret", state, Event::Secret)
}

#[sark_fiber::handler(pub)]
fn nested_x<'a>(state: &State<'a>) -> Resp {
    ok_body(b"nested-x", state, Event::Nested)
}

#[sark_fiber::handler(pub)]
fn open<'a>(state: &State<'a>) -> Resp {
    ok_body(b"open", state, Event::Open)
}

sark_fiber::define_route! {
    pub GuardDispatch: State<'a> => {
        scope "" guard (AccessLog,) => [
            GET "/log-pass" => log_pass,
        ],
        scope "/protected" guard (Deny,) => [
            GET "/secret" => secret,
        ],
        scope "/nested" guard (AccessLog,) => {
            scope "/inner" guard (Deny,) => [
                GET "/x" => nested_x,
            ],
        },
        GET "/open" => open,
    }
}
