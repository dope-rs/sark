use std::{cell::Cell, marker::PhantomData};

use sark::http::payload::response::status::Code;

pub struct State<'a, 'b, F: ?Sized = dyn for<'r#v> PartialEq<&'v [u8]>> {
    expected: &'a [u8],
    exact_pointer: bool,
    invariant: Cell<&'b str>,
    marker: PhantomData<F>,
}

impl<'a, 'b> State<'a, 'b> {
    pub fn new(expected: &'a [u8], exact: bool) -> Self {
        Self {
            expected,
            exact_pointer: exact,
            invariant: Cell::new("capability"),
            marker: PhantomData,
        }
    }
}

#[sark::request]
pub struct BodyRequest<'r#d> {
    #[raw_body]
    pub payload: storage::Shared,
}

#[sark::request]
pub struct PathRequest {
    #[path]
    pub id: storage::Shared,
}

#[sark::response(raw)]
pub struct Reply<'r#req> {
    pub(crate) status: Code,
    pub(crate) body: &'req [u8],
}

#[sark::handler(pub)]
pub(crate) fn echo<'i, 'r#req: 'i, 'r#d, 'r#__sark_state_data0>(
    req: BodyRequest,
    state: &State<
        '__sark_state_data0,
        '_,
        dyn for<'r#__sark_state_data0_> PartialEq<&'__sark_state_data0_ [u8]>,
    >,
) -> Reply<'i> {
    let valid = req.payload == state.expected
        && (!state.exact_pointer || req.payload.as_ref().as_ptr() == state.expected.as_ptr())
        && state.invariant.get() == "capability";
    Reply {
        status: if valid {
            Code::OK
        } else {
            Code::INTERNAL_SERVER_ERROR
        },
        body: req.payload,
    }
}

#[sark::handler(pub)]
pub(crate) fn item(req: PathRequest, state: &State<'_, '_>) -> Reply<'req> {
    Reply {
        status: if state.invariant.get() == "capability" {
            Code::OK
        } else {
            Code::INTERNAL_SERVER_ERROR
        },
        body: req.id,
    }
}

sark::define_router! {
    pub Router<'r#__state_data0>:
        State<'__state_data0, '_, dyn for<'r#__state_data0_> PartialEq<&'__state_data0_ [u8]>> => {
        GET "/" => echo,
        POST "/echo" => echo,
        GET "/items/:id" => request(path) item,
        GET "/items/new" => echo,
        GET "/items/next" => echo,
    }
}
