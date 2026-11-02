use std::{cell::Cell, ops::ControlFlow};

use o3::buffer::resident::bound;
use sark::{Guard, Head};
use sark_http::{
    field::{self, Value},
    payload::{
        fixed,
        response::{
            headers::{Entry, Name},
            statics,
            status::Code,
        },
    },
};

use crate::common::sync;

pub struct State {
    pub replies: Cell<usize>,
    pub value: Value<Vec<u8>>,
}

pub struct Deny;

impl Guard<State> for Deny {
    type Egress<'d> = bound::AtMost<0>;
    type Break<'req, 'd>
        = statics::Direct<'d, [Entry<'req>; 1]>
    where
        'd: 'req;

    fn check<'req, 'd>(_: Head<'req>, state: &'req State) -> ControlFlow<Self::Break<'req, 'd>>
    where
        'd: 'req,
    {
        state.replies.set(state.replies.get() + 1);
        ControlFlow::Break(statics::Direct::new(
            Code::UNAUTHORIZED,
            [Entry::new(
                field::literal!(Name::try_bytes(b"x-large")),
                state.value.borrow(),
            )],
            fixed::Body::new(b""),
        ))
    }
}

sark::define_router! {
    pub Router: State => {
        scope "" guard (Deny,) => [
            POST "/pending" => sync::static_only,
        ],
    }
}
