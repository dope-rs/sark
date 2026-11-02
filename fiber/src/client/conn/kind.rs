use std::convert::Infallible;

use dope::{
    core::driver::route::Identity,
    net::wire::{self, terminal},
};
use dope_fiber::reply::layout;
use sark_client::protocol::codec::Codec;

use crate::client::{
    channel::Port,
    config::report,
    dynamic::{channel, origin},
};

#[doc(hidden)]
pub trait Kind<'d, ID: Identity> {
    type Error;
    type Failure;
    type State;
    type Position;
    type Tag;

    fn codec(&self) -> &Codec;
}

impl<'f, 'e, 'b, 'c, 'w: 'f, 'd: 'f, ID: Identity, W: wire::Spec + 'w, F: 'w> Kind<'d, ID>
    for Port<'f, 'e, 'b, 'c, 'w, 'd, ID, W, F>
where
    F: report::Policy<terminal::Error<'w, W>>,
{
    type Error = terminal::Error<'w, W>;
    type Failure = F;
    type State = F::State<'f, 'd>;
    type Position = layout::Position<'b, 'd, ID>;
    type Tag = ();

    fn codec(&self) -> &Codec {
        &self.protocol.codec
    }
}

impl<'o, 'q, 'e, 'b, 'c, 'd, ID: Identity> Kind<'d, ID>
    for channel::Port<'o, 'q, 'e, 'b, 'c, 'd, ID>
{
    type Error = Infallible;
    type Failure = report::Never;
    type State = <report::Never as report::Policy<Infallible>>::State<'d, 'd>;
    type Position = layout::Position<'b, 'd, ID>;
    type Tag = origin::Key<'o, 'q, 'd, ID>;

    fn codec(&self) -> &Codec {
        &self.protocol.codec
    }
}
