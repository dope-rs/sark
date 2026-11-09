use std::pin;

use o3::buffer::{hold, resident::bound};

use crate::{session, switch};

pub trait Routes<'d, Runtime, Transitions: switch::Engine = switch::Disabled>:
    session::Dispatch<'d, Runtime, Self::Pending, Transitions>
{
    type Pending;
    type TransportTag: Copy + Default + 'd;
    type Egress: bound::Bound + 'd;

    fn response_tag(
        self: pin::Pin<&mut Self>,
        runtime: &mut Runtime,
        charge: hold::Charge,
    ) -> Self::TransportTag;

    fn accept(self: pin::Pin<&mut Self>, runtime: &mut Runtime, tag: Self::TransportTag);

    fn cancel(self: pin::Pin<&mut Self>, runtime: &mut Runtime, tag: Self::TransportTag);
}
