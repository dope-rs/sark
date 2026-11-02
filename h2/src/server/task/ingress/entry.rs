use o3::depot::queue::indexed::keyed;
use sark_core::{
    body::{self, stream::Stream},
    decode::{request, state},
};

use crate::{
    server::{state::exchange, task::ingress::states::Buffered},
    stream,
};

pub(in crate::server::task) enum Admission {}

pub(in crate::server::task) enum Build {}

#[repr(u8)]
pub(in crate::server::task) enum Entry<'d, P, F, B>
where
    P: request::Prepared,
    F: state::Family<P>,
    B: body::Build<'d>,
{
    Routed {
        state: exchange::Routed<'d, P, F>,
        link: keyed::Link<stream::Key, Admission>,
    },
    Buffered(Buffered<'d, P>),
    Streaming {
        body: Stream<'d, B>,
        link: keyed::Link<stream::Key, Build>,
    },
}

const _: () = assert!(
    std::mem::size_of::<keyed::Link<stream::Key, Admission>>()
        == std::mem::size_of::<keyed::Link<stream::Key>>(),
);
const _: () = assert!(
    std::mem::size_of::<keyed::Link<stream::Key, Build>>()
        == std::mem::size_of::<keyed::Link<stream::Key>>(),
);
const _: () = assert!(
    std::mem::size_of::<keyed::Fifo<stream::Key, Admission>>()
        == std::mem::size_of::<keyed::Fifo<stream::Key>>(),
);
const _: () = assert!(
    std::mem::size_of::<keyed::Fifo<stream::Key, Build>>()
        == std::mem::size_of::<keyed::Fifo<stream::Key>>(),
);
