use sark_fiber::{driver::route::KeyTag, sark_core::duplex, sark_http::field::metadata::block};
use sark_h1::server::{deadline::policies::Policy, listener::bind::Bind};

pub trait Layout<'w, 'd: 'w> {
    fn dimensions<D: Policy<'d, KeyTag<0>>>(&self, deadline: D) -> (usize, usize, block::Connect);
}

impl<'w, 'd: 'w, F: Bind<'w, 'd, KeyTag<0>>> Layout<'w, 'd> for F {
    fn dimensions<D: Policy<'d, KeyTag<0>>>(&self, _: D) -> (usize, usize, block::Connect) {
        let size = size_of::<<F::Application<'d, D> as duplex::Application<'d>>::Duplex>();
        let align = align_of::<<F::Application<'d, D> as duplex::Application<'d>>::Duplex>();
        let connect = <F::Application<'d, D> as duplex::Application<'d>>::CONNECT;
        (size, align, connect)
    }
}
