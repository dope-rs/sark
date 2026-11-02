use dope::{core::driver::route::Identity, manifold::network::listener::conn, net::wire};
use wire::{open, terminal};

/// Final application consumer of failures from the original wire owner.
pub trait Policy<W: wire::Spec> {
    fn open_failed<'w>(&self, cause: open::Error<'w, W>)
    where
        W: 'w;

    fn wire_failed<'w, 'd: 'w, ID: Identity>(
        &self,
        id: conn::Id<'d, ID>,
        cause: terminal::Error<'w, W>,
    ) where
        W: 'w;
}

impl<S> Policy<wire::Identity> for S {
    fn open_failed<'w>(&self, cause: open::Error<'w, wire::Identity>)
    where
        wire::Identity: 'w,
    {
        cause.eliminate()
    }

    fn wire_failed<'w, 'd: 'w, ID: Identity>(
        &self,
        _: conn::Id<'d, ID>,
        cause: terminal::Error<'w, wire::Identity>,
    ) {
        cause.eliminate()
    }
}
