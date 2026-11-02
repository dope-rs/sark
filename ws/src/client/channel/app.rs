use dope::{
    core::driver::route::Identity,
    manifold::{
        client::session::{self, ingress},
        service::bindings,
    },
};

use crate::client::channel;

pub use crate::client::Logic as App;

impl<H, M, ID: Identity> App<H, M, ID> {
    pub fn bind<'life, 'e: 'life, 'd: 'e>(
        self,
        root: &'life channel::Port<'e, 'd, ID>,
    ) -> session::Setup<
        'd,
        bindings::Bound<'life, 'd, channel::Port<'e, 'd, ID>, Self, ID, { u32::MAX as usize }>,
        ingress::Local,
    > {
        let session = bindings::Bound::bind(root, self);
        session::Setup::new(session, root.ingress)
    }
}
