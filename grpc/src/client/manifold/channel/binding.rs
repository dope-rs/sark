use std::convert;

use dope::{
    core::driver::route,
    manifold::{
        app::env,
        client::{session, session::codec::staging},
        network::client::topology::{factory, plan},
        service::bindings,
    },
    net::wire::{Identity, domain},
};
use session::ingress;

use crate::client::manifold::{Driver, channel, settings};

const MAX_CONNECTIONS: usize = u32::MAX as usize;

#[doc(hidden)]
pub struct Binding;

impl<'life, 'p: 'life, 'd: 'p, ID: route::Identity + 'd, E>
    factory::Application<'life, 'p, 'd, settings::Settings<ID>, ID, MAX_CONNECTIONS, E> for Binding
where
    E: env::Spec<Wire = Identity>,
{
    type Error = convert::Infallible;
    type Input = plan::Local<staging::Direct>;
    type Session = bindings::Bound<'life, 'd, channel::Port<'p, 'd, ID>, Driver, ID, MAX_CONNECTIONS>;
    type Source = ingress::Local;
    type Wire = domain::Local<Identity>;

    fn build(
        self,
        root: &'life channel::Port<'p, 'd, ID>,
    ) -> Result<(session::Setup<'d, Self::Session, Self::Source>, Self::Wire), Self::Error> {
        let session = bindings::Bound::bind(root, Driver::new());
        Ok((
            session::Setup::new(session, root.ingress),
            domain::Local::new(Identity),
        ))
    }
}
