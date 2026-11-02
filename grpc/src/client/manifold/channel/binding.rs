use std::convert;

use dope::{
    core::driver::{route, storage},
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

impl<ID: route::Identity, E> factory::Application<settings::Settings<ID>, ID, MAX_CONNECTIONS, E>
    for Binding
where
    E: env::Spec<Wire = Identity>,
{
    type Error = convert::Infallible;
    type Input = plan::Local<staging::Direct>;
    type Session<'life, 'p, 'd>
        = bindings::Bound<'life, 'd, channel::Port<'p, 'd, ID>, Driver, ID, MAX_CONNECTIONS>
    where
        Self: 'd,
        ID: 'd,
        settings::Settings<ID>: 'd,
        E::Wire: 'd,
        'd: 'p,
        'p: 'life,
        'd: 'life;
    type Source<'life, 'p, 'd>
        = ingress::Local
    where
        Self: 'd,
        ID: 'd,
        settings::Settings<ID>: 'd,
        E::Wire: 'd,
        'd: 'p,
        'p: 'life,
        'd: 'life;
    type Wire<'life, 'p, 'd>
        = domain::Local<'d, E::Wire, ID>
    where
        Self: 'd,
        ID: 'd,
        settings::Settings<ID>: 'd,
        E::Wire: 'd,
        'd: 'p,
        'p: 'life,
        'd: 'life;

    fn build<'life, 'p, 'd>(
        self,
        root: &'life <settings::Settings<ID> as storage::Plan>::Output<'p, 'd>,
    ) -> Result<
        (
            session::Setup<'d, Self::Session<'life, 'p, 'd>, Self::Source<'life, 'p, 'd>>,
            Self::Wire<'life, 'p, 'd>,
        ),
        Self::Error,
    >
    where
        Self: 'd,
        ID: 'd,
        settings::Settings<ID>: 'd,
        E::Wire: 'd,
        'd: 'p,
        'p: 'life,
        'd: 'life,
    {
        let session = bindings::Bound::bind(root, Driver::new());
        Ok((
            session::Setup::new(session, root.ingress),
            domain::Local::new(Identity),
        ))
    }
}
