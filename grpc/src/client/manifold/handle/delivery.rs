use std::ops;

use dope::{core::driver::schedule::ready, runtime::client};
use o3::buffer::resident;

use crate::{client::receive, message};

struct Fence<'app, 'd: 'app, P: ?Sized + 'app>(client::Bound<'app, 'd, P, ready::Target<'d>>);

impl<P: ?Sized> Drop for Fence<'_, '_, P> {
    fn drop(&mut self) {
        self.0.wake();
    }
}

/// Retains the request's provider and app lifetime until release.
#[must_use = "dropping a delivery releases its response owners and resumes ingress"]
pub struct Delivery<'app, 'd: 'app, P: ?Sized + 'app, T> {
    value: T,
    fence: Fence<'app, 'd, P>,
}

impl<'app, 'd: 'app, P: ?Sized + 'app, T> Delivery<'app, 'd, P, T> {
    pub(in crate::client::manifold) fn new(
        value: T,
        ready: ready::Target<'d>,
        bound: client::Bound<'app, 'd, P>,
    ) -> Self {
        Self {
            value,
            fence: Fence(bound.bind(ready)),
        }
    }

    /// Releases the retained response owners and resumes ingress now.
    pub fn release(self) {
        drop(self);
    }
}

impl<P: ?Sized, T> ops::Deref for Delivery<'_, '_, P, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'app, 'd: 'app, P: ?Sized + 'app>
    Delivery<'app, 'd, P, receive::UnaryResult<resident::Shared<'d>>>
{
    pub fn into_payload(
        self,
    ) -> Result<Delivery<'app, 'd, P, message::Payload<resident::Shared<'d>>>, crate::Status> {
        let Self { value, fence } = self;
        value.try_map(|message| {
            Ok(Delivery {
                value: message.payload,
                fence,
            })
        })
    }
}

impl<'app, 'd: 'app, P: ?Sized + 'app> receive::Unary
    for Delivery<'app, 'd, P, receive::UnaryResult<resident::Shared<'d>>>
{
    type Storage = resident::Shared<'d>;
    type Decoded<D: crate::Codec> = Delivery<'app, 'd, P, D::Owned<Self::Storage>>;

    fn into_decoded<D: crate::Codec>(
        self,
        codec: &mut D,
    ) -> Result<Self::Decoded<D>, crate::Status> {
        let Self { value, fence } = self;
        value.try_map(|message| {
            let value = codec.decode_frame(message)?;
            Ok(Delivery { value, fence })
        })
    }
}

impl<'app, 'd: 'app, P: ?Sized + 'app> receive::Event
    for Delivery<'app, 'd, P, receive::Response<resident::Shared<'d>>>
{
    type Storage = resident::Shared<'d>;
    type Decoded<D: crate::Codec> =
        Delivery<'app, 'd, P, receive::Decoded<D::Owned<Self::Storage>>>;

    fn into_decoded<D: crate::Codec>(
        self,
        codec: &mut D,
    ) -> Result<Self::Decoded<D>, crate::Status> {
        let Self { value, fence } = self;
        let value = <_ as receive::Event>::into_decoded(value, codec)?;
        Ok(Delivery { value, fence })
    }
}

const _: () = {
    use core::mem;

    assert!(
        mem::size_of::<Delivery<'static, 'static, str, ()>>()
            == mem::size_of::<ready::Target<'static>>()
    );
    assert!(
        mem::size_of::<Delivery<'static, 'static, str, message::Frame<resident::Shared<'static>>>>(
        ) == mem::size_of::<(
            message::Frame<resident::Shared<'static>>,
            ready::Target<'static>
        )>()
    );
    assert!(
        mem::align_of::<Delivery<'static, 'static, str, message::Frame<resident::Shared<'static>>>>(
        ) == mem::align_of::<(
            message::Frame<resident::Shared<'static>>,
            ready::Target<'static>
        )>()
    );
    assert!(
        mem::size_of::<
            Option<Delivery<'static, 'static, str, message::Frame<resident::Shared<'static>>>>,
        >() == mem::size_of::<
            Delivery<'static, 'static, str, message::Frame<resident::Shared<'static>>>,
        >()
    );
    assert!(
        mem::size_of::<
            Result<
                Delivery<'static, 'static, str, message::Frame<resident::Shared<'static>>>,
                crate::Status,
            >,
        >() == mem::size_of::<Result<message::Frame<resident::Shared<'static>>, crate::Status>>()
    );
};
