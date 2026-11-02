use core::{mem, num};

use sark_core::body::admit::Retention;

use crate::wire::body::{ingress, shape::Shape};

#[derive(Clone, Copy)]
pub(crate) struct Complete {
    pub consumed: usize,
    pub body_len: usize,
}

#[derive(Debug)]
pub enum Error {
    Bad,
    Ingress(ingress::Error),
}

impl From<ingress::Error> for Error {
    fn from(error: ingress::Error) -> Self {
        Self::Ingress(error)
    }
}

/// Owner-preserving state for a fixed-length body that crossed a transport
/// boundary. Only one staging page is retained by the protocol; complete pages
/// move immediately into the routed continuation.
#[derive(Default)]
pub(crate) enum Fixed {
    #[default]
    Vacant,
    Active {
        remaining: num::NonZero<usize>,
        body_len: num::NonZero<usize>,
    },
}

impl Fixed {
    pub(crate) fn begin(
        &mut self,
        input: crate::Input<'_>,
        head: usize,
        total: usize,
        keep: Retention,
        store: &mut ingress::Ingress<'_>,
    ) -> Result<usize, Error> {
        if !matches!(self, Self::Vacant) {
            return Err(Error::Bad);
        }
        let source = input.as_slice();
        let bytes = source.len();
        if bytes < head || bytes >= total {
            return Err(Error::Bad);
        }
        let body_len = num::NonZero::<usize>::MIN.saturating_add(total - head - 1);
        let left = num::NonZero::<usize>::MIN.saturating_add(total - bytes - 1);
        store.start(keep, Shape::Flat);
        *self = Self::Active {
            remaining: body_len,
            body_len,
        };
        store.push_flat(&source[head..], body_len)?;
        *self = Self::Active {
            remaining: left,
            body_len,
        };
        Ok(bytes)
    }

    pub(crate) fn resume(
        &mut self,
        wire: &[u8],
        store: &mut ingress::Ingress<'_>,
    ) -> Result<Option<Complete>, Error> {
        let Self::Active {
            remaining: left,
            body_len,
        } = *self
        else {
            return Err(Error::Bad);
        };
        let used = wire.len().min(left.get());
        store.push_flat(&wire[..used], body_len)?;
        if let Some(left) = num::NonZero::new(left.get() - used) {
            *self = Self::Active {
                remaining: left,
                body_len,
            };
            return Ok(None);
        }
        store.finish()?;
        *self = Self::Vacant;
        Ok(Some(Complete {
            consumed: used,
            body_len: body_len.get(),
        }))
    }
}

const _: () = {
    assert!(mem::size_of::<Fixed>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Fixed>() == mem::align_of::<usize>());
    assert!(!mem::needs_drop::<Fixed>());
};
