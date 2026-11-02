use o3::buffer::{resident, segments};
use sark_http::{payload::owned, request::body};

/// A retained backing converted without copying or changing its owner lifetime.
pub trait Store<'d>: Sized {
    type Output: body::Source<'d>;

    fn finish(storage: Option<Self>) -> Self::Output;
}

impl<'d> Store<'d> for owned::Segments<'d> {
    type Output = Self;

    fn finish(storage: Option<Self>) -> Self {
        match storage {
            Some(storage) => storage,
            None => Self::new(),
        }
    }
}

impl<'d> Store<'d> for segments::Chain<resident::Shared<'d>> {
    type Output = owned::Segments<'d>;

    fn finish(storage: Option<Self>) -> Self::Output {
        match storage {
            Some(storage) => storage.into(),
            None => owned::Segments::new(),
        }
    }
}

impl<'d> Store<'d> for &[u8] {
    type Output = Self;

    fn finish(storage: Option<Self>) -> Self {
        match storage {
            Some(storage) => storage,
            None => &[],
        }
    }
}

impl<'d> Store<'d> for () {
    type Output = owned::Segments<'d>;

    fn finish(_: Option<Self>) -> Self::Output {
        owned::Segments::new()
    }
}
