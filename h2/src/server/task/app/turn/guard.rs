use sark_core::decode::request;

pub(super) trait Guard {}

impl<G> Guard for request::Guarded<G> {}

impl<U, G> Guard for request::Start<U, G> {}
