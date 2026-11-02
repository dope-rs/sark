use std::ops;

use crate::server;

pub trait Sink {
    /// Called once after output reservation, before committing the opening response.
    /// The default accepts requests without Origin and rejects other requests with
    /// `Error::Forbidden` and HTTP 403. Override this method to authorize browser origins.
    /// An error sends HTTP 403 and is reported unchanged through [`Self::failed`].
    fn opening(&self, request: server::Opening<'_>) -> Result<(), server::Error> {
        if request.origin.is_some() {
            Err(server::Error::Forbidden)
        } else {
            Ok(())
        }
    }

    /// Maximum encoded application bytes for this complete message.
    fn capacity(&self, payload: usize) -> Result<usize, server::Error>;

    fn message(
        &self,
        msg: server::Message<'_>,
        out: &mut server::Response<'_>,
    ) -> Result<ops::ControlFlow<()>, server::Error>;

    fn failed(&self, error: server::Error);
}
