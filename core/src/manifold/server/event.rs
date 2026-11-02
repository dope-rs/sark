use sark_http::field::metadata::section::state::RetainedHead;

use crate::manifold::{client::event::Reset, server, stream};

/// One request-side event produced by an HTTP server carrier.
pub enum Event<C: server::Connection + ?Sized> {
    /// Carrier control progress with no application-visible request data.
    Control,
    /// The protocol handshake completed for this connection.
    Established,
    Head {
        message: C::Message,
        head: RetainedHead<C::Fields, C::Selection, C::Control>,
        section: stream::Section,
    },
    Data {
        message: C::Message,
        data: C::Data,
    },
    Reset {
        stream: C::Key,
        reason: Reset,
        retired: Option<C::Retired>,
    },
    /// A transport owner retired after semantic stream completion.
    Retired(C::Retired),
}
