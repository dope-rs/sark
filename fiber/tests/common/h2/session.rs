use o3::buffer::resident;
use sark_core::duplex;
use sark_http::payload::owned;

use crate::common::h2::apps::{DuplexEcho, STATIC_DUPLEX_BODY, STATIC_DUPLEX_INPUT};

impl<'d> duplex::Session<'d> for DuplexEcho {
    fn receive(&mut self, input: owned::Segments<'d>) -> duplex::Action<'d> {
        if input.contiguous() != Some(STATIC_DUPLEX_INPUT) {
            return duplex::Action::send_close(input);
        }
        duplex::Action::send_close(resident::Shared::from(STATIC_DUPLEX_BODY.as_slice()))
    }

    fn finish(&mut self) -> duplex::Action<'d> {
        duplex::Action::end()
    }

    fn activate(&mut self) -> duplex::Action<'d> {
        duplex::Action::idle()
    }
}
