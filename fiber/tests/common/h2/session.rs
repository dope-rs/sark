use o3::buffer::resident;
use sark_core::duplex;
use sark_http::payload::owned;

use crate::common::h2::apps::{DuplexEcho, STATIC_DUPLEX_BODY, STATIC_DUPLEX_INPUT};

impl<'d> duplex::Session<'d> for DuplexEcho {
    fn receive(
        &mut self,
        input: owned::Segments<'d>,
        _: &mut [u8],
        _: &mut Vec<u8>,
    ) -> (usize, duplex::Action<'d>) {
        if !input.segments().flatten().eq(STATIC_DUPLEX_INPUT) {
            return (0, duplex::Action::send_close(input));
        }
        (
            0,
            duplex::Action::send_close(resident::Shared::from(STATIC_DUPLEX_BODY.as_slice())),
        )
    }

    fn finish(&mut self, _: &mut [u8], _: &mut Vec<u8>) -> (usize, duplex::Action<'d>) {
        let action = match self.0 {
            1 => duplex::Action::send(resident::Shared::from(b"late".as_slice())),
            3 => duplex::Action::idle(),
            _ => duplex::Action::end(),
        };
        (0, action)
    }

    fn activate(&mut self, _: &mut [u8], _: &mut Vec<u8>) -> (usize, duplex::Action<'d>) {
        let action = match self.0 {
            1 | 3 => duplex::Action::end(),
            _ => duplex::Action::idle(),
        };
        (0, action)
    }
}
