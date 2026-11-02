use sark_grpc::{
    message::{self, input},
    o3::buffer::{segments, storage},
};

struct Forged;

impl input::Source<storage::Shared> for Forged {
    fn len(&self) -> usize {
        1
    }

    fn try_copy(&mut self, _: &mut [u8]) -> Result<(), input::Error> {
        Ok(())
    }

    fn try_move(&mut self, _: usize, _: &mut message::Payload) -> Result<(), segments::Error> {
        Ok(())
    }
}
