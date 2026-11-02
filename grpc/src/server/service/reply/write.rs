use core::task::Poll;

use o3::buffer::storage;

use crate::server::service::reply::proof::Proof;

pub trait Write: Proof {
    fn push(&mut self, payload: storage::Shared) -> Result<(), crate::Status>;

    fn encode<C: crate::Codec>(
        &mut self,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<(), crate::Status>;

    fn try_encode<C: crate::Codec>(
        &mut self,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<Poll<()>, crate::Status>;

    fn clear(&mut self);

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn poll_ready(&self) -> Result<Poll<()>, crate::Status>;

    fn push_message(&mut self, payload: Vec<u8>) -> Result<(), crate::Status>;
}
