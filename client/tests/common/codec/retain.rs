use o3::buffer::{hold, resident};
use sark_client::protocol;

use crate::common::codec::decoder;

pub struct Retain<'d, T> {
    pub result: Result<T, decoder::Error>,
    pub budget: resident::Budget<'d>,
}

impl<T> Retain<'_, T> {
    pub fn try_verify(self) -> Result<(), Self> {
        if let Err(decoder::Error::Protocol(protocol::Error::Retention(hold::Error::Resident(
            resident::AllocError::Budget(error),
        )))) = &self.result
        {
            let (need, spare) = error.into_parts();
            if spare == self.budget.available() && need.get() > spare {
                return Ok(());
            }
        }
        Err(self)
    }
}
