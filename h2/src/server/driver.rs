use o3::buffer::{Bytes, Retained};

use crate::conn::{Conn, ConnError};
use crate::role::ServerRole;

pub trait Transport {
    fn connection(&mut self) -> &mut Conn<ServerRole>;

    fn drain_events(&mut self) -> usize;
}

pub struct Driver<'a, T> {
    transport: &'a mut T,
}

impl<'a, T> Driver<'a, T>
where
    T: Transport,
{
    pub fn new(transport: &'a mut T) -> Self {
        Self { transport }
    }

    pub fn ingest(&mut self, bytes: &[u8]) -> Result<(), ConnError> {
        if self.transport.connection().goaway_sent()
            || self.transport.connection().goaway_received().is_some()
        {
            return Ok(());
        }

        let result = self.transport.connection().ingest(bytes);
        self.drain(result)
    }

    pub fn ingest_retained(&mut self, bytes: Bytes<Retained>) -> Result<(), ConnError> {
        if self.transport.connection().goaway_sent()
            || self.transport.connection().goaway_received().is_some()
        {
            return Ok(());
        }

        let result = self.transport.connection().ingest_retained(bytes);
        self.drain(result)
    }

    fn drain(&mut self, mut result: Result<(), ConnError>) -> Result<(), ConnError> {
        loop {
            let drained = self.transport.drain_events();
            match result {
                Ok(()) => return Ok(()),
                Err(ConnError::Overload) if drained != 0 => {
                    result = self.transport.connection().resume();
                }
                Err(error) => return Err(error),
            }
        }
    }
}
