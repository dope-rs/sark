use std::cell::Cell;

use prost::Message;
use sark_grpc::{Status, encode};

pub const TRUNCATED: &[u8] = b"\x0a\x01x\x0a\x02y";

#[derive(Message, sark_gen::Message)]
#[prost(skip_debug)]
pub struct Packet {
    #[prost(bytes = "vec", tag = "1")]
    pub data: Vec<u8>,
}

#[derive(sark_gen::Message)]
pub struct Borrowed<'a> {
    #[prost(bytes = "vec", tag = "1")]
    pub data: &'a Vec<u8>,
}

pub struct Probe {
    pub plans: Cell<usize>,
    pub bytes: Vec<u8>,
}

impl encode::Message for Probe {
    type Plan<'a> = &'a Self;

    fn plan(&self) -> Self::Plan<'_> {
        self.plans.set(self.plans.get() + 1);
        self
    }
}

impl encode::Plan for &Probe {
    fn write<S: encode::Sink>(self, sink: S) -> Result<S::Output, Status> {
        sink.try_slices([self.bytes.as_slice()])
    }
}
