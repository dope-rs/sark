use o3::{alloc, buffer::write::consume};

use crate::hpack::encode;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Headers {
    Encoder(encode::Create),
    Blocks(alloc::Exhausted),
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Ingress {
    Input(alloc::Error),
    Events(alloc::Error),
    Decoder(alloc::Error),
    Pool(alloc::Exhausted),
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Egress {
    Outbound(Outbound),
    Headers(Headers),
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Outbound {
    Buffer(alloc::Exhausted),
    Payloads(alloc::Error),
    Initial(consume::Error),
}
