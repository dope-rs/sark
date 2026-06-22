use core::marker::PhantomData;

use prost::Message;

use crate::status::{Code, Status};

pub trait Codec {
    type Encode;
    type Decode;

    fn encode(&mut self, item: &Self::Encode, out: &mut Vec<u8>) -> Result<(), Status>;
    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Decode, Status>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ProstCodec<Encode, Decode = Encode>(PhantomData<fn() -> (Encode, Decode)>);

impl<Encode, Decode> ProstCodec<Encode, Decode> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<Encode, Decode> Codec for ProstCodec<Encode, Decode>
where
    Encode: Message,
    Decode: Message + Default,
{
    type Encode = Encode;
    type Decode = Decode;

    fn encode(&mut self, item: &Encode, out: &mut Vec<u8>) -> Result<(), Status> {
        item.encode(out)
            .map_err(|e| Status::new(Code::Internal, e.to_string()))
    }

    fn decode(&mut self, bytes: &[u8]) -> Result<Decode, Status> {
        Decode::decode(bytes).map_err(|e| Status::new(Code::InvalidArgument, e.to_string()))
    }
}
