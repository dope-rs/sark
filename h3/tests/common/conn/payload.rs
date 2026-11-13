use dope_quic::wire::varint::VarInt;
use o3::{alloc, buffer::hold};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    Payload,
    conn::{
        event::{Event, Identity},
        output,
        output::{WritePayload, WritePrefix},
    },
    link::{Outcome, Transport},
};

pub trait Prefix {
    fn try_prefix(self, length: VarInt) -> Result<Vec<u8>, alloc::Error>;
}

impl Prefix for VarInt {
    fn try_prefix(self, length: VarInt) -> Result<Vec<u8>, alloc::Error> {
        let mut wire = Vec::new();
        self.encode(&mut wire)?;
        length.encode(&mut wire)?;
        Ok(wire)
    }
}

#[derive(Default)]
pub struct Capture {
    pub inline_writes: usize,
    pub owned_writes: usize,
    pub finishes: usize,
}

impl<'d> Transport<'d> for Capture {
    fn send_write(&mut self, write: &output::Write<'d>) -> Outcome {
        let boundary = write.boundary();
        match &write.prefix {
            WritePrefix::Inline(_) => self.inline_writes += 1,
            WritePrefix::Owned(_) => self.owned_writes += 1,
        }
        if let Some(payload) = write.payload.as_ref() {
            match payload {
                WritePayload::Owned(_) => self.owned_writes += 1,
                WritePayload::Shared(_) => self.owned_writes += 1,
                WritePayload::Retained(_) => self.owned_writes += 1,
                WritePayload::Segmented(_) => self.owned_writes += 1,
            }
        }
        if matches!(boundary, Boundary::End) {
            self.finishes += 1;
        }
        Outcome::Accepted
    }
}

pub trait Owned {
    fn into_owned(self) -> Option<Vec<u8>>;
}
impl Owned for Option<WritePayload<'_>> {
    fn into_owned(self) -> Option<Vec<u8>> {
        match self {
            Some(WritePayload::Owned(bytes)) => Some(bytes),
            _ => None,
        }
    }
}

pub trait Data<'d> {
    fn into_data(self) -> Option<Payload<'d>>;
}
impl<'d, S, B, I: Identity> Data<'d> for Option<Event<'d, S, B, I>> {
    fn into_data(self) -> Option<Payload<'d>> {
        match self {
            Some(Event::Data { data, .. }) => Some(data),
            _ => None,
        }
    }
}

pub trait Write {
    fn into_bytes(self) -> Vec<u8>;
    fn charge(&self) -> Option<hold::Charge>;
}
impl Write for output::Write<'_> {
    fn into_bytes(self) -> Vec<u8> {
        let mut bytes = match self.prefix {
            WritePrefix::Inline(prefix) => prefix.as_ref().to_vec(),
            WritePrefix::Owned(bytes) => bytes,
        };
        if let Some(payload) = self.payload {
            match payload {
                WritePayload::Owned(payload) => bytes.extend_from_slice(&payload),
                WritePayload::Shared(payload) => bytes.extend_from_slice(payload.as_ref()),
                WritePayload::Retained(payload) => bytes.extend_from_slice(payload.as_ref()),
                WritePayload::Segmented(payload) => {
                    for segment in payload.as_chain().segments() {
                        bytes.extend_from_slice(segment);
                    }
                }
            }
        }
        bytes
    }
    fn charge(&self) -> Option<hold::Charge> {
        let owned = |bytes: &Vec<u8>| hold::Charge {
            logical_bytes: bytes.len(),
            resident_bytes: bytes.capacity(),
            owners: usize::from(!bytes.is_empty() && bytes.capacity() != 0),
        };
        let prefix = match &self.prefix {
            WritePrefix::Inline(_) => hold::Charge::default(),
            WritePrefix::Owned(bytes) => owned(bytes),
        };
        let fields = match &self.payload {
            None => hold::Charge::default(),
            Some(WritePayload::Owned(bytes)) => owned(bytes),
            Some(_) => return None,
        };
        Some(hold::Charge {
            logical_bytes: prefix.logical_bytes + fields.logical_bytes,
            resident_bytes: prefix.resident_bytes + fields.resident_bytes,
            owners: prefix.owners + fields.owners,
        })
    }
}
