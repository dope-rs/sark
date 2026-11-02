use o3::buffer::storage::Shared;
use sark_h2::{
    ServerRole,
    conn::{self, DataOutcome, config, machine::Connection},
    stream::Id,
};

#[derive(Debug)]
pub enum Error {
    Config(config::Error),
    Connection(conn::Error),
    Data(DataOutcome),
    Payload,
}

pub trait Frames {
    fn data(self, count: usize) -> Vec<u8>;

    /// A full plaintext owner containing DATA and a non-flow-controlled extension.
    fn record(self, ping: Option<[u8; 8]>) -> [u8; 16_384];
}

impl Frames for Id {
    fn data(self, count: usize) -> Vec<u8> {
        let mut wire = Vec::new();
        for _ in 0..count {
            wire.extend_from_slice(&[0, 0, 5, 0, 0]);
            wire.extend_from_slice(&self.into_u32().to_be_bytes());
            wire.extend_from_slice(b"abcde");
        }
        wire
    }

    fn record(self, ping: Option<[u8; 8]>) -> [u8; 16_384] {
        let mut record = [0u8; 16_384];
        record[..5].copy_from_slice(&[0, 2, 0, 0, 0]);
        record[5..9].copy_from_slice(&self.into_u32().to_be_bytes());
        record[9..521].fill(b'x');
        record[521..530].copy_from_slice(&[0, 61, 238, 255, 0, 0, 0, 0, 0]);
        if let Some(opaque) = ping {
            record[523] = 221;
            record[16_367..16_376].copy_from_slice(&[0, 0, 8, 6, 0, 0, 0, 0, 0]);
            record[16_376..].copy_from_slice(&opaque);
        }
        record
    }
}

pub trait Ingress {
    fn try_owned(&mut self, bytes: &[u8]) -> Result<(), conn::Error>;
    fn try_chunks(&mut self, frame: &[u8; 14]) -> Result<(), conn::Error>;
}

impl Ingress for Connection<ServerRole> {
    fn try_owned(&mut self, bytes: &[u8]) -> Result<(), conn::Error> {
        self.ingress().ingest_owned(Shared::try_copy(bytes)?)
    }

    fn try_chunks(&mut self, frame: &[u8; 14]) -> Result<(), conn::Error> {
        for chunk in fragments(frame) {
            self.try_owned(chunk)?;
        }
        Ok(())
    }
}

pub(crate) fn fragments(frame: &[u8; 14]) -> [&[u8]; 5] {
    [
        &frame[..10],
        &frame[10..11],
        &frame[11..12],
        &frame[12..13],
        &frame[13..],
    ]
}

pub const GOAWAY: [u8; 4096] = {
    let mut record = [0u8; 4096];
    record[2] = 12;
    record[3] = 7;
    record[17] = b'b';
    record[18] = b'y';
    record[19] = b'e';
    record[20] = b'!';
    record[22] = 15;
    record[23] = 226;
    record[24] = 255;
    record
};
