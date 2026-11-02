use dope_quic::wire::{stream::Id, varint};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        self, Connection,
        event::{Event, Role},
        io::Receive,
        options::{self, Config},
    },
    frame::{self, Settings},
};
use sark_http::field::metadata::block::Connect;

use crate::common::conn::{Events, build::Settings as _};

pub mod content;
pub mod pressure;

pub struct Setup {
    egress: options::Egress,
}

impl Setup {
    pub fn with_writes(writes: usize) -> Self {
        Self {
            egress: options::Egress {
                write_capacity: writes,
                ..options::Egress::default()
            },
        }
    }

    pub fn new(writes: usize, wire: usize, resident: usize) -> Self {
        Self {
            egress: options::Egress {
                write_capacity: writes,
                wire_bytes: wire,
                resident_bytes: resident,
            },
        }
    }

    pub fn try_build<'d>(self) -> Result<Connection<'d>, options::Error> {
        Connection::with_config(
            Role::Server,
            Config {
                egress: self.egress,
                ..Config::default()
            },
        )
    }
}

#[derive(Debug)]
pub enum Dynamic {
    Config(options::Error),
    Connection(conn::Error),
    Stream(varint::Error),
    Wire(frame::EncodeError),
    Write,
    Settings,
}

pub fn try_dynamic_sender<'d>(writes: usize, bytes: usize) -> Result<Connection<'d>, Dynamic> {
    let mut conn = Setup {
        egress: options::Egress {
            write_capacity: writes,
            wire_bytes: bytes,
            ..options::Egress::default()
        },
    }
    .try_build()
    .map_err(Dynamic::Config)?;
    conn.control()
        .open_encoder(Id::try_from(3_u64).map_err(Dynamic::Stream)?)
        .map_err(Dynamic::Connection)?;
    conn.events()
        .poll_write()
        .map_err(Dynamic::Connection)?
        .ok_or(Dynamic::Write)?;
    let peer = Settings::new(
        varint::VarInt::from_u16(256),
        None,
        varint::VarInt::from_u8(1),
        Connect::Disabled,
    );
    Receive::new(&mut conn)
        .owned(
            Id::try_from(2_u64).map_err(Dynamic::Stream)?,
            peer.try_wire().map_err(Dynamic::Wire)?,
            Boundary::More,
        )
        .map_err(Dynamic::Connection)?;
    match Events::new(&mut conn)
        .poll_token()
        .map_err(Dynamic::Connection)?
    {
        Some(Event::Settings(settings)) if settings == peer => {}
        _ => return Err(Dynamic::Settings),
    }
    conn.control()
        .set_capacity(256)
        .map_err(Dynamic::Connection)?;
    conn.events()
        .poll_write()
        .map_err(Dynamic::Connection)?
        .ok_or(Dynamic::Write)?;
    Ok(conn)
}
