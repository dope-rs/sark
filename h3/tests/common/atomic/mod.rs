use dope_quic::wire::{stream::Id, varint};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection,
        event::{Event, Role},
        io::Receive,
        options::{self, Config},
    },
    frame::Settings,
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

pub fn dynamic_sender<'d>(writes: usize, bytes: usize) -> Connection<'d> {
    let mut conn = Setup::new(writes, bytes, options::Egress::default().resident_bytes)
        .try_build()
        .expect("dynamic configuration");
    conn.control()
        .open_encoder(Id::try_from(3_u64).expect("encoder stream"))
        .expect("open encoder");
    conn.events()
        .poll_write()
        .expect("encoder write")
        .expect("encoder prefix");
    let peer = Settings::new(
        varint::VarInt::from_u16(256),
        None,
        varint::VarInt::from_u8(1),
        Connect::Disabled,
    );
    Receive::new(&mut conn)
        .owned(
            Id::try_from(2_u64).expect("control stream"),
            peer.try_wire().expect("SETTINGS wire"),
            Boundary::More,
        )
        .expect("peer SETTINGS");
    assert!(matches!(
        Events::new(&mut conn).poll_token().expect("SETTINGS event"),
        Some(Event::Settings(settings)) if settings == peer
    ));
    conn.control().set_capacity(256).expect("table capacity");
    conn.events()
        .poll_write()
        .expect("capacity write")
        .expect("capacity instruction");
    conn
}
