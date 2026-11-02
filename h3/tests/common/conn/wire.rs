use std::time::Instant;

use dope_quic::{
    conn::{self, Connection, output::transmit},
    rustls,
};
use o3::buffer::resident;
use sark_h3::manifold::{self, Session};

#[derive(Debug)]
pub enum Transfer {
    Output(transmit::Error),
    Input(conn::Error),
}

pub struct Peer<'a, 'd> {
    work: &'a mut conn::Workspace,
    from: &'a mut Connection<0, Vec<u8>, resident::Shared<'d>>,
}

impl<'a, 'd> Peer<'a, 'd> {
    pub fn new(
        work: &'a mut conn::Workspace,
        from: &'a mut Connection<0, Vec<u8>, resident::Shared<'d>>,
    ) -> Self {
        Self { work, from }
    }

    pub fn try_send(
        self,
        into: &mut Connection<0, Vec<u8>, resident::Shared<'d>>,
        now: Instant,
        report: impl Fn(&rustls::Error),
    ) -> Result<(), Transfer> {
        let (packets, result) = self.from.with_scope(|mut scope| scope.output().send(now));
        result.map_err(Transfer::Output)?;
        for mut packet in packets {
            into.with_scope(|mut scope| {
                scope
                    .input(self.work)
                    .recv_packet(&mut packet, now, &report)
            })
            .map_err(Transfer::Input)?;
        }
        Ok(())
    }
}

pub struct Ingress<'a, 'd> {
    session: &'a mut Session<'d>,
}

impl<'a, 'd> Ingress<'a, 'd> {
    pub fn new(session: &'a mut Session<'d>) -> Self {
        Self { session }
    }

    pub fn try_receive(
        self,
        quic: &mut Connection<0, Vec<u8>, resident::Shared<'d>>,
    ) -> Result<(), manifold::Error> {
        quic.with_scope(|mut scope| {
            while let Some(event) = scope.streams().events().poll() {
                self.session
                    .ingress()
                    .ingest(&mut scope, manifold::Notification::from(event))?;
            }
            Ok(())
        })
    }
}
