use sark_h2::{
    ClientRole, ServerRole,
    conn::{self, CLIENT_PREFACE, event::Event, machine::Connection},
};

#[derive(Debug)]
pub enum Error {
    Transfer(conn::Error),
    Preface,
    Settings,
}

pub struct Preface<'a>(&'a [u8]);

impl<'a> Preface<'a> {
    pub fn new(settings: &'a [u8]) -> Self {
        Self(settings)
    }
    pub fn try_server(self, conn: &mut Connection<ServerRole>) -> Result<(), Error> {
        conn.transfer().drain(usize::MAX);
        let mut preface = CLIENT_PREFACE.to_vec();
        preface.extend_from_slice(self.0);
        conn.ingress().ingest(&preface).map_err(Error::Transfer)?;
        match conn.ingress().poll_event() {
            Some(Event::PrefaceComplete) => (),
            _ => return Err(Error::Preface),
        }
        match conn.ingress().poll_event() {
            Some(Event::SettingsApplied) => (),
            _ => return Err(Error::Settings),
        }
        conn.transfer().drain(usize::MAX);
        Ok(())
    }

    pub fn try_client(self, conn: &mut Connection<ClientRole>) -> Result<(), Error> {
        match conn.ingress().poll_event() {
            Some(Event::PrefaceComplete) => (),
            _ => return Err(Error::Preface),
        }
        conn.transfer().drain(usize::MAX);
        conn.ingress().ingest(self.0).map_err(Error::Transfer)?;
        match conn.ingress().poll_event() {
            Some(Event::SettingsApplied) => (),
            _ => return Err(Error::Settings),
        }
        conn.transfer().drain(usize::MAX);
        Ok(())
    }
}
