use std::{
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpStream},
    sync::Arc,
    time::Duration,
};

use dope_tls::rustls::{self, ClientConnection, StreamOwned, pki_types::ServerName};
use o3::base::marker::ThreadBound;
use sark_h2::{ClientRole, conn::machine::Connection};

pub const SEED: [u8; 32] = [11; 32];
pub const RECORDS: usize = 65;
pub const CHUNK: [u8; 512] = [b'x'; 512];
pub const BODY_LEN: usize = RECORDS * CHUNK.len();
pub const PING: [u8; 8] = *b"retain65";
pub const RESPONSE: &[u8] = b"retained";

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Tls(rustls::Error),
    Closed { buffered: usize },
    Handshake { exchanges: usize },
    Stalled,
}

pub struct Peer {
    inner: StreamOwned<ClientConnection, TcpStream>,
    _bound: ThreadBound,
}

impl Peer {
    pub fn connect(
        address: SocketAddr,
        config: Arc<rustls::ClientConfig>,
        name: ServerName<'static>,
    ) -> Result<Self, Error> {
        let state = ClientConnection::new(config, name).map_err(Error::Tls)?;
        let socket = TcpStream::connect(address).map_err(Error::Io)?;
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .map_err(Error::Io)?;
        socket
            .set_write_timeout(Some(Duration::from_secs(3)))
            .map_err(Error::Io)?;
        let mut peer = Self {
            inner: StreamOwned::new(state, socket),
            _bound: ThreadBound::NEW,
        };
        let mut buffered = 0;
        for _ in 0..64 {
            peer.flush_handshake()?;
            if !peer.inner.conn.is_handshaking() {
                return Ok(peer);
            }
            if peer
                .inner
                .conn
                .read_tls(&mut peer.inner.sock)
                .map_err(Error::Io)?
                == 0
            {
                return Err(Error::Closed { buffered });
            }
            buffered = peer
                .inner
                .conn
                .process_new_packets()
                .map_err(Error::Tls)?
                .plaintext_bytes_to_read();
            peer.flush_handshake()?;
        }
        Err(Error::Handshake { exchanges: 64 })
    }

    fn flush_handshake(&mut self) -> Result<(), Error> {
        while self.inner.conn.wants_write() {
            if self
                .inner
                .conn
                .write_tls(&mut self.inner.sock)
                .map_err(Error::Io)?
                == 0
            {
                return Err(Error::Stalled);
            }
        }
        Ok(())
    }

    pub fn close(mut self) -> io::Result<Vec<u8>> {
        self.inner.conn.send_close_notify();
        self.inner.flush()?;
        self.inner.sock.shutdown(Shutdown::Write)?;
        let mut plain = Vec::new();
        self.inner.read_to_end(&mut plain)?;
        Ok(plain)
    }
}

impl Read for Peer {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.inner.read(bytes)
    }
}

impl Write for Peer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub trait Output {
    fn flush(&mut self, wire: &mut Peer) -> io::Result<()>;
}

impl Output for Connection<ClientRole> {
    fn flush(&mut self, wire: &mut Peer) -> io::Result<()> {
        loop {
            let bytes = self.output().first();
            if bytes.is_empty() {
                return Ok(());
            }
            let length = bytes.len();
            wire.write_all(bytes)?;
            wire.flush()?;
            self.transfer().drain(length);
        }
    }
}

pub fn padded(mut bytes: Vec<u8>) -> Option<Vec<u8>> {
    let length = 16384usize.checked_sub(bytes.len())?.checked_sub(9)? as u32;
    bytes.extend_from_slice(&length.to_be_bytes()[1..]);
    bytes.extend_from_slice(&[255, 0, 0, 0, 0, 0]);
    bytes.resize(16384, 0);
    Some(bytes)
}
