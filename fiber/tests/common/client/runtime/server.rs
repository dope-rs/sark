use std::{net::TcpListener, str, sync::Arc, time::Duration};

use dope_tls::rustls;

use crate::common::client::runtime::sockets::{Error, Session};

pub struct Server {
    pub listener: TcpListener,
}

pub struct Peer(Session);

pub const RETAINED_PATHS: [&str; 5] = ["/held", "/retained", "/second", "/third", "/finish"];

impl Server {
    pub fn try_run<T>(
        self,
        config: Arc<rustls::ServerConfig>,
        cases: impl IntoIterator<Item = T>,
        mut serve: impl FnMut(Peer, T) -> Result<(), Error>,
    ) -> Result<(), Error> {
        for case in cases {
            serve(self.try_peer(&config)?, case)?;
        }
        Ok(())
    }

    fn try_peer(&self, config: &Arc<rustls::ServerConfig>) -> Result<Peer, Error> {
        let state = rustls::ServerConnection::new(Arc::clone(config)).map_err(Error::Tls)?;
        let (stream, _) = self.listener.accept().map_err(Error::Io)?;
        stream.set_nodelay(true).map_err(Error::Io)?;
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .map_err(Error::Io)?;
        stream
            .set_write_timeout(Some(Duration::from_secs(10)))
            .map_err(Error::Io)?;
        Ok(Peer(Session::new(stream, state.into())))
    }

    /// Omits close_notify so TCP FIN exposes the original truncated response.
    pub fn try_truncate(
        self,
        config: Arc<rustls::ServerConfig>,
        paths: &[&str],
    ) -> Result<(), Error> {
        self.try_run(config, paths, |mut peer, path| {
            peer.try_ready()?;
            peer.try_request("POST", path)
        })
    }

    /// Defers replacement readiness until the client observes retirement.
    pub fn try_retained(self, config: Arc<rustls::ServerConfig>) -> Result<(), Error> {
        let [first, failed, second, third, finish] = RETAINED_PATHS;
        let mut previous = self.try_peer(&config)?;
        previous.try_ready()?;
        let mut armed = self.try_peer(&config)?;
        armed.try_ready()?;
        let closed = previous.try_request("POST", failed);
        drop(previous);
        closed?;
        let mut peer = self.try_peer(&config)?;
        armed.try_request("GET", first)?;
        peer.try_ready()?;
        armed.try_request("GET", second)?;
        armed.try_request("GET", third)?;
        for _ in 0..3 {
            armed.try_write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")?;
        }
        peer.try_request("GET", finish)?;
        peer.try_write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
    }
}

impl Peer {
    pub fn try_ready(&mut self) -> Result<(), Error> {
        self.0.handshake()
    }

    pub fn try_request(&mut self, method: &str, path: &str) -> Result<(), Error> {
        loop {
            if let Some(end) = self
                .0
                .plain
                .windows(4)
                .position(|bytes| bytes == b"\r\n\r\n")
            {
                let request = str::from_utf8(&self.0.plain[..end + 4]).map_err(Error::Utf8)?;
                let found = request.lines().next();
                let expected = format!("{method} {path} HTTP/1.1");
                if found != Some(expected.as_str()) {
                    return Err(Error::Request {
                        found: found.map(str::to_owned),
                        expected,
                    });
                }
                self.0.plain.drain(..end + 4);
                return Ok(());
            }
            if self.0.read()? == 0 {
                return Err(Error::Closed {
                    buffered: self.0.plain.len(),
                });
            }
        }
    }

    pub fn try_write(&mut self, plain: &[u8]) -> Result<(), Error> {
        self.0.write(plain)
    }

    /// Waits for TLS closure or transport EOF and rejects leftover plaintext.
    pub fn try_closed(&mut self) -> Result<(), Error> {
        while self.0.read()? != 0 {}
        if self.0.plain.is_empty() {
            return Ok(());
        }
        Err(Error::Closed {
            buffered: self.0.plain.len(),
        })
    }
}
