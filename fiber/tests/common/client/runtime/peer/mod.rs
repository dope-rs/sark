use std::{io, io::Write as _, net, num::NonZero};

use dope_test::harness::peer::{Stream, TcpAcceptor};

mod conn;
mod error;
mod input;
mod request;
mod response;

pub use conn::Conn;
pub use error::Error;
pub use request::Request;
pub use response::Response;

pub fn serve(
    acceptor: &mut TcpAcceptor<'_>,
    mut reply: impl for<'a> FnMut(Request<'a>) -> Vec<u8>,
) -> Result<(), Error> {
    while let Some(accepted) = accept(acceptor)? {
        let mut stream = &accepted;
        let mut input = input::Input::new();
        while let Some(frame) = next(&mut input, stream)? {
            let output = reply(input.request(frame)?);
            input.consume(frame);
            stream.write_all(&output).map_err(Error::Io)?;
        }
    }
    Ok(())
}

/// Serves one framed exchange on each accepted connection.
pub fn serve_each(
    acceptor: &mut TcpAcceptor<'_>,
    mut run: impl for<'a> FnMut(&mut &Stream<'_>, Request<'a>) -> io::Result<()>,
) -> Result<(), Error> {
    while let Some(accepted) = accept(acceptor)? {
        let mut stream = &accepted;
        let mut input = input::Input::new();
        if let Some(frame) = next(&mut input, stream)? {
            run(&mut stream, input.request(frame)?).map_err(Error::Io)?;
        }
    }
    Ok(())
}

/// Counts connections while serving an exact nonzero request budget.
pub fn counted(
    acceptor: &mut TcpAcceptor<'_>,
    requests: NonZero<usize>,
    mut reply: impl FnMut(&mut Conn<&Stream<'_>>, usize) -> Result<(), Error>,
) -> Result<usize, Error> {
    let mut count = 0;
    let mut done = 0;
    while done < requests.get() {
        let accepted = acceptor.accept().map_err(Error::Io)?;
        let mut conn = Conn::new(&accepted);
        count += 1;
        while done < requests.get() && conn.try_request()?.is_some() {
            done += 1;
            reply(&mut conn, done)?;
        }
    }
    Ok(count)
}

/// After client teardown, checks every queued connection for submitted bytes.
pub fn empty(acceptor: &mut TcpAcceptor<'_>) -> Result<(), Error> {
    while let Some(accepted) = acceptor.try_accept().map_err(Error::Io)? {
        closed(&accepted)?;
    }
    Ok(())
}

/// Checks an accepted connection, including any retained framing bytes, for EOF.
pub fn closed(mut stream: impl io::Read) -> Result<(), Error> {
    let mut byte = [0];
    if stream.read(&mut byte).map_err(Error::Io)? != 0 {
        return Err(Error::Submitted(byte[0]));
    }
    Ok(())
}

/// Closes a peer socket, accepting an already retired connection.
pub fn try_shutdown(stream: &Stream<'_>) -> io::Result<()> {
    match stream.shutdown(net::Shutdown::Both) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
        Err(error) => Err(error),
    }
}

pub trait Ready {
    fn try_read(&mut self, output: &mut [u8]) -> io::Result<usize>;
}

impl Ready for &net::TcpStream {
    fn try_read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.set_nonblocking(true)?;
        let outcome = io::Read::read(self, output);
        self.set_nonblocking(false)?;
        outcome
    }
}

impl Ready for &Stream<'_> {
    fn try_read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.nonblock(true);
        let outcome = io::Read::read(self, output);
        self.nonblock(false);
        outcome
    }
}

fn next(input: &mut input::Input, mut stream: &Stream<'_>) -> Result<Option<input::Frame>, Error> {
    match input.next(&mut stream) {
        Err(Error::Io(error))
            if error.kind() == io::ErrorKind::ConnectionAborted
                && stream.is_stopped()
                && input.is_empty() =>
        {
            Ok(None)
        }
        result => result,
    }
}

/// Accept cancellation is terminal only before a socket has been acquired.
fn accept<'a>(acceptor: &mut TcpAcceptor<'a>) -> Result<Option<Stream<'a>>, Error> {
    match acceptor.accept() {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.kind() == io::ErrorKind::Interrupted => Ok(None),
        Err(error) => Err(Error::Io(error)),
    }
}
