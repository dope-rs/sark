use dope::{
    core::Errno,
    manifold::client::close::Reason,
    net::link::{event::ConnectFault, pool::change::opening::Exhaustion},
};
use sark_client::protocol::Error;

#[derive(Debug)]
pub enum Failure {
    Protocol(Error),
    Connect(ConnectFault),
    Exhausted(Exhaustion),
    Io(Errno),
    Closed(Reason),
    UnexpectedEvent,
}
