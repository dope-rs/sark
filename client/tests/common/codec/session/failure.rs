use dope::{
    core::Errno,
    manifold::client::{attempt::ConnectFault, close::Reason},
    net::link::{egress, pool::change::opening::Exhaustion},
};
use sark_client::protocol::Error;

#[derive(Debug)]
pub enum Failure {
    Protocol(Error),
    Connect(ConnectFault),
    Exhausted(Exhaustion),
    Io(Errno),
    Egress(egress::Rejection),
    Closed(Reason),
    UnexpectedEvent,
}
