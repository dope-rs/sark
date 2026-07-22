use std::fmt;

#[derive(Debug)]
pub enum Error {
    NotConnected,
    Closed,
    Backpressure,
    CapacityOverflow,
    WaiterCapacity,
    Timeout,
    Parse(String),
    Http(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotConnected => f.write_str("http conn not yet ready"),
            Self::Closed => f.write_str("connection closed"),
            Self::Backpressure => f.write_str("egress over cap"),
            Self::CapacityOverflow => f.write_str("response buffer capacity exceeded"),
            Self::WaiterCapacity => f.write_str("waiter capacity exhausted"),
            Self::Timeout => f.write_str("request timed out"),
            Self::Parse(msg) => write!(f, "response parse error: {msg}"),
            Self::Http(msg) => write!(f, "http error: {msg}"),
        }
    }
}

impl std::error::Error for Error {}
