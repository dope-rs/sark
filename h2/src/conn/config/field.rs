#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Field {
    Stream,
    Hpack,
    Event,
    Inbound,
    Outbound,
}
