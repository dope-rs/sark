#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Phase {
    Head,
    Body,
    Trailers,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Event {
    Informational,
    Head,
    Data,
    Trailers,
    End,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct Error {
    phase: Phase,
    event: Event,
}

impl Error {
    pub const fn new(phase: Phase, event: Event) -> Self {
        Self { phase, event }
    }
}

const _: () = assert!(size_of::<Phase>() == 1);
const _: () = assert!(size_of::<Event>() == 1);
const _: () = assert!(size_of::<Error>() == 2);
