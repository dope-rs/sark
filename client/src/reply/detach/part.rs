/// One response component involved in a detach failure.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Part {
    Headers,
    Trailers,
    Body,
}

const _: () = assert!(size_of::<Part>() == 1);
