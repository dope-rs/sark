#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Axis {
    Body,
    Wire,
    Line,
    Trailer,
    Fields,
    Name,
    Size,
}
