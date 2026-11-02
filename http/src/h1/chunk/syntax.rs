#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Syntax {
    Size,
    SizeTail,
    NameStart,
    Name,
    NameTail,
    ValueStart,
    Token,
    ValueEnd,
    ValueTail,
    Quoted,
    Escape,
    SizeLf,
    DataCr,
    DataLf,
    TrailerStart,
    TrailerName,
    TrailerValue,
    TrailerLf,
    TrailerEnd,
}
