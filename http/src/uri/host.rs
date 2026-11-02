use core::mem;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Host {
    Name,
    Future,
    Invalid,
}

const _: () = assert!(mem::size_of::<Host>() == 1);
