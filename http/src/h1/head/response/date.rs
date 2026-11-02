/// Date-header action for one encoded HTTP/1 response head.
#[derive(Clone, Copy)]
#[repr(u8)]
pub enum Date {
    Omit,
    Include,
}

const _: () = assert!(core::mem::size_of::<Date>() == core::mem::size_of::<bool>());
