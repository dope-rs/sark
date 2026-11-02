/// Server-header action for one encoded HTTP/1 response head.
#[derive(Clone, Copy)]
#[repr(u8)]
pub enum Server {
    Omit,
    Include,
}

const _: () = assert!(core::mem::size_of::<Server>() == core::mem::size_of::<bool>());
