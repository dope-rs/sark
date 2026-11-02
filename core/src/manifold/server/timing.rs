//! Static application deadline topology.

/// Application with no deadline state or clock work.
#[derive(Clone, Copy, Default)]
pub struct Off;

/// Application with connection-local deadline state.
#[derive(Clone, Copy, Default)]
pub struct On;

const _: () = assert!(core::mem::size_of::<Off>() == 0);
const _: () = assert!(core::mem::size_of::<On>() == 0);
