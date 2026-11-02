use std::{cell, ptr};

use crate::server::runtime::state::active::Signals;

pub(super) struct Node {
    pub(super) value: Signals,
    pub(super) next: cell::Cell<Option<ptr::NonNull<Node>>>,
}
