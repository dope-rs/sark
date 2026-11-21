use std::ops::Drop;

use crate::common::route::glue::empty::{DROPS, HeadHeaders, Tracked};

impl<B> Drop for Tracked<'_, B> {
    fn drop(&mut self) {
        DROPS.set(DROPS.get() + 1);
    }
}

impl Drop for HeadHeaders<'_> {
    fn drop(&mut self) {
        DROPS.set(DROPS.get() + 1);
    }
}
