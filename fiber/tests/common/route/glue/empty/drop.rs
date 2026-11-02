use std::ops::Drop;

use sark_core::request::mode;

use crate::common::route::glue::empty::{__TrackedMode, DROPS, HeadHeaders, Tracked};

impl<'d, M: __TrackedMode<'d>> Drop for Tracked<'d, M> {
    fn drop(&mut self) {
        DROPS.set(DROPS.get() + 1);
    }
}

impl<'d, M: mode::Mode<'d>> Drop for HeadHeaders<'d, M> {
    fn drop(&mut self) {
        DROPS.set(DROPS.get() + 1);
    }
}
