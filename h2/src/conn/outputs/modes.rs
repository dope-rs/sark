pub(crate) enum Buffered {}
pub(crate) enum Retained {}

pub(crate) trait Mode {
    const RETAINED: bool;
}

impl Mode for Buffered {
    const RETAINED: bool = false;
}

impl Mode for Retained {
    const RETAINED: bool = true;
}
