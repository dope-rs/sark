use std::str;

use o3::buffer::resident;

/// One admitted close payload whose reason suffix was validated as UTF-8.
pub(in crate::client) struct Close<'d> {
    payload: resident::Shared<'d>,
    code: Option<u16>,
}

impl<'d> Close<'d> {
    /// # Safety
    /// This unchanged owner is the complete admitted close payload: `None`
    /// requires an empty payload; `Some` requires its code and UTF-8 suffix.
    pub(super) unsafe fn admitted(payload: resident::Shared<'d>, code: Option<u16>) -> Self {
        Self { payload, code }
    }

    pub(in crate::client) const fn code(&self) -> Option<u16> {
        self.code
    }

    pub(in crate::client) fn reason(&self) -> &str {
        let start = usize::from(self.code.is_some()) * 2;
        // SAFETY: admission retains the exact immutable payload and proves
        // its reason starts after the code, or at zero for an empty close.
        unsafe { str::from_utf8_unchecked(self.payload.as_ref().get_unchecked(start..)) }
    }
}
