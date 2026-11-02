use crate::{field::metadata::block, payload::response, uri};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Plan {
    form: block::Form,
    scheme: uri::Scheme,
    authority: block::Authority,
    method: response::Method,
}

impl Plan {
    pub(super) const fn new(
        form: block::Form,
        scheme: uri::Scheme,
        source: block::Authority,
        method: response::Method,
    ) -> Self {
        Self {
            form,
            scheme,
            authority: source,
            method,
        }
    }

    pub const fn form(self) -> block::Form {
        self.form
    }

    pub const fn scheme(self) -> uri::Scheme {
        self.scheme
    }

    pub const fn authority(self) -> block::Authority {
        self.authority
    }

    pub const fn method(self) -> response::Method {
        self.method
    }
}
