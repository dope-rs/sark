use o3::buffer::{Residency, hold};
use sark_http::payload::{owned, response::Body as _};

/// Exact response-body ownership staged by one output decision.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Owner {
    pub len: usize,
    pub charge: hold::Charge,
}

impl Owner {
    pub fn inline(len: usize) -> Self {
        Self {
            len,
            charge: hold::Charge::default(),
        }
    }

    pub fn shared<T: AsRef<[u8]> + Residency>(body: &T) -> Self {
        let len = body.as_ref().len();
        let size = Residency::resident(body);
        Self {
            len,
            charge: hold::Charge::from_owner(len, size),
        }
    }

    pub fn segmented(body: &owned::Segments<'_>) -> Self {
        Self {
            len: body.len(),
            charge: body.charge(),
        }
    }
}
