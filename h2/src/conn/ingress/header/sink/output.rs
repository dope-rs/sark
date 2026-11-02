use sark_http::field::metadata::section::{rules, state::decode::Final};

use crate::conn::ingress::header;

pub(in crate::conn::ingress) struct Decoded<P: rules::Policy> {
    pub(in crate::conn::ingress) head: Final<P>,
    pub(in crate::conn::ingress) validation: header::Validation,
}
