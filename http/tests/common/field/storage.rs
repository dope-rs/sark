use sark_http::field::storage::Constant;

pub struct Auth;

impl Constant for Auth {
    const VALUE: &'static str = "Authorization";
}
