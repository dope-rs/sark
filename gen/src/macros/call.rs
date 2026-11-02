use proc_macro2::{Span, TokenStream};
use quote::quote_spanned;

use crate::{macros::tokens, runtime::paths::Paths};

pub(crate) fn installed(paths: &Paths, result: TokenStream) -> TokenStream {
    let [v_error, v_req] = ["error", "request"].map(tokens::local);
    let ns_fiber = &paths.fiber;
    quote_spanned! {Span::mixed_site()=> match #result {
        ::core::result::Result::Ok(::core::result::Result::Ok(::core::result::Result::Ok(#v_req))) =>
            ::core::result::Result::Ok(#v_req),
        ::core::result::Result::Ok(::core::result::Result::Ok(::core::result::Result::Err(#v_error))) =>
            ::core::result::Result::Err(#ns_fiber::dope::runtime::topology::Failure::Run(
                #ns_fiber::dope::runtime::executor::session::Error::Drive(#v_error),
            )),
        ::core::result::Result::Ok(::core::result::Result::Err(#v_error)) =>
            ::core::result::Result::Err(#ns_fiber::dope::runtime::topology::Failure::Run(#v_error)),
        ::core::result::Result::Err(#v_error) =>
            ::core::result::Result::Err(#ns_fiber::dope::runtime::topology::Failure::Install(#v_error)),
    }}
}
