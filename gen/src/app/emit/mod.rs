mod serve;
mod service;

use super::spec::Gen;
use proc_macro2::TokenStream;

pub(super) fn render(spec: &Gen) -> TokenStream {
    service::tokens(spec)
}
