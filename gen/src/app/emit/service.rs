use proc_macro2::TokenStream;
use quote::quote;

use super::super::spec::Gen;
use super::serve::ServeEmit;

pub(super) fn tokens(spec: &Gen) -> TokenStream {
    let emit = ServeEmit::new(spec);
    let head_parts_tokens = emit.head_parts();
    let head_visitor_tokens = emit.head_visitor();
    let plan_tokens = emit.plan();
    let app_tokens = emit.app();
    let handle_bytes_tokens = emit.handle_bytes();

    quote! {
        #head_parts_tokens
        #head_visitor_tokens
        #plan_tokens
        #app_tokens
        #handle_bytes_tokens
    }
}
