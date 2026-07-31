use proc_macro2::TokenStream;
use quote::quote;

use super::super::spec::Gen;
use super::serve::ServeEmit;

pub(super) fn tokens(spec: &Gen) -> TokenStream {
    let emit = ServeEmit::new(spec);
    let app_tokens = emit.app();
    let handle_bytes_tokens = emit.handle_bytes();

    quote! {
        #app_tokens
        #handle_bytes_tokens
    }
}
