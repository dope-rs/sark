pub(in crate::app::serve::handle) struct Names {
    pub(in crate::app::serve::handle) prepared_ident: syn::Ident,
    pub(in crate::app::serve::handle) head_plan_ident: syn::Ident,
    pub(in crate::app::serve::handle) head_route_ident: syn::Ident,
    pub(in crate::app::serve::handle) head_selection_ident: syn::Ident,
    pub(in crate::app::serve::handle) cause: proc_macro2::TokenStream,
}
