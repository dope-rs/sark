use crate::{app::serve::handle::decode::arms, macros::tokens::local, runtime::paths::Paths};

pub(super) struct Flow<'a> {
    pub(super) prepared: &'a syn::Ident,
    pub(super) state: &'a syn::Ident,
    pub(super) family: &'a syn::Ident,
    pub(super) maps: &'a [proc_macro2::TokenStream],
    pub(super) each: &'a [proc_macro2::TokenStream],
    pub(super) folds: &'a [proc_macro2::TokenStream],
    pub(super) finish: &'a [proc_macro2::TokenStream],
    pub(super) open: &'a arms::Arms,
}

impl Flow<'_> {
    pub(super) fn render(&self, paths: &Paths) -> proc_macro2::TokenStream {
        use quote::quote;

        let ns_core = &paths.core;
        let [v_each, v_map, v_fold] = ["each", "map", "fold"].map(local);

        let Self {
            prepared,
            state,
            family,
            maps,
            each,
            folds,
            finish,
            open,
        } = self;
        let open = open.render(paths, prepared);
        quote! {
            impl<F: #ns_core::decode::state::Set>
                #ns_core::decode::state::Flow<#prepared, F> for #state<F>
            {
                type Family = #family;

                fn each<'a, M: #ns_core::decode::state::Each<#prepared, F>>(
                    &'a mut self,
                    #v_each: M,
                ) -> M::Output<'a> { match self { #( #each )* } }

                fn map_retain<
                    G: #ns_core::decode::state::Set,
                    M: #ns_core::decode::state::Map<#prepared, F, G>,
                >(self, #v_map: M) ->
                    ::core::ops::ControlFlow<M::Break, (M::Retained, #state<G>)>
                { match self { #( #maps )* } }

                fn fold<T: #ns_core::decode::state::Fold<#prepared, F>>(
                    self,
                    #v_fold: T,
                ) -> T::Output { match self { #( #folds )* } }

                fn finish(self) -> ::core::result::Result<Self, Self>
                where F: #ns_core::decode::state::Finish,
                { match self { #( #finish )* } }

                #open
            }
        }
    }
}
