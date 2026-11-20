use super::Case;
use crate::{
    app::{serve::handle::decode::arms, spec},
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(super) fn render<'a>(
    paths: &Paths,
    vis: &syn::Visibility,
    prepared: &syn::Ident,
    state: &syn::Ident,
    family: &syn::Ident,
    cases: impl Iterator<Item = &'a Case> + Clone,
) -> proc_macro2::TokenStream {
    use quote::quote;

    let ns_core = &paths.core;
    let [v_each, v_fold, v_map, v_visit] = ["each", "fold", "map", "visit"].map(local);
    let [v_body, v_slot, v_ret] = ["body", "slot", "retained"].map(local);
    let variants = cases.clone().map(|case| &case.variants);
    let each = cases.clone().map(|case| &case.each);
    let folds = cases.clone().map(|case| &case.fold);
    let bodies = cases.clone().map(|case| &case.body);
    let finish = cases.clone().map(|case| &case.finish);
    let maps = cases.clone().map(|Case { open, finished, open_fn, done_fn, .. }| {
        quote! {
            Self::#open(#v_slot, #v_body) => {
                let (#v_ret, #v_slot, #v_body) = #v_map.#open_fn(#v_slot, #v_body)?;
                ::core::ops::ControlFlow::Continue((#v_ret, #state::#open(#v_slot, #v_body)))
            },
            Self::#finished(#v_slot, #v_body) => {
                let (#v_ret, #v_slot, #v_body) = #v_map.#done_fn(#v_slot, #v_body)?;
                ::core::ops::ControlFlow::Continue((#v_ret, #state::#finished(#v_slot, #v_body)))
            },
        }
    });
    let mut open = arms::Arms::new();
    for case in cases {
        match case.kind {
            spec::Kind::Sync | spec::Kind::Fiber => {
                open.push_active(&case.end_fn, &case.open, &case.finished)
            }
            spec::Kind::Stream | spec::Kind::Duplex => open.push_never(&case.open, &case.finished),
        }
    }
    let open = open.render(paths, prepared);
    quote! {
        #[doc(hidden)] #vis struct #family;
        #[doc(hidden)]
        #vis enum #state<F: #ns_core::decode::state::Set> { #( #variants )* }
        impl #ns_core::decode::state::Family<#prepared> for #family {
            type State<F: #ns_core::decode::state::Set> = #state<F>;
        }
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

        impl<F: #ns_core::decode::state::Set>
            #ns_core::decode::state::body::Flow<F> for #state<F>
        {
            fn fold<V: #ns_core::decode::state::body::Fold<F>>(
                self,
                #v_visit: V,
            ) -> V::Output { match self { #( #bodies )* } }
        }
    }
}
