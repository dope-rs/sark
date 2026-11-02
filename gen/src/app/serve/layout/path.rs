use crate::{macros::tokens::local, runtime::paths::Paths};

pub(in crate::app::serve) struct Path(proc_macro2::TokenStream);

impl Path {
    pub(in crate::app::serve) fn new(paths: &Paths, leaves: usize, slot: usize) -> Self {
        use quote::quote;
        let ns_fiber = &paths.fiber;

        let path = match leaves {
            0 | 1 => quote!(#ns_fiber::dope_fiber::task::storage::fixed::tree::Here),
            leaves => {
                let split = leaves / 2;
                if slot < split {
                    let path = Self::new(paths, split, slot);
                    quote!(#ns_fiber::dope_fiber::task::storage::fixed::tree::Left<#path>)
                } else {
                    let path = Self::new(paths, leaves - split, slot - split);
                    quote!(#ns_fiber::dope_fiber::task::storage::fixed::tree::Right<#path>)
                }
            }
        };
        Self(path)
    }

    pub(in crate::app::serve) fn route(
        &self,
        generics: &crate::types::Generics,
    ) -> proc_macro2::TokenStream {
        let f_tasks = local("tasks");
        use quote::quote;

        let path = self;
        let this = generics.local("this");
        quote! { #this.#f_tasks.as_mut().scope().into_route::<#path>() }
    }
}

impl quote::ToTokens for Path {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        self.0.to_tokens(tokens);
    }
}
