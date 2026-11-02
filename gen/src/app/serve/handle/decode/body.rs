use crate::{macros::tokens::local, runtime::paths::Paths};

pub(super) struct Body {
    core: Vec<proc_macro2::TokenStream>,
    plain: Vec<proc_macro2::TokenStream>,
    guard: Vec<proc_macro2::TokenStream>,
}

impl Body {
    pub(super) const fn new() -> Self {
        Self {
            core: Vec::new(),
            plain: Vec::new(),
            guard: Vec::new(),
        }
    }

    pub(super) fn push(
        &mut self,
        paths: &Paths,
        open_ty: &proc_macro2::TokenStream,
        done_ty: &proc_macro2::TokenStream,
        open: &syn::Ident,
        done: &syn::Ident,
        guarded: bool,
    ) {
        use quote::quote;

        let ns_core = &paths.core;
        let [v_body, v_visit] = ["body", "visit"].map(local);

        let arms = quote! {
            Self::#open(.., #v_body) => #v_visit.apply(
                #ns_core::decode::state::body::Value::#open_ty(#v_body),
            ),
            Self::#done(.., #v_body) => #v_visit.apply(
                #ns_core::decode::state::body::Value::#done_ty(#v_body),
            ),
        };
        self.core.push(arms.clone());
        if guarded {
            self.guard.push(arms);
        } else {
            self.plain.push(arms);
        }
    }

    pub(super) fn render(
        self,
        paths: &Paths,
        core: &syn::Ident,
        plain: &syn::Ident,
        guard: &syn::Ident,
    ) -> [proc_macro2::TokenStream; 3] {
        use quote::quote;

        let ns_core = &paths.core;
        let v_visit = local("visit");

        let Self {
            core: c,
            plain: p,
            guard: g,
        } = self;
        let render = |arms: Vec<proc_macro2::TokenStream>, state: &syn::Ident| {
            quote! {
                impl<F: #ns_core::decode::state::Set>
                    #ns_core::decode::state::body::Flow<F> for #state<F>
                {
                    fn fold<V: #ns_core::decode::state::body::Fold<F>>(
                        self,
                        #v_visit: V,
                    ) -> V::Output { match self { #( #arms )* } }
                }
            }
        };
        [render(c, core), render(p, plain), render(g, guard)]
    }
}
