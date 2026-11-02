use crate::{macros::tokens::local, runtime::paths::Paths};

pub(super) struct Arms {
    borrow: Vec<proc_macro2::TokenStream>,
    take: Vec<proc_macro2::TokenStream>,
    consume: Vec<proc_macro2::TokenStream>,
}

impl Arms {
    pub(super) fn new() -> Self {
        Self {
            borrow: Vec::new(),
            take: Vec::new(),
            consume: Vec::new(),
        }
    }

    pub(super) fn push_active(
        &mut self,
        action: &syn::Ident,
        open: &syn::Ident,
        done: &syn::Ident,
    ) {
        use quote::quote;

        let [v_body, v_never, v_slot, v_visit] = ["body", "never", "slot", "visit"].map(local);
        let borrow = quote! {
            Self::#open(.., #v_body) => #v_visit.apply(#v_body),
            Self::#done(.., #v_never) => match *#v_never {},
        };
        let take = quote! {
            Self::#open(#v_slot, #v_body) => #v_visit.#action(#v_slot, #v_body),
            Self::#done(.., #v_never) => match #v_never {},
        };
        let consume = quote! {
            Self::#open(.., #v_body) => #v_visit(#v_body),
            Self::#done(.., #v_never) => match #v_never {},
        };
        self.borrow.push(borrow);
        self.take.push(take);
        self.consume.push(consume);
    }

    pub(super) fn push_never(&mut self, open: &syn::Ident, done: &syn::Ident) {
        use quote::quote;

        let v_never = local("never");

        self.borrow.push(quote! {
            Self::#open(.., #v_never) => match *#v_never {},
            Self::#done(.., #v_never) => match *#v_never {},
        });
        let arms = quote! {
            Self::#open(.., #v_never) => match #v_never {},
            Self::#done(.., #v_never) => match #v_never {},
        };
        self.take.push(arms.clone());
        self.consume.push(arms);
    }

    pub(super) fn render(&self, paths: &Paths, prep: &syn::Ident) -> proc_macro2::TokenStream {
        use quote::quote;

        let ns_core = &paths.core;
        let v_visit = local("visit");

        let Self {
            borrow,
            take,
            consume,
        } = self;
        quote! {
                fn each_open<'a, V: #ns_core::decode::state::open::Each<F>>(
                    &'a mut self,
                    #v_visit: V,
                ) -> V::Output<'a>
                where F: #ns_core::decode::state::open::Set,
                { match self { #( #borrow )* } }

                fn fold_open<V: #ns_core::decode::state::open::Fold<#prep, F>>(
                    self,
                    #v_visit: V,
                ) -> V::Output
                where F: #ns_core::decode::state::open::Set,
                { match self { #( #take )* } }

                fn consume<O, V: ::core::ops::FnOnce(F::Body) -> O>(
                    self,
                    #v_visit: V,
                ) -> O
                where F: #ns_core::decode::state::open::Set,
                { match self { #( #consume )* } }
        }
    }
}
