use crate::{app::spec, runtime::paths::Paths};

#[derive(Clone, Copy)]
pub(in crate::app::serve) struct Id(usize);

#[derive(Clone, Copy)]
pub(in crate::app::serve) enum State {
    Disabled,
    Enabled,
}

#[derive(Clone, Copy)]
pub(in crate::app::serve) enum Connect {
    Disabled,
    Enabled,
}

impl State {
    pub(in crate::app::serve) fn input_type(self, paths: &Paths) -> proc_macro2::TokenStream {
        let ns_fiber = &paths.fiber;
        match self {
            Self::Enabled => quote::quote!(#ns_fiber::dope::manifold::app::receive::GatedScoped),
            Self::Disabled => quote::quote!(#ns_fiber::dope::manifold::app::receive::Coupled),
        }
    }

    pub(in crate::app::serve) fn retention(self, paths: &Paths) -> proc_macro2::TokenStream {
        let ns_fiber = &paths.fiber;
        match self {
            Self::Disabled => quote::quote!(()),
            Self::Enabled => quote::quote!(#ns_fiber::dope::manifold::app::receive::limits::Budget),
        }
    }

    pub(in crate::app::serve) fn config(self, paths: &Paths) -> proc_macro2::TokenStream {
        let ns_fiber = &paths.fiber;
        let budget = self.retention(paths);
        quote::quote!(#ns_fiber::Config<#budget>)
    }

    pub(in crate::app::serve) fn stamp(
        self,
        clock: proc_macro2::TokenStream,
        budget: proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        match self {
            Self::Disabled => clock,
            Self::Enabled => quote::quote!((#clock, #budget)),
        }
    }

    pub(in crate::app::serve) fn clock(
        self,
        generics: &crate::types::Generics,
    ) -> proc_macro2::TokenStream {
        let [v_date] = generics.locals(["date"]);
        match self {
            Self::Disabled => quote::quote!(#v_date.as_ref().get_ref()),
            Self::Enabled => quote::quote!(#v_date.as_ref().get_ref().0),
        }
    }
}

impl Id {
    fn variant(self) -> syn::Ident {
        let slot = self.0;
        quote::format_ident!("__Route{slot:04}")
    }
}

impl Connect {
    pub(in crate::app::serve) fn register(self, entry: &spec::Entry) -> Self {
        match (self, &entry.protocol) {
            (Self::Enabled, _) | (_, Some(_)) => Self::Enabled,
            (Self::Disabled, None) => Self::Disabled,
        }
    }
}

/// Interns repeated registrations of each nominal duplex route family.
pub(in crate::app::serve) struct Plan<'spec> {
    spec: &'spec spec::Spec,
    set: Vec<&'spec syn::TypePath>,
    connect: Connect,
}

impl<'spec> Plan<'spec> {
    pub(in crate::app::serve) fn new(spec: &'spec spec::Spec) -> Self {
        Self {
            spec,
            set: Vec::new(),
            connect: Connect::Disabled,
        }
    }

    pub(in crate::app::serve) fn register(&mut self, entry: &'spec spec::Entry) -> Id {
        self.connect = self.connect.register(entry);
        if let Some(slot) = self.set.iter().position(|route| *route == &entry.route) {
            return Id(slot);
        }
        let id = Id(self.set.len());
        self.set.push(&entry.route);
        id
    }

    pub(in crate::app::serve) fn state(&self) -> State {
        match self.set.as_slice() {
            [] => State::Disabled,
            [_, ..] => State::Enabled,
        }
    }

    pub(in crate::app::serve) const fn connect(&self) -> Connect {
        self.connect
    }

    fn ident(&self) -> syn::Ident {
        quote::format_ident!("__{}Duplex", self.spec.name)
    }

    pub(in crate::app::serve) fn ty(&self, paths: &Paths) -> proc_macro2::TokenStream {
        let l_env = self.spec.state_generics.lifetime("env");
        use quote::quote;
        let ns_core = &paths.core;

        match self.state() {
            State::Disabled => return quote!(#ns_core::duplex::Never),
            State::Enabled => {}
        }
        if let [route] = self.set.as_slice() {
            let state = self.spec.state.ty();
            return quote!(<#route as #ns_core::duplex::Route<#l_env, #state>>::Session);
        }
        let ident = self.ident();
        let lives = &self.spec.state_generics.lifetimes;
        let values = &self.spec.state_generics.arguments;
        quote!(#ident<#l_env, #( #lives, )* 'd, #( #values, )*>)
    }

    pub(in crate::app::serve) fn definitions(&self, paths: &Paths) -> proc_macro2::TokenStream {
        let symbols = &self.spec.state_generics;
        let [v_input, v_sess] = symbols.locals(["input", "session"]);
        let l_env = self.spec.state_generics.lifetime("env");
        use quote::quote;
        let ns_core = &paths.core;
        let ns_http = &paths.http;

        match self.set.as_slice() {
            [] | [_] => return proc_macro2::TokenStream::new(),
            [_, _, ..] => {}
        }
        let ident = self.ident();
        let state = self.spec.state.ty();
        let lives = &self.spec.state_generics.lifetimes;
        let decls = &self.spec.state_generics.parameters;
        let values = &self.spec.state_generics.arguments;
        let clauses = &self.spec.state_generics.predicates;
        let bounds = quote! { #( #lives: #l_env, )* 'd: #l_env, #( #clauses, )* };
        let kinds: Vec<_> = self
            .set
            .iter()
            .enumerate()
            .map(|(slot, route)| {
                let kind = Id(slot).variant();
                quote! {
                    #kind(
                        <#route as #ns_core::duplex::Route<#l_env, #state>>::Session,
                        ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
                    ),
                }
            })
            .collect();
        let variants: Vec<_> = (0..self.set.len()).map(|slot| Id(slot).variant()).collect();
        let limits: Vec<_> = self
            .set
            .iter()
            .map(|route| {
                quote! {
                    #route: #ns_core::duplex::Route<#l_env, #state>,
                    <#route as #ns_core::duplex::Route<#l_env, #state>>::Session:
                        #ns_core::duplex::Session<'d>,
                }
            })
            .collect();
        quote! {
            #[doc(hidden)]
            enum #ident<#l_env, #( #lives, )* 'd, #( #decls, )*>
            where
                #bounds
                #( #limits )*
            {
                #( #kinds )*
            }

            impl<#l_env, #( #lives, )* 'd, #( #decls, )*>
                #ns_core::duplex::Session<'d>
                for #ident<#l_env, #( #lives, )* 'd, #( #values, )*>
            where
                #bounds
                #( #limits )*
            {
                #[inline(always)]
                fn receive(
                    &mut self,
                    #v_input: #ns_http::payload::owned::Segments<'d>,
                ) -> #ns_core::duplex::Action<'d> {
                    match self { #( Self::#variants(#v_sess, _) => #v_sess.receive(#v_input), )* }
                }

                #[inline(always)]
                fn finish(&mut self) -> #ns_core::duplex::Action<'d> {
                    match self { #( Self::#variants(#v_sess, _) => #v_sess.finish(), )* }
                }

                #[inline(always)]
                fn activate(&mut self) -> #ns_core::duplex::Action<'d> {
                    match self { #( Self::#variants(#v_sess, _) => #v_sess.activate(), )* }
                }
            }
        }
    }

    pub(in crate::app::serve) fn wrap(
        &self,
        family: Id,
        value: proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        if let [_] = self.set.as_slice() {
            return value;
        }
        let ident = self.ident();
        let kind = family.variant();
        quote::quote!(#ident::#kind(#value, ::core::marker::PhantomData))
    }
}
