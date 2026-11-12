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
        scratch: Option<proc_macro2::TokenStream>,
    ) -> proc_macro2::TokenStream {
        let scratch = scratch.map(|scratch| quote::quote!(, #scratch));
        match self {
            Self::Disabled => clock,
            Self::Enabled => quote::quote!((#clock, #budget #scratch)),
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
    set: Vec<(&'spec syn::TypePath, bool)>,
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
        if let Some(slot) = self
            .set
            .iter()
            .position(|(route, _)| *route == &entry.route)
        {
            self.set[slot].1 |= !entry.upgrade;
            return Id(slot);
        }
        let id = Id(self.set.len());
        self.set.push((&entry.route, !entry.upgrade));
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

    fn families(&self, multiplexed: bool) -> impl Iterator<Item = (Id, &'spec syn::TypePath)> {
        self.set
            .iter()
            .enumerate()
            .filter_map(move |(slot, (route, enabled))| {
                (!multiplexed || *enabled).then_some((Id(slot), *route))
            })
    }

    fn ident(&self, multiplexed: bool) -> syn::Ident {
        let suffix = if multiplexed && self.set.iter().any(|(_, enabled)| !enabled) {
            "TaskDuplex"
        } else {
            "Duplex"
        };
        quote::format_ident!("__{}{suffix}", self.spec.name)
    }

    pub(in crate::app::serve) fn ty(
        &self,
        paths: &Paths,
        multiplexed: bool,
    ) -> proc_macro2::TokenStream {
        let l_env = self.spec.state_generics.lifetime("env");
        use quote::quote;
        let ns_core = &paths.core;

        let mut families = self.families(multiplexed);
        match (families.next(), families.next()) {
            (None, _) => return quote!(#ns_core::duplex::Never),
            (Some((_, route)), None) => {
                let state = self.spec.state.ty();
                return quote!(<#route as #ns_core::duplex::Route<#l_env, #state>>::Session);
            }
            _ => {}
        }
        let ident = self.ident(multiplexed);
        let lives = &self.spec.state_generics.lifetimes;
        let values = &self.spec.state_generics.arguments;
        quote!(#ident<#l_env, #( #lives, )* 'd, #( #values, )*>)
    }

    pub(in crate::app::serve) fn definitions(
        &self,
        paths: &Paths,
        h1: bool,
    ) -> proc_macro2::TokenStream {
        let direct = (h1 && self.set.iter().any(|(_, enabled)| !enabled))
            .then(|| self.definition(paths, false));
        let multiplexed = self.definition(paths, true);
        quote::quote!(#direct #multiplexed)
    }

    fn definition(&self, paths: &Paths, multiplexed: bool) -> proc_macro2::TokenStream {
        let symbols = &self.spec.state_generics;
        let [v_input, v_output, v_scratch, v_sess] =
            symbols.locals(["input", "output", "scratch", "session"]);
        let l_env = self.spec.state_generics.lifetime("env");
        use quote::quote;
        let ns_core = &paths.core;
        let ns_http = &paths.http;

        if self.families(multiplexed).nth(1).is_none() {
            return proc_macro2::TokenStream::new();
        }
        let ident = self.ident(multiplexed);
        let state = self.spec.state.ty();
        let lives = &self.spec.state_generics.lifetimes;
        let decls = &self.spec.state_generics.parameters;
        let values = &self.spec.state_generics.arguments;
        let clauses = &self.spec.state_generics.predicates;
        let bounds = quote! { #( #lives: #l_env, )* 'd: #l_env, #( #clauses, )* };
        let kinds: Vec<_> = self
            .families(multiplexed)
            .map(|(family, route)| {
                let kind = family.variant();
                quote! {
                    #kind(
                        <#route as #ns_core::duplex::Route<#l_env, #state>>::Session,
                        ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
                    ),
                }
            })
            .collect();
        let variants: Vec<_> = self
            .families(multiplexed)
            .map(|(family, _)| family.variant())
            .collect();
        let limits: Vec<_> = self
            .families(multiplexed)
            .map(|(_, route)| {
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
                    #v_output: #ns_core::o3::buffer::write::Slice<'_, &mut ::core::primitive::usize>,
                    #v_scratch: &mut ::std::vec::Vec<::core::primitive::u8>,
                ) -> #ns_core::duplex::Action<'d> {
                    match self { #( Self::#variants(#v_sess, _) => #v_sess.receive(#v_input, #v_output, #v_scratch), )* }
                }

                #[inline(always)]
                fn finish(&mut self, #v_output: #ns_core::o3::buffer::write::Slice<'_, &mut ::core::primitive::usize>, #v_scratch: &mut ::std::vec::Vec<::core::primitive::u8>) -> #ns_core::duplex::Action<'d> {
                    match self { #( Self::#variants(#v_sess, _) => #v_sess.finish(#v_output, #v_scratch), )* }
                }

                #[inline(always)]
                fn activate(&mut self, #v_output: #ns_core::o3::buffer::write::Slice<'_, &mut ::core::primitive::usize>, #v_scratch: &mut ::std::vec::Vec<::core::primitive::u8>) -> #ns_core::duplex::Action<'d> {
                    match self { #( Self::#variants(#v_sess, _) => #v_sess.activate(#v_output, #v_scratch), )* }
                }
            }
        }
    }

    pub(in crate::app::serve) fn wrap(
        &self,
        family: Id,
        multiplexed: bool,
        value: proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        if self.families(multiplexed).nth(1).is_none() {
            return value;
        }
        let ident = self.ident(multiplexed);
        let kind = family.variant();
        quote::quote!(#ident::#kind(#value, ::core::marker::PhantomData))
    }
}
