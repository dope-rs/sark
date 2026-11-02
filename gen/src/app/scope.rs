use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{visit::Visit, visit_mut::VisitMut};

use crate::{app::spec, runtime::paths::Paths, types};

/// Imports caller paths without abstracting their concrete types or bounds.
pub(super) struct Scope {
    module: syn::Ident,
    visibility: syn::Visibility,
    imports: types::Imports,
}

pub(in crate::app) fn module(name: &syn::Ident) -> syn::Ident {
    let name = format_ident!("{}Policy", name).to_string();
    let encoded = name
        .bytes()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format_ident!("__sark_head_{encoded}")
}

impl Scope {
    pub(super) fn new(paths: &Paths, spec: &mut spec::Spec) -> Self {
        let module = module(&spec.name);
        let visibility = std::mem::replace(&mut spec.vis, syn::parse_quote!(pub));
        spec.state_generics.reserve().visit_ident(&spec.name);
        let mut imports = types::Imports::new(
            spec.state_generics.clone(),
            crate::handler::names::encoded("__SarkApp", &spec.name).to_string(),
        );
        let generics = syn::Generics {
            params: spec.state_generics.parameters.iter().cloned().collect(),
            ..syn::Generics::default()
        };
        imports.bind(&generics);
        match &mut spec.state {
            spec::State::Bound(state) | spec::State::Empty(state) => {
                imports.visit_type_mut(state);
            }
        }
        for parameter in &mut spec.state_generics.parameters {
            imports.visit_generic_param_mut(parameter);
        }
        for predicate in &mut spec.state_generics.predicates {
            imports.visit_where_predicate_mut(predicate);
        }
        for entry in spec.route_specs.iter_mut() {
            imports.visit_type_path_mut(&mut entry.route);
            for guard in &mut entry.guards {
                imports.visit_type_path_mut(guard);
            }
            match &mut entry.mode {
                spec::Mode::Fiber { capacity, .. } | spec::Mode::Stream { capacity } => {
                    imports.constant(capacity);
                }
                spec::Mode::Sync | spec::Mode::Duplex => {}
            }
        }
        spec.bounds(paths);
        Self {
            module,
            visibility,
            imports,
        }
    }

    pub(super) fn finish(self, tokens: TokenStream) -> TokenStream {
        let Self {
            module,
            visibility,
            imports,
        } = self;
        let (outer, inner) = match imports.finish() {
            Ok(imports) => imports,
            Err(error) => return error.into_compile_error(),
        };
        let items = match syn::parse2::<syn::File>(tokens.clone()) {
            Ok(file) => file.items,
            Err(error) => return error.into_compile_error(),
        };
        let exports = items.iter().filter_map(|item| {
            let (vis, ident) = match item {
                syn::Item::Enum(item) => (&item.vis, &item.ident),
                syn::Item::Struct(item) => (&item.vis, &item.ident),
                syn::Item::Type(item) => (&item.vis, &item.ident),
                _ => return None,
            };
            matches!(vis, syn::Visibility::Public(_)).then_some(ident)
        });
        quote! {
            #outer
            #(#visibility use #module::#exports;)*
            mod #module {
                #inner
                #tokens
            }
        }
    }
}
