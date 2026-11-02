use syn::{
    GenericArgument, GenericParam, Lifetime, WherePredicate, ext::IdentExt, visit::Visit,
    visit_mut::VisitMut,
};

use crate::{macros::tokens, types::lifetime};

/// Declaration, application, and proof carried by a generated state family.
#[derive(Clone)]
pub(crate) struct Generics {
    pub(crate) lifetimes: Vec<Lifetime>,
    pub(crate) parameters: Vec<GenericParam>,
    pub(crate) arguments: Vec<GenericArgument>,
    pub(crate) predicates: Vec<WherePredicate>,
    references: Vec<syn::Ident>,
}

impl Generics {
    pub(crate) fn new(input: syn::Generics) -> Self {
        let mut output = Self {
            lifetimes: Vec::new(),
            parameters: Vec::new(),
            arguments: Vec::new(),
            references: Vec::new(),
            predicates: input
                .where_clause
                .into_iter()
                .flat_map(|clause| clause.predicates)
                .collect(),
        };
        for param in input.params {
            match param {
                GenericParam::Lifetime(param) => {
                    let lifetime = param.lifetime;
                    if !param.bounds.is_empty() {
                        let bounds = param.bounds;
                        output
                            .predicates
                            .push(syn::parse_quote!(#lifetime: #bounds));
                    }
                    output.lifetimes.push(lifetime);
                }
                GenericParam::Type(mut param) => {
                    let name = &param.ident;
                    if !param.bounds.is_empty() {
                        let bounds = &param.bounds;
                        output.predicates.push(syn::parse_quote!(#name: #bounds));
                    }
                    output.arguments.push(syn::parse_quote!(#name));
                    param.colon_token = None;
                    param.bounds.clear();
                    param.default = None;
                    output.parameters.push(GenericParam::Type(param));
                }
                GenericParam::Const(mut param) => {
                    let name = &param.ident;
                    output.arguments.push(syn::parse_quote!({ #name }));
                    param.default = None;
                    output.parameters.push(GenericParam::Const(param));
                }
            }
        }
        output
    }

    pub(crate) fn include(&mut self, lives: impl IntoIterator<Item = Lifetime>) {
        for lifetime in lives {
            if !self
                .lifetimes
                .iter()
                .any(|known| known.ident.unraw() == lifetime.ident.unraw())
            {
                self.lifetimes.push(lifetime);
            }
        }
    }

    pub(crate) fn lifetime(&self, base: &str) -> Lifetime {
        Lifetime::new(
            &format!("'{}", self.fresh(base)),
            proc_macro2::Span::call_site(),
        )
    }

    pub(crate) fn ident(&self, base: &str) -> syn::Ident {
        syn::Ident::new(&self.fresh(base), proc_macro2::Span::mixed_site())
    }

    pub(crate) fn local(&self, base: &str) -> syn::Ident {
        tokens::local(&self.fresh(base))
    }

    pub(crate) fn lives<const N: usize>(&self, bases: [&str; N]) -> [Lifetime; N] {
        bases.map(|base| self.lifetime(base))
    }

    pub(crate) fn idents<const N: usize>(&self, bases: [&str; N]) -> [syn::Ident; N] {
        bases.map(|base| self.ident(base))
    }

    pub(crate) fn locals<const N: usize>(&self, bases: [&str; N]) -> [syn::Ident; N] {
        bases.map(|base| self.local(base))
    }

    pub(crate) fn reserve(&mut self) -> impl for<'ast> Visit<'ast> + '_ {
        Names(&mut self.references)
    }

    fn fresh(&self, base: &str) -> String {
        let mut name = base.to_owned();
        loop {
            let mut used = Used {
                name: &name,
                found: false,
            };
            for lifetime in &self.lifetimes {
                used.visit_lifetime(lifetime);
            }
            for param in &self.parameters {
                used.visit_generic_param(param);
            }
            for bound in &self.predicates {
                used.visit_where_predicate(bound);
            }
            for name in &self.references {
                used.visit_ident(name);
            }
            if !used.found {
                break;
            }
            name.push('_');
        }
        name
    }

    /// The driver placeholder has the same meaning in state bounds and state types.
    pub(crate) fn bind_driver(input: &mut syn::Generics) -> Option<Lifetime> {
        let mut driver = None;
        lifetime::rewrite(|lifetime| {
            if lifetime.ident == "_" {
                lifetime.ident = quote::format_ident!("d");
                driver = Some(lifetime.clone());
            }
        })
        .visit_generics_mut(input);
        driver
    }
}

struct Used<'a> {
    name: &'a str,
    found: bool,
}

impl<'ast> Visit<'ast> for Used<'_> {
    fn visit_ident(&mut self, ident: &'ast syn::Ident) {
        self.found |= ident.unraw() == self.name;
    }

    fn visit_lifetime(&mut self, lifetime: &'ast Lifetime) {
        self.found |= lifetime.ident.unraw() == self.name;
    }
}

struct Names<'a>(&'a mut Vec<syn::Ident>);

impl<'ast> Visit<'ast> for Names<'_> {
    fn visit_ident(&mut self, ident: &'ast syn::Ident) {
        self.0.push(ident.unraw());
    }
}

fn parameters(generics: &syn::Generics) -> impl Iterator<Item = (syn::Ident, bool)> + '_ {
    generics
        .params
        .iter()
        .filter_map(|parameter| match parameter {
            syn::GenericParam::Type(value) => Some((value.ident.clone(), true)),
            syn::GenericParam::Const(value) => Some((value.ident.clone(), false)),
            syn::GenericParam::Lifetime(_) => None,
        })
}

pub(crate) struct Imports {
    names: crate::types::Generics,
    prefix: String,
    bindings: Vec<(syn::Ident, bool)>,
    paths: std::collections::BTreeMap<String, (syn::Path, syn::Ident)>,
    in_type: bool,
    error: Option<syn::Error>,
}

impl Imports {
    pub(crate) fn new(names: Generics, prefix: String) -> Self {
        Self {
            names,
            prefix,
            bindings: Vec::new(),
            paths: std::collections::BTreeMap::new(),
            in_type: false,
            error: None,
        }
    }

    pub(crate) fn bind(&mut self, generics: &syn::Generics) {
        self.bindings = parameters(generics).collect();
    }

    pub(crate) fn constant(&mut self, expr: &mut syn::Expr) {
        let previous = std::mem::replace(&mut self.in_type, true);
        syn::visit_mut::VisitMut::visit_expr_mut(self, expr);
        self.in_type = previous;
    }

    pub(crate) fn finish(
        self,
    ) -> syn::Result<(proc_macro2::TokenStream, proc_macro2::TokenStream)> {
        if let Some(error) = self.error {
            return Err(error);
        }
        let (paths, aliases): (Vec<_>, Vec<_>) = self
            .paths
            .values()
            .map(|(path, alias)| (path, alias))
            .unzip();
        Ok((
            quote::quote!(#( use #paths as #aliases; )*),
            quote::quote!(use super::{ #( #aliases, )* };),
        ))
    }

    fn path(&mut self, path: &mut syn::Path, qself: &mut Option<syn::QSelf>) {
        use quote::ToTokens;
        let Some(first) = path.segments.first() else {
            return;
        };
        if path.leading_colon.is_some()
            || first.ident == "Self"
            || first.ident == "crate"
            || self.bindings.iter().any(|(name, is_type)| {
                name == &first.ident && (*is_type || path.segments.len() == 1)
            })
            || qself.as_ref().is_some_and(|value| value.position == 0)
        {
            return;
        }
        let count = path
            .segments
            .iter()
            .take(
                qself
                    .as_ref()
                    .map_or(path.segments.len(), |value| value.position),
            )
            .take_while(|segment| segment.ident == "self" || segment.ident == "super")
            .count()
            .max(1);
        let root = syn::Path {
            leading_colon: None,
            segments: std::iter::once(syn::PathSegment {
                ident: first.ident.clone(),
                arguments: syn::PathArguments::None,
            })
            .chain(path.segments.iter().skip(1).take(count - 1).cloned())
            .collect(),
        };
        let arguments = first.arguments.clone();
        let key = root.to_token_stream().to_string();
        let next = self.paths.len();
        let alias = &self
            .paths
            .entry(key)
            .or_insert_with(|| {
                (
                    root,
                    self.names.ident(&format!("{}Path{next}", self.prefix)),
                )
            })
            .1;
        path.segments = std::iter::once(syn::PathSegment {
            ident: alias.clone(),
            arguments,
        })
        .chain(path.segments.iter().skip(count).cloned())
        .collect();
        if let Some(qself) = qself {
            qself.position -= count - 1;
        }
    }
}

impl syn::visit_mut::VisitMut for Imports {
    fn visit_impl_item_fn_mut(&mut self, item: &mut syn::ImplItemFn) {
        let count = self.bindings.len();
        self.bindings.extend(parameters(&item.sig.generics));
        syn::visit_mut::visit_impl_item_fn_mut(self, item);
        self.bindings.truncate(count);
    }

    fn visit_type_mut(&mut self, ty: &mut syn::Type) {
        let previous = std::mem::replace(&mut self.in_type, true);
        syn::visit_mut::visit_type_mut(self, ty);
        self.in_type = previous;
    }

    fn visit_type_path_mut(&mut self, ty: &mut syn::TypePath) {
        syn::visit_mut::visit_type_path_mut(self, ty);
        self.path(&mut ty.path, &mut ty.qself);
    }

    fn visit_trait_bound_mut(&mut self, bound: &mut syn::TraitBound) {
        let previous = std::mem::replace(&mut self.in_type, true);
        syn::visit_mut::visit_trait_bound_mut(self, bound);
        self.in_type = previous;
        self.path(&mut bound.path, &mut None);
    }

    fn visit_expr_path_mut(&mut self, expr: &mut syn::ExprPath) {
        syn::visit_mut::visit_expr_path_mut(self, expr);
        if self.in_type || expr.qself.is_some() || expr.path.segments.len() > 1 {
            self.path(&mut expr.path, &mut expr.qself);
        }
    }

    fn visit_pat_ident_mut(&mut self, pattern: &mut syn::PatIdent) {
        if self.in_type {
            self.error = Some(syn::Error::new_spanned(
                pattern,
                "generated const arguments cannot introduce bindings",
            ));
            return;
        }
        syn::visit_mut::visit_pat_ident_mut(self, pattern);
    }

    fn visit_expr_struct_mut(&mut self, expr: &mut syn::ExprStruct) {
        syn::visit_mut::visit_expr_struct_mut(self, expr);
        self.path(&mut expr.path, &mut expr.qself);
    }

    fn visit_pat_struct_mut(&mut self, pattern: &mut syn::PatStruct) {
        syn::visit_mut::visit_pat_struct_mut(self, pattern);
        self.path(&mut pattern.path, &mut pattern.qself);
    }

    fn visit_pat_tuple_struct_mut(&mut self, pattern: &mut syn::PatTupleStruct) {
        syn::visit_mut::visit_pat_tuple_struct_mut(self, pattern);
        self.path(&mut pattern.path, &mut pattern.qself);
    }

    fn visit_item_mut(&mut self, item: &mut syn::Item) {
        if self.in_type {
            self.error = Some(syn::Error::new_spanned(
                item,
                "generated const arguments cannot declare local items",
            ));
            return;
        }
        syn::visit_mut::visit_item_mut(self, item);
    }

    fn visit_macro_mut(&mut self, mac: &mut syn::Macro) {
        if self.in_type {
            self.error = Some(syn::Error::new_spanned(
                mac,
                "generated types require explicit type and const arguments",
            ));
        }
    }
}
