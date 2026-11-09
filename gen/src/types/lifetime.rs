use syn::{
    ext::IdentExt,
    visit::{self, Visit},
    visit_mut::{self, VisitMut},
};

pub(crate) struct Lifetimes<'a> {
    ty: &'a syn::Type,
}

impl<'a> Lifetimes<'a> {
    pub(crate) fn new(ty: &'a syn::Type) -> Self {
        Self { ty }
    }

    pub(crate) fn is_within(&self, allowed: &[syn::Lifetime]) -> bool {
        let mut denied = false;
        self.visit(|lifetime| {
            denied |= lifetime.is_none_or(|lifetime| {
                let name = lifetime.ident.unraw();
                name != "static"
                    && name != "d"
                    && !allowed.iter().any(|allowed| allowed.ident.unraw() == name)
            });
        });
        !denied
    }

    pub(crate) fn response(
        &self,
        request: &syn::Lifetime,
        driver: Option<&syn::Lifetime>,
        state: &[syn::Lifetime],
    ) -> syn::Type {
        self.rewrite(|lifetime| {
            let name = lifetime.ident.unraw();
            if name == "d" {
                *lifetime = driver.unwrap_or(request).clone();
            } else if name != "static" && !state.iter().any(|state| state.ident.unraw() == name) {
                *lifetime = request.clone();
            }
        })
    }

    pub(crate) fn session(&self, source: Option<&syn::Lifetime>, ident: &str) -> syn::Type {
        self.rewrite(|lifetime| {
            let selected =
                source.is_some_and(|source| source.ident.unraw() == lifetime.ident.unraw());
            if lifetime.ident == "_" || selected {
                lifetime.ident = quote::format_ident!("{}", ident);
            }
        })
    }

    pub(crate) fn anonymous(&self, ident: &str) -> syn::Type {
        self.rewrite(|lifetime| {
            if lifetime.ident == "_" {
                lifetime.ident = quote::format_ident!("{}", ident);
            }
        })
    }

    pub(crate) fn distinct(&self, prefix: &str) -> (syn::Type, Vec<syn::Lifetime>) {
        let mut names = super::Generics::new(syn::Generics::default());
        names.reserve().visit_type(self.ty);
        let mut lives = Vec::new();
        let ty = self.rewrite(|lifetime| {
            if lifetime.ident == "_" {
                *lifetime = names.lifetime(&format!("{prefix}{}", lives.len()));
                lives.push(lifetime.clone());
            }
        });
        (ty, lives)
    }

    pub(crate) fn named(&self) -> Vec<syn::Lifetime> {
        let mut named: Vec<syn::Lifetime> = Vec::new();
        self.visit(|lifetime| {
            let Some(lifetime) = lifetime else { return };
            let name = lifetime.ident.unraw();
            if name != "_"
                && name != "static"
                && !named.iter().any(|known| known.ident.unraw() == name)
            {
                named.push(lifetime.clone());
            }
        });
        named
    }

    /// Counts explicit `'_` lifetime slots for the expansion to bind.
    pub(crate) fn placeholders(&self) -> usize {
        let mut count = 0;
        self.visit(|lifetime| {
            count += usize::from(lifetime.is_some_and(|lifetime| lifetime.ident == "_"));
        });
        count
    }

    fn visit(&self, visit: impl FnMut(Option<&syn::Lifetime>)) {
        Free::new(visit).visit_type(self.ty);
    }

    fn rewrite(&self, apply: impl FnMut(&mut syn::Lifetime)) -> syn::Type {
        let mut ty = self.ty.clone();
        rewrite(apply).visit_type_mut(&mut ty);
        ty
    }
}

pub(super) fn rewrite(apply: impl FnMut(&mut syn::Lifetime)) -> impl VisitMut {
    Free::new(apply)
}

/// Visits only free lifetimes; function signatures own their elided lifetimes.
struct Free<F> {
    apply: F,
    bound: Vec<syn::Ident>,
    elided: bool,
}

impl<F> Free<F> {
    fn new(apply: F) -> Self {
        Self {
            apply,
            bound: Vec::new(),
            elided: false,
        }
    }

    fn enter(&mut self, lives: Option<&syn::BoundLifetimes>, elided: bool) -> (usize, bool) {
        let previous = (self.bound.len(), self.elided);
        if let Some(lives) = lives {
            self.bound
                .extend(lives.lifetimes.iter().filter_map(|param| {
                    let syn::GenericParam::Lifetime(param) = param else {
                        return None;
                    };
                    Some(param.lifetime.ident.unraw())
                }));
        }
        self.elided |= elided;
        previous
    }

    fn leave(&mut self, previous: (usize, bool)) {
        self.bound.truncate(previous.0);
        self.elided = previous.1;
    }

    fn is_free(&self, lifetime: &syn::Lifetime) -> bool {
        let name = lifetime.ident.unraw();
        !(name == "_" && self.elided) && !self.bound.contains(&name)
    }
}

impl<'ast, F> Visit<'ast> for Free<F>
where
    F: FnMut(Option<&'ast syn::Lifetime>),
{
    fn visit_lifetime(&mut self, lifetime: &'ast syn::Lifetime) {
        if self.is_free(lifetime) {
            (self.apply)(Some(lifetime));
        }
    }

    fn visit_type_reference(&mut self, refer: &'ast syn::TypeReference) {
        if refer.lifetime.is_none() && !self.elided {
            (self.apply)(None);
        }
        visit::visit_type_reference(self, refer);
    }

    fn visit_type_fn_ptr(&mut self, ty: &'ast syn::TypeFnPtr) {
        let previous = self.enter(ty.lifetimes.as_ref(), true);
        for arg in &ty.inputs {
            self.visit_named_arg(arg);
        }
        self.visit_return_type(&ty.output);
        self.leave(previous);
    }

    fn visit_trait_bound(&mut self, bound: &'ast syn::TraitBound) {
        let previous = self.enter(bound.lifetimes.as_ref(), false);
        self.visit_path(&bound.path);
        self.leave(previous);
    }

    fn visit_parenthesized_generic_arguments(
        &mut self,
        args: &'ast syn::ParenthesizedGenericArguments,
    ) {
        let previous = self.enter(None, true);
        visit::visit_parenthesized_generic_arguments(self, args);
        self.leave(previous);
    }
}

impl<F> VisitMut for Free<F>
where
    F: FnMut(&mut syn::Lifetime),
{
    fn visit_lifetime_mut(&mut self, lifetime: &mut syn::Lifetime) {
        if self.is_free(lifetime) {
            (self.apply)(lifetime);
        }
    }

    fn visit_type_fn_ptr_mut(&mut self, ty: &mut syn::TypeFnPtr) {
        let previous = self.enter(ty.lifetimes.as_ref(), true);
        for arg in &mut ty.inputs {
            self.visit_named_arg_mut(arg);
        }
        self.visit_return_type_mut(&mut ty.output);
        self.leave(previous);
    }

    fn visit_trait_bound_mut(&mut self, bound: &mut syn::TraitBound) {
        let previous = self.enter(bound.lifetimes.as_ref(), false);
        self.visit_path_mut(&mut bound.path);
        self.leave(previous);
    }

    fn visit_predicate_type_mut(&mut self, bound: &mut syn::PredicateType) {
        let previous = self.enter(bound.lifetimes.as_ref(), false);
        self.visit_type_mut(&mut bound.bounded_ty);
        for item in &mut bound.bounds {
            self.visit_type_param_bound_mut(item);
        }
        self.leave(previous);
    }

    fn visit_parenthesized_generic_arguments_mut(
        &mut self,
        args: &mut syn::ParenthesizedGenericArguments,
    ) {
        let previous = self.enter(None, true);
        visit_mut::visit_parenthesized_generic_arguments_mut(self, args);
        self.leave(previous);
    }
}
