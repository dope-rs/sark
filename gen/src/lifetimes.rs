use quote::format_ident;
use syn::visit::Visit;
use syn::visit_mut::VisitMut;

pub(super) struct TypeLifetimes<'a> {
    ty: &'a syn::Type,
}

impl<'a> TypeLifetimes<'a> {
    pub(super) fn new(ty: &'a syn::Type) -> Self {
        Self { ty }
    }

    pub(super) fn any(&self) -> bool {
        let mut detector = AnyLifetime(false);
        detector.visit_type(self.ty);
        detector.0
    }

    pub(super) fn has_non_static(&self) -> bool {
        let mut detector = NonStaticLifetime(false);
        detector.visit_type(self.ty);
        detector.0
    }

    pub(super) fn normalized_to(&self, ident: &str) -> syn::Type {
        let mut ty = self.ty.clone();
        LifetimeRenamer(ident).visit_type_mut(&mut ty);
        ty
    }
}

struct AnyLifetime(bool);

impl<'ast> Visit<'ast> for AnyLifetime {
    fn visit_lifetime(&mut self, _lifetime: &'ast syn::Lifetime) {
        self.0 = true;
    }
}

struct NonStaticLifetime(bool);

impl<'ast> Visit<'ast> for NonStaticLifetime {
    fn visit_lifetime(&mut self, lifetime: &'ast syn::Lifetime) {
        self.0 |= lifetime.ident != "static";
    }

    fn visit_type_reference(&mut self, reference: &'ast syn::TypeReference) {
        self.0 |= reference
            .lifetime
            .as_ref()
            .is_none_or(|lifetime| lifetime.ident != "static");
        syn::visit::visit_type_reference(self, reference);
    }
}

struct LifetimeRenamer<'a>(&'a str);

impl VisitMut for LifetimeRenamer<'_> {
    fn visit_lifetime_mut(&mut self, lifetime: &mut syn::Lifetime) {
        lifetime.ident = format_ident!("{}", self.0);
    }
}
