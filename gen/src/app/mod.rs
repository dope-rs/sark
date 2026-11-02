use crate::runtime::paths::Paths;

pub(super) mod inputs;
pub(crate) mod route;
mod scope;
mod serve;
mod spec;

pub(super) struct App {
    spec: spec::Spec,
}

impl App {
    pub(super) fn new(paths: &Paths, input: inputs::Input) -> syn::Result<Self> {
        if input.state_ty.is_none() {
            return Err(syn::Error::new_spanned(
                &input.name,
                "fiber applications require an explicit state type",
            ));
        }
        Self::build(paths, input, spec::StateScope::Driver)
    }

    pub(super) fn new_router(paths: &Paths, input: inputs::Input) -> syn::Result<Self> {
        Self::build(paths, input, spec::StateScope::Router)
    }

    fn build(paths: &Paths, input: inputs::Input, scope: spec::StateScope) -> syn::Result<Self> {
        Ok(Self {
            spec: spec::Spec::new(paths, input, scope)?,
        })
    }

    fn prepare(&mut self, paths: &Paths) -> (scope::Scope, proc_macro2::TokenStream) {
        let scope = scope::Scope::new(paths, &mut self.spec);
        let schema = self
            .spec
            .route_plan
            .check_schema(paths, &self.spec.route_specs);
        (scope, schema)
    }

    pub(super) fn expand(
        mut self,
        paths: &Paths,
        h1: crate::runtime::H1,
    ) -> proc_macro2::TokenStream {
        use quote::quote;

        let (scope, schema) = self.prepare(paths);
        let tokens = serve::build(paths, &self.spec, h1);
        scope.finish(quote! {
            #schema
            #tokens
        })
    }

    pub(super) fn router(
        mut self,
        paths: &Paths,
        h1: crate::runtime::H1,
    ) -> proc_macro2::TokenStream {
        use self::serve::handle::router;

        let (scope, schema) = self.prepare(paths);
        let handle = router::build(paths, &self.spec, h1);
        scope.finish(quote::quote! {
            #schema
            #handle
        })
    }
}
