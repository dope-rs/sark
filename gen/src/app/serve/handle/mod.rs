use crate::{
    app::serve::{self, app},
    runtime::paths::Paths,
};

pub(in crate::app::serve) mod decode;
pub(super) mod dispatch;
mod h1;
mod head;
pub(in crate::app) mod router;
mod task;
mod tasks;

pub(super) use h1::plan::H1Plan;

pub(super) struct Context<'view, 'source, 'spec> {
    pub(super) plan: &'source serve::ProtocolPlan<'spec>,
    pub(super) name: syn::Ident,
    pub(super) core_ident: syn::Ident,
    pub(super) exec: &'view serve::Execution<'source, 'spec>,
}

fn guard_rejection(
    paths: &Paths,
    symbols: &crate::types::Generics,
    guards: &[syn::TypePath],
    state: &syn::Type,
    app: &impl quote::ToTokens,
    sink: proc_macro2::TokenStream,
    pass: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let core = &paths.core;
    let reject = symbols.lifetime("__guard_rejected");
    let [head, state_ref, response] =
        symbols.locals(["__guard_head", "__guard_state", "__guard_response"]);
    quote::quote! { #reject: {
        let #state_ref = #app;
        #(if let ::core::ops::ControlFlow::Break(#response) =
            <#guards as #core::route::guard::Guard<#state>>::check(#head, #state_ref)
        {
            break #reject #core::route::guard::Reject::try_reject(#sink, #response);
        })*
        #pass
    } }
}

impl<'view, 'source, 'spec> Context<'view, 'source, 'spec> {
    fn new(
        plan: &'source serve::ProtocolPlan<'spec>,
        exec: &'view serve::Execution<'source, 'spec>,
    ) -> Self {
        let name = quote::format_ident!("{}Inner", plan.spec.name);
        let core = quote::format_ident!("{}Core", name);
        Self {
            plan,
            name,
            core_ident: core,
            exec,
        }
    }
}

pub(super) fn build<'source, 'spec>(
    paths: &Paths,
    plan: &'source serve::ProtocolPlan<'spec>,
    exec: &serve::Execution<'source, 'spec>,
    h1: Option<&H1Plan<'source, 'spec>>,
) -> proc_macro2::TokenStream {
    let context = Context::new(plan, exec);
    let dispatch = dispatch::Dispatch::build(paths, &context);
    let head = head::Head::build(
        paths,
        context.plan.spec,
        &dispatch.methods,
        &context.plan.entries,
        context.plan.duplex.connect(),
    );
    let core = crate::macros::tokens::local("core");
    let decode = decode::Decode::build(
        paths,
        context.plan.spec,
        &dispatch.methods,
        &head,
        &context.plan.entries,
        quote::quote!(self.#core),
    );
    let tasks = tasks::Tasks::build(paths, &context);
    let neutral = task::build(paths, &context, &dispatch, &decode, &tasks);
    let (app, direct) = app::build(paths, plan, exec, h1, &decode.contract, &dispatch.shape);
    let duplex = plan.duplex.definitions(paths, h1.is_some());
    let h1 = h1.map(|plan| h1::types::Types::build(paths, &context, plan, &dispatch, &direct));
    quote::quote! {
        #app
        #duplex
        #neutral
        #h1
    }
}
