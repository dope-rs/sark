use crate::{
    app::{
        serve::{self, duplex, handle::h1::plan},
        spec,
    },
    runtime::paths::Paths,
};

pub(in crate::app::serve) mod decode;
pub(super) mod dispatch;
mod h1;
mod head;
pub(in crate::app) mod router;
mod task;
mod tasks;

pub(super) struct Context<'view, 'source, 'spec> {
    pub(super) plan: &'source serve::ProtocolPlan<'spec>,
    pub(super) name: syn::Ident,
    pub(super) core_ident: syn::Ident,
    pub(super) exec: &'view serve::Execution<'source, 'spec>,
}

/// H1-only route state. Task-only expansion has no value of this type, making
/// continuation, cache, guard, and duplex planning unreachable there.
pub(in crate::app::serve) struct H1Plan<'source, 'spec> {
    pub(super) cleartext: Option<&'source proc_macro2::TokenStream>,
    pub(super) cache_count: usize,
    pub(super) duplex: &'source duplex::Plan<'spec>,
    pub(super) continuations: plan::Plan<'source, 'spec>,
}

pub(in crate::app::serve) fn h1_payload(
    paths: &Paths,
    tag: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    quote::quote! {
        #ns_fiber::dope::net::link::egress::data::compose::Either<
            #ns_h1::server::payload::Body<'d, #tag>,
            #ns_h1::server::payload::Stream<'d, #tag>,
        >
    }
}

fn emit_guards<M>(
    paths: &Paths,
    spec: &spec::Spec,
    entries: &[serve::RouteSpec<'_, M>],
) -> Vec<proc_macro2::TokenStream> {
    let symbols = &spec.state_generics;
    let [n_head, n_reject] = symbols.locals(["__head", "__reject"]);
    let [n_resp, n_state] = symbols.locals(["__response", "__state"]);
    let [l_driver, l_req] = spec
        .state_generics
        .lives(["__sark_driver", "__sark_request"]);
    let g_r = spec.state_generics.ident("__R");
    use quote::quote;

    let ns_core = &paths.core;
    let ns_http = &paths.http;
    let state_ty = spec.state.ty();
    let lives = &spec.state_generics.lifetimes;
    let decls = &spec.state_generics.parameters;
    let clauses = &spec.state_generics.predicates;
    let generics = match (spec.driver, lives.is_empty() && decls.is_empty()) {
        (spec::Driver::Absent, true) => proc_macro2::TokenStream::new(),
        (spec::Driver::Absent, false) => quote! { < #( #lives, )* #( #decls, )* > },
        (spec::Driver::Present, _) => quote! { < 'd, #( #lives, )* #( #decls, )* > },
    };
    entries
        .iter()
        .map(|item| {
            let slot = &item.head;
            let entry = item.entry;
            let guards = &entry.guards;
            let (head, state) = match entry.guards.is_empty() {
                true => (quote!(_), quote!(_)),
                false => (quote!(#n_head), quote!(#n_state)),
            };
            let checks = entry.guards.iter().map(|guard| {
                quote! {
                    if let ::core::ops::ControlFlow::Break(#n_resp) =
                        <#guard as #ns_core::route::guard::Guard<#state_ty>>::check(
                            #n_head,
                            #n_state,
                        )
                    {
                        let #n_resp =
                            #ns_http::payload::response::dispatch::IntoOutput::into_output(
                                #n_resp,
                            );
                        return ::core::ops::ControlFlow::Break(
                            #ns_core::route::guard::Reject::try_reject(
                                #n_reject,
                                #n_resp,
                            ),
                        );
                    }
                }
            });
            quote! {
                impl #generics #ns_core::route::guard::Slot<#state_ty> for #slot
                where
                    #( #clauses, )*
                    #( #guards: #ns_core::route::guard::Guard<#state_ty>, )*
                {
                    #[inline(always)]
                    fn try_admit<
                        #l_req,
                        #l_driver,
                        #g_r,
                    >(
                        #head: #ns_core::route::guard::Head<#l_req>,
                        #state: &#l_req #state_ty,
                        #n_reject: #g_r,
                    ) -> ::core::ops::ControlFlow<
                        <#g_r as #ns_core::route::guard::Reject<
                            #l_req,
                            #l_driver,
                        >>::Break,
                        #g_r,
                    >
                    where
                        #l_driver: #l_req,
                        #state_ty: #l_req,
                        #g_r: #ns_core::route::guard::Reject<
                            #l_req,
                            #l_driver,
                        >,
                    {
                        #( #checks )*
                        ::core::ops::ControlFlow::Continue(#n_reject)
                    }
                }
            }
        })
        .collect()
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

impl<'source, 'spec> H1Plan<'source, 'spec> {
    pub(in crate::app::serve) fn new(
        plan: &'source serve::ProtocolPlan<'spec>,
        exec: &serve::Execution<'source, 'spec>,
        cleartext: Option<&'source proc_macro2::TokenStream>,
    ) -> Self {
        Self {
            cleartext,
            cache_count: exec.sync_count(),
            duplex: &plan.duplex,
            continuations: plan::Plan::full(plan),
        }
    }
}

pub(super) fn build<'source, 'spec>(
    paths: &Paths,
    plan: &'source serve::ProtocolPlan<'spec>,
    exec: &serve::Execution<'source, 'spec>,
    h1: Option<&H1Plan<'source, 'spec>>,
) -> (
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
    dispatch::Shape,
) {
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
    let neutral = task::Task::new(paths, &context, &decode.prepared.state)
        .build(paths, &context, &dispatch, &head, &decode, &tasks);
    let duplex = plan.duplex.definitions(paths, h1.is_some());
    match h1 {
        Some(h1) => {
            use h1::types::Types;

            let h1 = Types::build(paths, &context, h1, &dispatch);
            (
                quote::quote! {
                    #duplex
                    #neutral
                    #h1
                },
                decode.contract,
                dispatch.shape,
            )
        }
        None => (
            quote::quote!(#duplex #neutral),
            decode.contract,
            dispatch.shape,
        ),
    }
}
