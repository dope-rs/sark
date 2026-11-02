use handle::{decode, dispatch};

use crate::{app::serve::handle, macros::tokens::local, runtime::paths::Paths};

pub(super) fn build(
    paths: &Paths,
    ctx: &handle::Context<'_, '_, '_>,
    disp: &dispatch::Dispatch,
    decode: &decode::Decode,
) -> proc_macro2::TokenStream {
    let f_core = local("core");
    let symbols = &ctx.plan.spec.state_generics;
    let [v_body, v_encode, v_error] = symbols.locals(["body", "encoder", "error"]);
    let [v_head, v_prep] = symbols.locals(["head", "prepared"]);
    let [v_proj, v_req, v_source] = symbols.locals(["project", "request", "source"]);
    let [v_value, v_visit] = symbols.locals(["value", "visit"]);
    let [l_driver, l_req, l_out] =
        ctx.plan
            .spec
            .state_generics
            .lives(["__sark_driver", "__sark_request", "o"]);
    let [g_e, g_f, g_g, g_i, g_t, g_v, g_store] = ctx
        .plan
        .spec
        .state_generics
        .idents(["__E", "__F", "__G", "__I", "__T", "__V", "__P"]);
    use quote::quote;

    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let ns_http = &paths.http;

    let name = &ctx.name;
    let core = &ctx.core_ident;
    let bounds = &disp.shape.base_bounds;
    let def = &disp.shape.generic_def;
    let args = &disp.shape.generic_use;
    let req = &decode.prepared.state.request;
    let policy = (!ctx.exec.fibers().is_empty()).then(|| {
        quote! {
            #g_store: #ns_fiber::manifold::runtime::Runnable,
        }
    });
    let guarded = decode.guard.is_some().then(|| {
        quote! {
            impl #def #ns_core::decode::Guarded for #name #args
            where
                #policy
                #bounds
            {
                fn try_admit<
                    #l_req,
                    #l_driver: #l_req,
                    #g_e: #ns_core::decode::Encode<
                        #l_req,
                        #l_driver,
                    >,
                    #g_f: #ns_core::decode::state::Set,
                    #g_g: #ns_core::decode::state::Set,
                    #g_v: #ns_core::decode::state::admit::TryVisit<
                        Self::Prepared,
                        #g_f,
                        #g_g,
                        <#g_e as #ns_core::decode::Encode<
                            #l_req, #l_driver,
                        >>::Error,
                    >,
                >(
                    &self,
                    #v_req: #ns_core::decode::request::Guarded<
                        <Self::Routes as #ns_core::decode::state::Family<
                            Self::Prepared,
                        >>::State<#g_f>,
                    >,
                    #v_encode: #g_e,
                    #v_visit: &mut #g_v,
                ) -> ::core::ops::ControlFlow<
                    #g_v::Break<#g_e::Prepared, #g_e::Target>,
                    #req<#g_g>,
                > {
                    #ns_core::decode::Guarded::try_admit(
                        &self.#f_core,
                        #v_req,
                        #v_encode,
                        #v_visit,
                    )
                }
            }
        }
    });
    quote! {
            impl #def #ns_core::decode::Decode for #name #args
            where
                #policy
                #bounds
            {
                type Head = <#core #args as #ns_core::decode::Decode>::Head;
                type Prepared = <#core #args as #ns_core::decode::Decode>::Prepared;
                type Routes = <#core #args as #ns_core::decode::Decode>::Routes;
                type Plan = <#core #args as #ns_core::decode::Decode>::Plan;
                type Cause = <#core #args as #ns_core::decode::Decode>::Cause;
                type Error<#l_req> = <#core #args as #ns_core::decode::Decode>::Error<#l_req>;
                type Projected<#l_req, #l_driver> =
                    <#core #args as #ns_core::decode::Decode>::Projected<#l_req, #l_driver>
                where
                    #l_driver: #l_req;
                type Egress<#l_out> = <#core #args as #ns_core::decode::Decode>::Egress<#l_out>;

                fn reject<#l_req>(#v_error: Self::Error<#l_req>) -> #ns_core::route::Reject {
                    <#core #args as #ns_core::decode::Decode>::reject(#v_error)
                }

                fn try_full(
                    &self,
                    #v_head: #ns_http::field::metadata::section::state::RetainedHead<
                        #ns_http::field::metadata::packed::Decoded,
                        #ns_http::field::metadata::section::state::Selected<
                            #ns_http::field::metadata::section::rules::RetainRaw,
                        >,
                    >,
                ) -> ::core::result::Result<
                    Self::Head,
                    #ns_core::decode::HeadError<Self::Cause>,
                > {
                    #ns_core::decode::Decode::try_full(&self.#f_core, #v_head)
                }

                fn try_head(
                    &self,
                    #v_head: #ns_http::field::metadata::section::state::RetainedHead<
                        <Self::Plan as #ns_http::field::metadata::section::Policy>::Block,
                        #ns_http::field::metadata::section::state::Selected<Self::Plan>,
                    >,
                ) -> ::core::result::Result<
                    Self::Head,
                    #ns_core::decode::HeadError<Self::Cause>,
                > {
                    #ns_core::decode::Decode::try_head(&self.#f_core, #v_head)
                }

                fn try_dispatch<
                    #l_req,
                    #l_driver,
                    #g_e: #ns_core::decode::Encode<
                        #l_req,
                        #l_driver,
                    >,
                >(
                    &self,
                    #v_req: Self::Projected<#l_req, #l_driver>,
                    #v_encode: #g_e,
                ) -> ::core::result::Result<
                    #g_e::Prepared,
                    (
                        #ns_core::decode::dispatch::Error<
                            <#g_e as #ns_core::decode::Encode<
                                #l_req, #l_driver,
                            >>::Error,
                            ::core::convert::Infallible,
                            Self::Error<#l_req>,
                        >,
                        #g_e::Target,
                    ),
                > {
                    #ns_core::decode::Decode::try_dispatch(
                        &self.#f_core,
                        #v_req,
                        #v_encode,
                    )
                }

                fn try_project<
                    #l_driver,
                    #g_i: #ns_http::owner::handoff::Project<#l_driver>,
                    #g_t,
                    #g_f: for<#l_req> ::core::ops::FnOnce(
                        #ns_core::decode::Scoped<
                            #l_req,
                            #l_driver,
                            Self,
                            #g_i::Proof,
                        >,
                    ) -> #g_t,
                >(
                    &self,
                    #v_prep: Self::Prepared,
                    #v_body: #ns_core::body::Input<#g_i>,
                    #v_proj: #g_f,
                ) -> #ns_core::body::Projection<
                    #g_t,
                    #g_i::Owner,
                    (Self::Prepared, #ns_core::body::Input<#g_i>),
                >
                {
                    #ns_core::decode::Decode::try_project(
                        &self.#f_core,
                        #v_prep,
                        #v_body,
                        |#v_req| {
                            let (#v_value, #v_source) = #v_req.into_parts();
                            #v_proj(#ns_core::decode::Scoped::new(#v_value, #v_source))
                        },
                    )
                }
            }

            #guarded
    }
}
