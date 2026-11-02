use decode::state;
use handle::decode;

use crate::{
    app::{serve::handle, spec},
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(super) fn generate(
    paths: &Paths,
    ctx: &handle::Context<'_, '_, '_>,
    state: &state::State,
) -> proc_macro2::TokenStream {
    let [f_core, f_ids, f_makers] = ["core", "ids", "task_producers"].map(local);
    let [f_state, f_time] = ["state", "timer"].map(local);
    let symbols = &ctx.plan.spec.state_generics;
    let [n_body, n_insert] = symbols.locals(["__body", "__insert"]);
    let [n_owner, n_parent] = symbols.locals(["__owner", "__parent"]);
    let [n_prep, n_make] = symbols.locals(["__prepared", "__producer"]);
    let [n_req, n_id] = symbols.locals(["__request", "__sark_id"]);
    let [n_target, n_task] = symbols.locals(["__target", "__task"]);
    let [n_taker, v_head, v_this] = symbols.locals(["__task_route", "head", "this"]);
    let g_target = ctx.plan.spec.state_generics.ident("__C");
    use quote::quote;

    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;

    let ident = &ctx.exec.task.fiber;
    let prep = &state.prepared;
    let fibers = ctx.exec.fibers().iter().copied().map(|(task, fiber)| {
        let var = task.prepared();
        let slot = task.request();
        let field = syn::Index::from(fiber.slot);
        let path = fiber.path().route(&ctx.plan.spec.state_generics);
        let kind = task.variant();
        quote! {
            #prep::#var(#slot {
                head: #v_head,
            }) => {
                let mut #n_taker = #path;
                let #n_insert = #ns_fiber::dope_fiber::task::storage::scope::Route::try_admit(
                    &mut #n_taker, #n_owner, #n_parent,
                ).map_err(
                    #ns_core::manifold::task::Error::Admission,
                )?;
                let #n_id = #v_this.#f_ids.try_next().map_err(
                    #ns_core::manifold::task::Error::Sequence,
                )?;
                let #n_req = #v_head.retain().with_id(#n_id).with_body(#n_body);
                let #n_make = #v_this.#f_makers.#field;
                let #n_task = #n_make((
                    #ns_core::manifold::task::request::Input::Buffered(#n_req),
                    *#v_this.#f_state,
                    <#ns_fiber::manifold::runtime::Prepared<
                        #g_target,
                    > as
                        #ns_fiber::manifold::runtime::Runnable
                    >::handle(#v_this.#f_time),
                ));
                let #n_task = #n_insert((#n_task, #n_target));
                ::core::result::Result::Ok(
                    #ident::#kind(#n_task),
                )
            }
        }
    });
    let rejected = ctx
        .plan
        .entries
        .iter()
        .filter(|route| route.kind != spec::Kind::Fiber)
        .map(|route| {
            let var = &route.prepared;
            quote! {
                #prep::#var(_) =>
                    ::core::result::Result::Err(
                        #ns_core::manifold::task::Error::Unsupported,
                    )
            }
        });
    let arms = fibers
        .chain(rejected)
        .collect::<Vec<proc_macro2::TokenStream>>();
    quote! {
        let mut #v_this = self.project().#f_core.project();
        match #n_prep {
            #( #arms, )*
        }
    }
}
