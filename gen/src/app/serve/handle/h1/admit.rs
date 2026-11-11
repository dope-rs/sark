use crate::{
    app::serve::{self, handle},
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(in crate::app::serve::handle) fn fiber(
    paths: &Paths,
    context: &handle::Context<'_, '_, '_>,
    task: serve::Task<'_, '_>,
    runtime: &proc_macro2::TokenStream,
    state: proc_macro2::TokenStream,
    timer: proc_macro2::TokenStream,
    make: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let generics = &context.plan.spec.state_generics;
    let flat = serve::flat::name(context.plan.spec, false);
    let variant = task.phase("Future");
    let route = task.route();
    let [n_admit, n_close] = generics.locals(["__admission", "__close"]);
    let [n_used, n_input] = generics.locals(["__consumed", "__input"]);
    let [n_resp, n_run] = generics.locals(["__response", "__runtime"]);
    let [n_source, n_task] = generics.locals(["__source", "__task"]);
    let [n_tasks] = generics.locals(["__tasks"]);
    let ns_core = &paths.core;
    let ns_h1 = &paths.h1;
    let ns_http = &paths.http;
    use quote::quote;

    let [v_this, n_quota] = generics.locals(["this", "__quota"]);
    let f_quotas = local("quotas");
    let quota = task
        .quota()
        .map(|index| quote!(let #n_quota = &mut #v_this.#f_quotas[#index];));
    let check = task.quota().map(|_| quote! {
        if *#n_quota == 0 {
            return #ns_h1::session::Outcome::Close(#ns_http::h1::codec::canned::SERVICE_UNAVAILABLE);
        }
    });
    let debit = task.quota().map(|_| quote!(*#n_quota -= 1;));
    quote! {{
        let #n_source = &mut #n_tasks;
        #quota
        move |
            #n_input: #ns_core::manifold::task::request::Input<'d, #route>,
            #n_used,
            #n_close,
            #n_resp: #ns_h1::wire::response::Context,
            #n_run: &mut #runtime,
        | {
            #check
            let ::core::option::Option::Some(#n_admit) = #n_source.admit()
            else {
                return #ns_h1::session::Outcome::Close(
                    #ns_http::h1::codec::canned::SERVICE_UNAVAILABLE,
                );
            };
            let #n_task = (#make)((#n_input, #state, #timer));
            let #n_task = #n_admit.insert(#flat::#variant(#n_task));
            #debit
            #n_run.insert(#ns_h1::server::task::runtime::Running::Future(#n_resp, #n_task));
            #ns_h1::session::Outcome::Park {
                consumed: #n_used,
                close: #n_close,
            }
        }
    }}
}

pub(in crate::app::serve::handle) fn stream(
    paths: &Paths,
    context: &handle::Context<'_, '_, '_>,
    task: serve::Task<'_, '_>,
    runtime: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let generics = &context.plan.spec.state_generics;
    let flat = serve::flat::name(context.plan.spec, false);
    let variant = task.phase("Body");
    let n_admit = generics.local("__admission");
    let [n_run, n_source] = generics.locals(["__runtime", "__source"]);
    let [n_stream, n_task] = generics.locals(["__stream", "__task"]);
    let [n_tasks] = generics.locals(["__tasks"]);
    let ns_h1 = &paths.h1;
    use quote::quote;

    let [v_this, n_quota] = generics.locals(["this", "__quota"]);
    let f_quotas = local("quotas");
    let quota = task
        .quota()
        .map(|index| quote!(let #n_quota = &mut #v_this.#f_quotas[#index];));
    let check = task
        .quota()
        .map(|_| quote!(if *#n_quota == 0 { return None; }));
    let debit = task.quota().map(|_| quote!(*#n_quota -= 1;));
    quote! {{
        let #n_source = &mut #n_tasks;
        #quota
        move || {
            let #n_source = #n_source;
            #check
            #n_source.admit().map(|#n_admit| {
                move |#n_stream, #n_run: &mut #runtime, _: #ns_h1::wire::response::Context| {
                    let #n_task = #n_admit.insert(#flat::#variant(#n_stream));
                    #debit
                    #n_run.insert(#ns_h1::server::task::runtime::Running::Body(#n_task));
                }
            })
        }
    }}
}
