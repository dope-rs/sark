use crate::{
    app::serve::{self, handle},
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
    let identity = &context.exec.identity;
    let variant = task.variant();
    let route = task.route();
    let [n_admit, n_close] = generics.locals(["__admission", "__close"]);
    let [n_used, n_input] = generics.locals(["__consumed", "__input"]);
    let [n_resp, n_run] = generics.locals(["__response", "__runtime"]);
    let [n_source, n_task] = generics.locals(["__source", "__task"]);
    let [n_tasks] = generics.locals(["__tasks"]);
    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    use quote::quote;

    let stored = if task.is_stream() {
        task.make(paths, &n_task)
    } else {
        quote!(#ns_fiber::dope_fiber::task::storage::fixed::staged::Task::new(#n_task))
    };
    quote! {{
        let #n_source = #n_tasks.as_mut();
        move || {
            #ns_fiber::dope_fiber::task::storage::fixed::direct::Source::admit(#n_source).map(
                |#n_admit| {
                    move |
                        #n_input: #ns_core::manifold::task::request::Input<'d, #route>,
                        #n_used,
                        #n_close,
                        #n_resp: #ns_h1::wire::response::Context,
                        #n_run: &mut #runtime,
                    | {
                        let #n_task = (#make)((#n_input, #state, #timer));
                        let #n_task = #ns_fiber::dope_fiber::task::storage::fixed::direct::Insert::insert(
                            #n_admit,
                            #stored,
                        );
                        #n_run.insert(
                            #ns_h1::server::task::runtime::Running::fiber(
                                #identity::#variant(#n_task),
                                #n_resp,
                            ),
                        );
                        #ns_h1::session::Outcome::Park {
                            consumed: #n_used,
                            close: #n_close,
                        }
                    }
                },
            )
        }
    }}
}

pub(in crate::app::serve::handle) fn stream(
    paths: &Paths,
    generics: &crate::types::Generics,
    identity: &syn::Ident,
    runtime: &proc_macro2::TokenStream,
    variant: &syn::Ident,
) -> proc_macro2::TokenStream {
    let [n_admit, n_resp] = generics.locals(["__admission", "__response"]);
    let [n_run, n_source] = generics.locals(["__runtime", "__source"]);
    let [n_stream, n_task] = generics.locals(["__stream", "__task"]);
    let [n_tasks] = generics.locals(["__tasks"]);
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    use quote::quote;

    quote! {{
        let #n_source = #n_tasks.as_mut();
        move || {
            #ns_fiber::dope_fiber::task::storage::fixed::direct::Source::admit(#n_source).map(
                |#n_admit| {
                    move |
                        #n_stream,
                        #n_run: &mut #runtime,
                        #n_resp: #ns_h1::wire::response::Context,
                    | {
                        let #n_task = #ns_fiber::dope_fiber::task::storage::fixed::direct::Insert::insert(
                            #n_admit,
                            #n_stream,
                        );
                        #n_run.insert(
                            #ns_h1::server::task::runtime::Running::stream(
                                #identity::#variant(#n_task),
                                #n_resp,
                            ),
                        );
                    }
                },
            )
        }
    }}
}
