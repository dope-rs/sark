use handle::{decode::arms, head};

use crate::{
    app::{
        serve::{self, handle},
        spec,
    },
    macros::tokens::local,
    runtime::paths::Paths,
};

mod flow;
mod slots;

pub(in crate::app::serve::handle) struct State {
    pub(in crate::app::serve::handle) prepared: syn::Ident,
    pub(in crate::app::serve::handle) request: syn::Ident,
    pub(in crate::app::serve::handle) guarded: syn::Ident,
    pub(in crate::app::serve::handle) routes: syn::Ident,
    pub(in crate::app::serve::handle) slots: Vec<proc_macro2::TokenStream>,
    pub(in crate::app::serve::handle) definition: proc_macro2::TokenStream,
}

impl State {
    pub(super) fn build<M>(
        paths: &Paths,
        spec: &spec::Spec,
        head: &head::Head,
        entries: &[serve::RouteSpec<'_, M>],
    ) -> Self {
        use quote::{format_ident, quote};

        let ns_core = &paths.core;
        let [v_body, v_slot, v_map, v_ret] = ["body", "slot", "map", "retained"].map(local);
        let [v_output, v_each, v_fold, v_visit] = ["output", "each", "fold", "visit"].map(local);
        let [v_head, v_len] = ["head", "content_length"].map(local);
        let [v_bound, v_limit, v_error, v_state] =
            ["boundary", "limit", "error", "state"].map(local);

        let vis = &spec.vis;
        let selected = &head.names.prepared_ident;
        let prepared = format_ident!("{}Prepared", spec.name);
        let ident = format_ident!("{}Request", spec.name);
        let plain = format_ident!("{}Plain", spec.name);
        let guarded = format_ident!("{}Guarded", spec.name);
        let family = format_ident!("{}RequestFamily", spec.name);
        let plain_f = format_ident!("{}PlainFamily", spec.name);
        let guard_f = format_ident!("{}GuardedFamily", spec.name);
        let structs = slots::define(paths, vis, entries);
        let impls = slots::implement(paths, &prepared, entries);

        let mut defs = Vec::new();
        let mut p_defs = Vec::new();
        let mut g_defs = Vec::new();
        let mut map_arms = Vec::new();
        let mut p_maps = Vec::new();
        let mut g_maps = Vec::new();
        let mut each = Vec::new();
        let mut p_each = Vec::new();
        let mut g_each = Vec::new();
        let mut folds = Vec::new();
        let mut p_folds = Vec::new();
        let mut g_folds = Vec::new();
        let mut kinds = Vec::new();
        let mut finish = Vec::new();
        let mut p_end = Vec::new();
        let mut g_end = Vec::new();
        let mut bodies = Vec::new();
        let mut p_bodies = Vec::new();
        let mut g_bodies = Vec::new();
        let mut c_open = arms::Arms::new();
        let mut p_open = arms::Arms::new();
        let mut g_open = arms::Arms::new();
        let mut admits = Vec::new();
        let mut start = Vec::new();
        let mut r_defs = Vec::new();
        let all_p = entries.iter().all(|item| item.entry.guards.is_empty());
        let all_g = entries.iter().all(|item| !item.entry.guards.is_empty());

        for item in entries {
            let entry = item.entry;
            let variant = &item.prepared;
            let head = &item.head;
            let slot = &item.request;
            let route = &entry.route;
            let open = format_ident!("{variant}Open");
            let finished = format_ident!("{variant}Finished");
            let body = quote! {
                #ns_core::body::admit::Plan::new(
                    <<#route as #ns_core::route::Spec>::Request
                        as #ns_core::route::Schema>::BODY_RETENTION,
                    <#route as #ns_core::route::Spec>::MAX_BODY,
                    #ns_core::body::admit::Length::from_option(#v_len),
                    <<<#route as #ns_core::route::Spec>::Request
                        as #ns_core::route::Own>::Mode as #ns_core::route::Mode>::STREAM,
                )
            };
            let (open_ty, done_ty, open_fn, done_fn, end_fn) = match item.kind {
                spec::Kind::Sync => (
                    quote!(SyncOpen),
                    quote!(SyncFinished),
                    quote!(sync_open),
                    quote!(sync_done),
                    syn::Ident::new("sync", proc_macro2::Span::call_site()),
                ),
                spec::Kind::Fiber => (
                    quote!(TaskOpen),
                    quote!(TaskFinished),
                    quote!(task_open),
                    quote!(task_done),
                    syn::Ident::new("task", proc_macro2::Span::call_site()),
                ),
                spec::Kind::Stream => (
                    quote!(UnsupportedOpen),
                    quote!(UnsupportedFinished),
                    quote!(reject_open),
                    quote!(reject_done),
                    syn::Ident::new("reject", proc_macro2::Span::call_site()),
                ),
                spec::Kind::Duplex => (
                    quote!(DuplexOpen),
                    quote!(DuplexFinished),
                    quote!(duplex_open),
                    quote!(duplex_done),
                    syn::Ident::new("duplex", proc_macro2::Span::call_site()),
                ),
            };
            let variants = quote! {
                #open(#slot, F::#open_ty),
                #finished(#slot, F::#done_ty),
            };
            defs.push(variants.clone());
            r_defs.push(quote! { #variant(#slot), });
            let mapping = |target: &syn::Ident| {
                quote! {
                    Self::#open(#v_slot, #v_body) => match #v_map.#open_fn(#v_slot, #v_body) {
                        ::core::ops::ControlFlow::Continue((#v_ret, #v_slot, #v_body)) =>
                            ::core::ops::ControlFlow::Continue((#v_ret, #target::#open(#v_slot, #v_body))),
                        ::core::ops::ControlFlow::Break(#v_output) =>
                            ::core::ops::ControlFlow::Break(#v_output),
                    },
                    Self::#finished(#v_slot, #v_body) => match #v_map.#done_fn(#v_slot, #v_body) {
                        ::core::ops::ControlFlow::Continue((#v_ret, #v_slot, #v_body)) =>
                            ::core::ops::ControlFlow::Continue((#v_ret, #target::#finished(#v_slot, #v_body))),
                        ::core::ops::ControlFlow::Break(#v_output) =>
                            ::core::ops::ControlFlow::Break(#v_output),
                    },
                }
            };
            map_arms.push(mapping(&ident));
            let e_arms = quote! {
                Self::#open(#v_slot, #v_body) => #v_each.#open_fn(#v_slot, #v_body),
                Self::#finished(#v_slot, #v_body) => #v_each.#done_fn(#v_slot, #v_body),
            };
            each.push(e_arms.clone());
            let f_arms = quote! {
                Self::#open(#v_slot, #v_body) => #v_fold.#open_fn(#v_slot, #v_body),
                Self::#finished(#v_slot, #v_body) => #v_fold.#done_fn(#v_slot, #v_body),
            };
            folds.push(f_arms.clone());
            let b_arms = quote! {
                Self::#open(.., #v_body) => #v_visit.apply(
                    #ns_core::decode::state::body::Value::#open_ty(#v_body),
                ),
                Self::#finished(.., #v_body) => #v_visit.apply(
                    #ns_core::decode::state::body::Value::#done_ty(#v_body),
                ),
            };
            bodies.push(b_arms.clone());
            kinds.push(quote! {
                #ident::#open(#v_slot, #v_body) => #v_fold.open(#v_slot, #v_body),
                #ident::#finished(#v_slot, #v_body) => #v_fold.finished(#v_slot, #v_body),
            });
            let end_arms = quote! {
                Self::#open(#v_slot, #v_body) => ::core::result::Result::Ok(Self::#finished(
                    #v_slot, <F as #ns_core::decode::state::Finish>::#end_fn(#v_body),
                )),
                Self::#finished(#v_slot, #v_body) =>
                    ::core::result::Result::Err(Self::#finished(#v_slot, #v_body)),
            };
            finish.push(end_arms.clone());
            let gated = !entry.guards.is_empty();
            let branch = if gated { &mut g_open } else { &mut p_open };
            match item.kind {
                spec::Kind::Sync | spec::Kind::Fiber => {
                    c_open.push_active(&end_fn, &open, &finished);
                    branch.push_active(&end_fn, &open, &finished);
                }
                spec::Kind::Stream | spec::Kind::Duplex => {
                    c_open.push_never(&open, &finished);
                    branch.push_never(&open, &finished);
                }
            }

            let subset = if entry.guards.is_empty() {
                &plain
            } else {
                &guarded
            };
            let sub_map = mapping(subset);
            if entry.guards.is_empty() {
                p_defs.push(variants);
                p_maps.push(sub_map);
                p_each.push(e_arms);
                p_folds.push(f_arms);
                p_bodies.push(b_arms);
                p_end.push(end_arms);
                admits.push(quote! {
                    Self::#open(#v_slot, #v_body) => match
                        #ns_core::decode::state::admit::Visit::#open_fn(#v_visit, #v_slot, #v_body)
                    {
                        ::core::ops::ControlFlow::Continue((#v_slot, #v_body)) =>
                            ::core::ops::ControlFlow::Continue(#ident::#open(#v_slot, #v_body)),
                        ::core::ops::ControlFlow::Break(#v_output) =>
                            ::core::ops::ControlFlow::Break(#v_output),
                    },
                    Self::#finished(#v_slot, #v_body) => match
                        #ns_core::decode::state::admit::Visit::#done_fn(#v_visit, #v_slot, #v_body)
                    {
                        ::core::ops::ControlFlow::Continue((#v_slot, #v_body)) =>
                            ::core::ops::ControlFlow::Continue(#ident::#finished(#v_slot, #v_body)),
                        ::core::ops::ControlFlow::Break(#v_output) =>
                            ::core::ops::ControlFlow::Break(#v_output),
                    },
                });
            } else {
                g_defs.push(variants);
                g_maps.push(sub_map);
                g_each.push(e_arms);
                g_folds.push(f_arms);
                g_bodies.push(b_arms);
                g_end.push(end_arms);
            }

            let req_slot = match item.kind {
                spec::Kind::Duplex => {
                    quote! { #slot { head: #v_head, content_length: #v_len } }
                }
                spec::Kind::Sync | spec::Kind::Fiber | spec::Kind::Stream => {
                    quote! { #slot { head: #v_head } }
                }
            };
            let method = entry.method.http_token(paths);
            let started = |phase: &syn::Ident, body: proc_macro2::TokenStream| {
                let state = quote! { #subset::#phase(#req_slot, #body) };
                if all_p {
                    quote! { #ns_core::decode::request::Plain::new(#state) }
                } else if all_g {
                    quote! { #ns_core::decode::request::Guarded::new(#state, #method) }
                } else if entry.guards.is_empty() {
                    quote! { #ns_core::decode::request::Start::plain(#state) }
                } else {
                    quote! { #ns_core::decode::request::Start::guarded(#state, #method) }
                }
            };
            let restore = quote! {
                #selected::#variant(#head {
                    head: #v_head, content_length: #v_len,
                })
            };
            match item.kind {
                spec::Kind::Duplex => {
                    let open_v = started(&open, quote!(()));
                    let done_v = started(&finished, quote!(()));
                    start.push(quote! {
                        Self::#variant(#head {
                            head: #v_head, content_length: #v_len,
                        }) => ::core::result::Result::Ok(match #v_bound {
                            #ns_core::manifold::stream::Boundary::End => #done_v,
                            #ns_core::manifold::stream::Boundary::More => #open_v,
                        })
                    });
                }
                spec::Kind::Sync | spec::Kind::Fiber | spec::Kind::Stream => {
                    let open_v = started(&open, quote!(#v_body));
                    let done_v = started(&finished, quote!(#v_body));
                    start.push(quote! {
                        Self::#variant(#head {
                            head: #v_head, content_length: #v_len,
                        }) => match #body.try_start(#v_limit, #v_bound) {
                            ::core::result::Result::Ok(
                                #ns_core::body::admit::Start::Open(#v_body),
                            ) => ::core::result::Result::Ok(#open_v),
                            ::core::result::Result::Ok(
                                #ns_core::body::admit::Start::Finished(#v_body),
                            ) => ::core::result::Result::Ok(#done_v),
                            ::core::result::Result::Err(#v_error) =>
                                ::core::result::Result::Err(
                                    #ns_core::o3::mem::Rejected::new(#restore, #v_error),
                                ),
                        }
                    });
                }
            }
        }

        let start_ty = if all_p {
            quote!(#ns_core::decode::request::Plain<#plain<#ns_core::decode::state::Routed>>)
        } else if all_g {
            quote!(#ns_core::decode::request::Guarded<#guarded<#ns_core::decode::state::Routed>>)
        } else {
            quote! {
                #ns_core::decode::request::Start<
                    #plain<#ns_core::decode::state::Routed>,
                    #guarded<#ns_core::decode::state::Routed>,
                >
            }
        };
        let (exec, uniform) = match spec.route_specs.homogeneity {
            spec::Homogeneity::Uniform(kind) => {
                let kind = match kind {
                    spec::Kind::Sync => quote!(#ns_core::decode::exec::Sync),
                    spec::Kind::Fiber => quote!(#ns_core::decode::exec::Task),
                    spec::Kind::Duplex => quote!(#ns_core::decode::exec::Duplex),
                    spec::Kind::Stream => quote!(#ns_core::decode::exec::Unsupported),
                };
                (
                    quote!(#kind),
                    quote! {
                    impl #ns_core::decode::state::Homogeneous<#prepared, #kind> for #family {
                        fn fold<
                            F: #ns_core::decode::state::Set,
                            T: #ns_core::decode::exec::Fold<#prepared, F, #kind>,
                        >(#v_state: #ident<F>, #v_fold: T) -> T::Output {
                            match #v_state { #( #kinds )* }
                        }
                    }
                    },
                )
            }
            spec::Homogeneity::Mixed => (
                quote!(#ns_core::decode::exec::Mixed),
                proc_macro2::TokenStream::new(),
            ),
        };
        let core = flow::Flow {
            prepared: &prepared,
            state: &ident,
            family: &family,
            maps: &map_arms,
            each: &each,
            folds: &folds,
            bodies: &bodies,
            finish: &finish,
            open: &c_open,
        }
        .render(paths);
        let p_types = (!all_g).then(|| {
            let p_flow = flow::Flow {
                prepared: &prepared,
                state: &plain,
                family: &plain_f,
                maps: &p_maps,
                each: &p_each,
                folds: &p_folds,
                bodies: &p_bodies,
                finish: &p_end,
                open: &p_open,
            }
            .render(paths);
            quote! {
                #[doc(hidden)] #vis struct #plain_f;
                #[doc(hidden)]
                #vis enum #plain<F: #ns_core::decode::state::Set> { #( #p_defs )* }
                impl #ns_core::decode::state::Family<#prepared> for #plain_f {
                    type State<F: #ns_core::decode::state::Set> = #plain<F>;
                }
                #p_flow
                impl<F, G> #ns_core::decode::state::Admission<#prepared, F, G> for #plain<F>
                where
                    F: #ns_core::decode::state::Set,
                    G: #ns_core::decode::state::Set,
                {
                    type Output = #ident<G>;
                    fn admit<V: #ns_core::decode::state::admit::Visit<#prepared, F, G>>(
                        self,
                        #v_visit: &mut V,
                    ) -> ::core::ops::ControlFlow<V::Break, Self::Output> {
                        match self { #( #admits )* }
                    }
                }
            }
        });
        let g_types = (!all_p).then(|| {
            let g_flow = flow::Flow {
                prepared: &prepared,
                state: &guarded,
                family: &guard_f,
                maps: &g_maps,
                each: &g_each,
                folds: &g_folds,
                bodies: &g_bodies,
                finish: &g_end,
                open: &g_open,
            }
            .render(paths);
            quote! {
                #[doc(hidden)] #vis struct #guard_f;
                #[doc(hidden)]
                #vis enum #guarded<F: #ns_core::decode::state::Set> { #( #g_defs )* }
                impl #ns_core::decode::state::Family<#prepared> for #guard_f {
                    type State<F: #ns_core::decode::state::Set> = #guarded<F>;
                }
                #g_flow
            }
        });
        let tokens = quote! {
            #[doc(hidden)]
            #vis enum #prepared { #( #r_defs )* }
            #[doc(hidden)] #vis struct #family;
            #[doc(hidden)]
            #vis enum #ident<F: #ns_core::decode::state::Set> { #( #defs )* }
            #( #impls )*

            impl #ns_core::decode::request::Prepared for #prepared {
                type Execution = #exec;
                type Family = #family;
            }
            impl #ns_core::decode::state::Family<#prepared> for #family {
                type State<F: #ns_core::decode::state::Set> = #ident<F>;
            }
            #uniform
            #core
            #p_types
            #g_types

            impl #ns_core::decode::request::Head for #selected {
                type Prepared = #prepared;
                type Start = #start_ty;

                #[inline(always)]
                fn try_start(
                    self,
                    #v_limit: ::core::primitive::usize,
                    #v_bound: #ns_core::manifold::stream::Boundary,
                ) -> ::core::result::Result<
                    Self::Start,
                    #ns_core::o3::mem::Rejected<Self, #ns_core::body::Error>,
                > {
                    match self {
                        #( #start, )*
                    }
                }
            }
        };
        Self {
            prepared,
            request: ident,
            guarded,
            routes: if all_p { plain_f } else { guard_f },
            slots: structs,
            definition: tokens,
        }
    }
}
