use handle::head;
use proc_macro2::TokenStream;

use crate::{
    app::{
        serve::{self, handle},
        spec,
    },
    macros::tokens::local,
    runtime::paths::Paths,
};

mod flow;

pub(in crate::app::serve::handle) struct State {
    pub(in crate::app::serve::handle) prepared: syn::Ident,
    pub(in crate::app::serve::handle) request: syn::Ident,
    pub(in crate::app::serve::handle) guarded: syn::Ident,
    pub(in crate::app::serve::handle) routes: syn::Ident,
    pub(in crate::app::serve::handle) definition: TokenStream,
}

struct Case {
    guarded: bool,
    kind: spec::Kind,
    open: syn::Ident,
    finished: syn::Ident,
    open_fn: syn::Ident,
    done_fn: syn::Ident,
    end_fn: syn::Ident,
    variants: TokenStream,
    each: TokenStream,
    fold: TokenStream,
    body: TokenStream,
    finish: TokenStream,
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
        let [v_body, v_slot] = ["body", "slot"].map(local);
        let [v_each, v_fold, v_visit] = ["each", "fold", "visit"].map(local);
        let [v_head, v_len] = ["head", "content_length"].map(local);
        let [v_bound, v_limit, v_error, v_state] =
            ["boundary", "limit", "error", "state"].map(local);

        let vis = &spec.vis;
        let selected = &head.names.prepared_ident;
        let prepared = format_ident!("{}Prepared", spec.name);
        let ident = format_ident!("{}Request", spec.name);
        let family = format_ident!("{}RequestFamily", spec.name);
        let all_p = entries.iter().all(|item| item.entry.guards.is_empty());
        let all_g = entries.iter().all(|item| !item.entry.guards.is_empty());
        let mixed = !all_p && !all_g;
        let (plain, guarded, routes) = if mixed {
            (
                format_ident!("{}Plain", spec.name),
                format_ident!("{}Guarded", spec.name),
                format_ident!("{}GuardedFamily", spec.name),
            )
        } else {
            (ident.clone(), ident.clone(), family.clone())
        };
        let mut cases = Vec::with_capacity(entries.len());
        let mut admits = Vec::new();
        let mut start = Vec::with_capacity(entries.len());

        for item in entries {
            let entry = item.entry;
            let variant = &item.prepared;
            let slot = &item.request;
            let route = &entry.route;
            let open = format_ident!("{variant}Open");
            let finished = format_ident!("{variant}Finished");
            let [open_ty, done_ty, open_fn, done_fn, end_fn] = match item.kind {
                spec::Kind::Sync => ["SyncOpen", "SyncFinished", "sync_open", "sync_done", "sync"],
                spec::Kind::Fiber => ["TaskOpen", "TaskFinished", "task_open", "task_done", "task"],
                spec::Kind::Stream => [
                    "UnsupportedOpen",
                    "UnsupportedFinished",
                    "reject_open",
                    "reject_done",
                    "reject",
                ],
                spec::Kind::Duplex => [
                    "DuplexOpen",
                    "DuplexFinished",
                    "duplex_open",
                    "duplex_done",
                    "duplex",
                ],
            }
            .map(|name| syn::Ident::new(name, proc_macro2::Span::call_site()));
            let variants = quote! {
                #open(#slot, F::#open_ty),
                #finished(#slot, F::#done_ty),
            };
            let each = quote! {
                Self::#open(#v_slot, #v_body) => #v_each.#open_fn(#v_slot, #v_body),
                Self::#finished(#v_slot, #v_body) => #v_each.#done_fn(#v_slot, #v_body),
            };
            let fold = quote! {
                Self::#open(#v_slot, #v_body) => #v_fold.#open_fn(#v_slot, #v_body),
                Self::#finished(#v_slot, #v_body) => #v_fold.#done_fn(#v_slot, #v_body),
            };
            let body = quote! {
                Self::#open(.., #v_body) => #v_visit.apply(
                    #ns_core::decode::state::body::Value::#open_ty(#v_body),
                ),
                Self::#finished(.., #v_body) => #v_visit.apply(
                    #ns_core::decode::state::body::Value::#done_ty(#v_body),
                ),
            };
            let finish = quote! {
                Self::#open(#v_slot, #v_body) => ::core::result::Result::Ok(Self::#finished(
                    #v_slot, <F as #ns_core::decode::state::Finish>::#end_fn(#v_body),
                )),
                Self::#finished(#v_slot, #v_body) =>
                    ::core::result::Result::Err(Self::#finished(#v_slot, #v_body)),
            };
            let gated = !entry.guards.is_empty();
            let subset = if gated { &guarded } else { &plain };
            if !gated {
                let (input, payload) = match item.kind {
                    spec::Kind::Duplex => (quote!(()), quote!(::core::default::Default::default())),
                    _ => (
                        quote!(#v_body),
                        quote!(::core::convert::From::from(#v_body)),
                    ),
                };
                admits.push(quote! {
                    Self::#open(#v_slot, #input) => #ident::#open(#v_slot, #payload),
                    Self::#finished(#v_slot, #input) => #ident::#finished(#v_slot, #payload),
                });
            }

            let req_slot = match item.kind {
                spec::Kind::Duplex => {
                    quote! { #slot { head: #v_head, content_length: #v_len } }
                }
                spec::Kind::Sync | spec::Kind::Fiber | spec::Kind::Stream => {
                    quote! { #slot { head: #v_head } }
                }
            };
            let method = gated.then(|| entry.method.http_token(paths));
            let started = |phase: &syn::Ident, body: TokenStream| {
                let state = quote! { #subset::#phase(#req_slot, #body) };
                if all_p {
                    quote! { #ns_core::decode::request::Plain::new(#state) }
                } else if all_g {
                    quote! { #ns_core::decode::request::Guarded::new(#state, #method) }
                } else if !gated {
                    quote! { #ns_core::decode::request::Start::plain(#state) }
                } else {
                    quote! { #ns_core::decode::request::Start::guarded(#state, #method) }
                }
            };
            match item.kind {
                spec::Kind::Duplex => {
                    let open_v = started(&open, quote!(()));
                    let done_v = started(&finished, quote!(()));
                    start.push(quote! {
                        Self::#variant(#v_head, #v_len) => ::core::result::Result::Ok(match #v_bound {
                            #ns_core::manifold::stream::Boundary::End => #done_v,
                            #ns_core::manifold::stream::Boundary::More => #open_v,
                        })
                    });
                }
                spec::Kind::Sync | spec::Kind::Fiber | spec::Kind::Stream => {
                    let open_v = started(&open, quote!(#v_body));
                    let done_v = started(&finished, quote!(#v_body));
                    start.push(quote! {
                        Self::#variant(#v_head, #v_len) => match #ns_core::body::admit::Plan::new(
                            <<#route as #ns_core::route::Spec>::Request
                                as #ns_core::route::Schema>::BODY_RETENTION,
                            <#route as #ns_core::route::Spec>::MAX_BODY,
                            #ns_core::body::admit::Length::from_option(#v_len),
                            <<<#route as #ns_core::route::Spec>::Request
                                as #ns_core::route::Own>::Mode as #ns_core::route::Mode>::STREAM,
                        ).try_start(#v_limit, #v_bound) {
                            ::core::result::Result::Ok(
                                #ns_core::body::admit::Start::Open(#v_body),
                            ) => ::core::result::Result::Ok(#open_v),
                            ::core::result::Result::Ok(
                                #ns_core::body::admit::Start::Finished(#v_body),
                            ) => ::core::result::Result::Ok(#done_v),
                            ::core::result::Result::Err(#v_error) =>
                                ::core::result::Result::Err(#ns_core::o3::mem::Rejected::new(
                                    #selected::#variant(#v_head, #v_len),
                                    #v_error,
                                )),
                        }
                    });
                }
            }
            cases.push(Case {
                guarded: gated,
                kind: item.kind,
                open,
                finished,
                open_fn,
                done_fn,
                end_fn,
                variants,
                each,
                fold,
                body,
                finish,
            });
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
        let (exec, uniform) =
            match cases
                .first()
                .map(|case| case.kind)
                .filter(|kind| cases.iter().all(|case| case.kind == *kind))
            {
                Some(kind) => {
                    let kinds = cases.iter().map(|Case { open, finished, .. }| quote! {
                    #ident::#open(#v_slot, #v_body) => #v_fold.open(#v_slot, #v_body),
                    #ident::#finished(#v_slot, #v_body) => #v_fold.finished(#v_slot, #v_body),
                });
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
                None => (
                    quote!(#ns_core::decode::exec::Mixed),
                    proc_macro2::TokenStream::new(),
                ),
            };
        let core = flow::render(paths, vis, &prepared, &ident, &family, cases.iter());
        let subsets = mixed.then(|| {
            let plain_f = format_ident!("{}PlainFamily", spec.name);
            let p = flow::render(
                paths,
                vis,
                &prepared,
                &plain,
                &plain_f,
                cases.iter().filter(|case| !case.guarded),
            );
            let g = flow::render(
                paths,
                vis,
                &prepared,
                &guarded,
                &routes,
                cases.iter().filter(|case| case.guarded),
            );
            quote!(#p #g)
        });
        let admission = (!all_g).then(|| {
            quote! {
                impl<G: #ns_core::decode::state::admit::Payload>
                    #ns_core::decode::state::Admission<G> for #plain<#ns_core::decode::state::Routed>
                {
                    type Output = #ident<G>;
                    fn admit(self) -> Self::Output {
                        match self { #( #admits )* }
                    }
                }
            }
        });
        let prepared_variants = entries.iter().map(|item| &item.prepared);
        let prepared_slots = entries.iter().map(|item| &item.request);
        let tokens = quote! {
            #[doc(hidden)]
            #vis enum #prepared { #( #prepared_variants(#prepared_slots), )* }

            impl #ns_core::decode::request::Prepared for #prepared {
                type Execution = #exec;
                type Family = #family;
            }
            #uniform
            #core
            #subsets
            #admission

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
            routes,
            definition: tokens,
        }
    }
}
