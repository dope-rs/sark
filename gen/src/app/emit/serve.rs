use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::super::spec::{Gen, RouteKind};
use crate::route_compiler::Seg;
use crate::route_compiler::param_dfa::ParamRoute;
use crate::route_compiler::static_tree::StaticRoute;

struct TaskSpec<'a> {
    route: &'a syn::TypePath,
    route_index: &'a syn::Index,
    capacity: &'a syn::Expr,
    slot: usize,
    future: syn::Ident,
    maker: syn::Ident,
}

impl TaskSpec<'_> {
    fn task_type(&self) -> TokenStream {
        let route = self.route;
        let future = &self.future;
        quote! {
            <<#route as ::sark::service::RouteSpec>::Kind
                as ::sark::service::manifold::Kind<'d, #route, #future>>::Task
        }
    }
}

fn task_specs(spec: &Gen) -> Vec<TaskSpec<'_>> {
    spec.route_specs
        .iter()
        .zip(spec.routes.iter())
        .zip(spec.idx.iter())
        .filter_map(|((entry, route), route_index)| {
            if entry.kind == RouteKind::Sync {
                return None;
            }
            Some((entry, route, route_index))
        })
        .enumerate()
        .map(|(slot, (entry, route, route_index))| TaskSpec {
            route,
            route_index,
            capacity: entry.capacity.as_ref().expect("async route capacity"),
            slot,
            future: format_ident!("__F{:04}", slot),
            maker: format_ident!("__MK{:04}", slot),
        })
        .collect()
}

pub(super) struct ServeEmit<'a> {
    spec: &'a Gen,
}

impl<'a> ServeEmit<'a> {
    pub(super) fn new(spec: &'a Gen) -> Self {
        Self { spec }
    }

    pub(super) fn head_parts(&self) -> TokenStream {
        let routes = &self.spec.routes;
        let key_ident = &self.spec.key_ident;
        let parts_ident = &self.spec.parts_ident;
        let key_vars = &self.spec.key_vars;
        let parts_header_bytes_arms = &self.spec.parts_header_bytes_arms;
        let parts_query_name_arms = &self.spec.parts_query_name_arms;
        let parts_query_slice_arms = &self.spec.parts_query_slice_arms;
        let parts_query_parse_arms = &self.spec.parts_query_parse_arms;
        let route_tag_arms: Vec<TokenStream> = key_vars
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let tag = (i as u64) + 1;
                quote! { #parts_ident::#key { .. } => #tag, }
            })
            .collect();
        quote! {
            impl sark::service::HeadParts<#key_ident> for #parts_ident {
                const NEED_FIELDS: bool = false;
                const NEED_HEADER: bool = false #( || <<#routes as sark::service::RouteSpec>::Request as sark::service::RouteRequestImpl>::NEED_HEADER )*;
                const NEED_KNOWN_HEADER: bool =
                    false #( || <<#routes as sark::service::RouteSpec>::Request as sark::service::RouteRequestImpl>::NEED_KNOWN_HEADER )*;
                const NEED_QUERY: bool = false #( || <<#routes as sark::service::RouteSpec>::Request as sark::service::RouteRequestImpl>::NEED_QUERY )*;

                fn new(_route: #key_ident) -> Self {
                    #parts_ident::Miss {
                        method_key: None,
                        path_hit: false,
                    }
                }

                fn wants_query(&self) -> bool {
                    match self {
                        #( #parts_ident::#key_vars { .. } => <<#routes as sark::service::RouteSpec>::Request as sark::service::RouteRequestImpl>::NEED_QUERY, )*
                        #parts_ident::Miss { .. } => false,
                    }
                }

                fn route_tag(&self) -> u64 {
                    match self {
                        #( #route_tag_arms )*
                        #parts_ident::Miss { method_key, path_hit } => {
                            sark::service::Key::miss_tag(*method_key, *path_hit)
                        }
                    }
                }

                fn set_header_name<V>(&mut self, name: &[u8], value: &V) -> sark::error::Result<()>
                where
                    V: sark::service::HeaderValue,
                {
                    match self {
                        #( #parts_header_bytes_arms )*
                        #parts_ident::Miss { .. } => {}
                    }
                    Ok(())
                }

                fn apply_header<I: sark::sark_core::http::head::HeadInput + ?Sized>(
                    &mut self,
                    input: &I,
                    line: &[u8],
                    line_start: usize,
                    colon_idx: usize,
                    pretrim_start: Option<usize>,
                    pretrim_end: Option<usize>,
                    scan: &mut sark_core::http::codec::HeaderScan,
                    flags: &mut sark::sark_core::http::head::Flags,
                    scan_info: Option<&sark::sark_core::http::head::HeaderLineScan>,
                ) -> sark::error::Result<()> {
                    match self {
                        #( #parts_ident::#key_vars { headers, .. } => <<#routes as sark::service::RouteSpec>::Request as sark::service::RouteRequestImpl>::apply_header(headers, input, line, line_start, colon_idx, pretrim_start, pretrim_end, scan, flags, scan_info), )*
                        #parts_ident::Miss { .. } => sark::parser::head::HeaderApply::generic::<I, #key_ident, Self>(input, line, line_start, colon_idx, pretrim_start, pretrim_end, self, scan, flags, scan_info),
                    }
                }

                fn set_header<V>(&mut self, slot: u8, value: &V) -> sark::error::Result<()>
                where
                    V: sark::service::HeaderValue,
                {
                    match self {
                        #( #parts_ident::#key_vars { headers, .. } => <<#routes as sark::service::RouteSpec>::Request as sark::service::RouteRequestImpl>::set_header_u8(headers, slot, value)?, )*
                        #parts_ident::Miss { .. } => {}
                    }
                    Ok(())
                }

                fn set_query_name<V>(&mut self, name: &[u8], value: &V) -> sark::error::Result<()>
                where
                    V: sark::service::HeaderValue,
                {
                    match self {
                        #( #parts_query_name_arms )*
                        #parts_ident::Miss { .. } => {}
                    }
                    Ok(())
                }

                fn set_query_slice(
                    &mut self,
                    name: &[u8],
                    input: &[u8],
                    range: std::ops::Range<usize>,
                ) -> sark::error::Result<()> {
                    match self {
                        #( #parts_query_slice_arms )*
                        #parts_ident::Miss { .. } => {}
                    }
                    Ok(())
                }

                fn parse_query(
                    &mut self,
                    input: &[u8],
                    range: std::ops::Range<usize>,
                ) -> sark::error::Result<()> {
                    match self {
                        #( #parts_query_parse_arms )*
                        #parts_ident::Miss { .. } => {}
                    }
                    Ok(())
                }
            }
        }
    }

    pub(super) fn head_visitor(&self) -> TokenStream {
        let vis = &self.spec.vis;
        let parts_ident = &self.spec.parts_ident;
        let visitor_ident = format_ident!("{}Visitor", parts_ident);
        let routes = &self.spec.routes;
        quote! {
            #[allow(dead_code)]
            #vis struct #visitor_ident {
                parts: #parts_ident,
            }

            #[allow(dead_code)]
            impl #visitor_ident {
                pub fn new() -> Self {
                    Self {
                        parts: #parts_ident::Miss {
                            method_key: None,
                            path_hit: false,
                        },
                    }
                }

                pub fn into_parts(self) -> #parts_ident {
                    self.parts
                }
            }

            impl Default for #visitor_ident {
                fn default() -> Self {
                    Self::new()
                }
            }

            impl ::sark::http::head::Visitor for #visitor_ident {
                type Parsed = ::sark::http::head::ParsedRequest;

                const WANTS_KNOWN: bool =
                    false #( || <<#routes as sark::service::RouteSpec>::Request as sark::service::RouteRequestImpl>::NEED_KNOWN_HEADER )*;

                fn start_line(
                    &mut self,
                    _parsed: &Self::Parsed,
                    _raw: &[u8],
                ) -> ::sark::error::Result<()> {
                    Ok(())
                }

                fn known(
                    &mut self,
                    _key: ::sark::http::head::KnownHeader,
                    _value: &[u8],
                ) -> ::sark::error::Result<()> {
                    Ok(())
                }

                fn unknown(
                    &mut self,
                    _name: &[u8],
                    _value: &[u8],
                ) -> ::sark::error::Result<()> {
                    Ok(())
                }
            }
        }
    }

    pub(super) fn plan(&self) -> TokenStream {
        let vis = &self.spec.vis;
        let plan_ident = &self.spec.plan_ident;
        let key_ident = &self.spec.key_ident;
        quote! {
            #vis struct #plan_ident;

            impl sark::service::HeadPlan for #plan_ident {
                type RouteKey = #key_ident;

                fn route_key_probe<P: sark::service::PathProbe>(
                    &self,
                    _method_key: sark::service::Key,
                    _path: &P,
                ) -> Self::RouteKey {
                    #key_ident::Miss
                }
            }
        }
    }

    pub(super) fn app(&self) -> TokenStream {
        let vis = &self.spec.vis;
        let public_name = &self.spec.name;
        let name = format_ident!("{}Inner", public_name);
        let core_ident = format_ident!("{}Core", name);
        let state_ty = &self.spec.state_ty;
        let routes = &self.spec.routes;
        let sync_count = self
            .spec
            .route_specs
            .iter()
            .filter(|entry| entry.kind == RouteKind::Sync)
            .count();
        let route_bounds = &self.spec.route_bounds;
        let tasks = task_specs(self.spec);
        let task_count = tasks.len();
        let futures: Vec<_> = tasks.iter().map(|task| &task.future).collect();
        let makers: Vec<_> = tasks.iter().map(|task| &task.maker).collect();
        let kind_bounds: Vec<TokenStream> = tasks
            .iter()
            .map(|task| {
                let route = task.route;
                let future = &task.future;
                quote! {
                    <#route as ::sark::service::RouteSpec>::Kind:
                        ::sark::service::manifold::InvokeKind<#route>
                        + ::sark::service::manifold::Kind<
                            'd,
                            #route,
                            #future,
                            Owner = (),
                        >
                }
            })
            .collect();
        let maker_bounds: Vec<TokenStream> = tasks
            .iter()
            .map(|task| {
                let route = task.route;
                let future = &task.future;
                let maker = &task.maker;
                quote! {
                    #future: ::sark::fiber::Fiber<
                            'd,
                            Output = <<#route as ::sark::service::RouteSpec>::Kind
                                as ::sark::service::manifold::InvokeKind<#route>>::Output,
                        > + 'd,
                    #maker: ::core::marker::Copy
                        + 'd
                        + ::core::ops::FnOnce(
                            &'d #route,
                            <#route as ::sark::service::RouteSpec>::Params<'d>,
                            ::sark::request::Ref<'d>,
                            <#route as ::sark::service::RouteSpec>::Headers<'d>,
                            <#route as ::sark::service::RouteSpec>::ParsedBody<'d>,
                            &'d #state_ty,
                            &'d ::sark::Timer<'d>,
                        ) -> #future,
                }
            })
            .collect();
        let producer_values: Vec<TokenStream> = tasks
            .iter()
            .map(|task| {
                let route = task.route;
                quote! {
                    <#route as ::sark::service::manifold::TaskRoute<'d, #state_ty>>::invoke_task
                }
            })
            .collect();
        let task_types: Vec<TokenStream> = tasks.iter().map(TaskSpec::task_type).collect();
        let capacities: Vec<_> = tasks.iter().map(|task| task.capacity).collect();
        let task_tags: Vec<_> = (0..task_count)
            .map(|slot| format_ident!("__{}TaskTag{:04}", public_name, slot))
            .collect();
        let task_slab_types: Vec<TokenStream> = task_types
            .iter()
            .zip(capacities.iter())
            .zip(task_tags.iter())
            .map(|((task, capacity), tag)| {
                quote! {
                    ::sark::fiber::FixedSlab<'d, #task, { #capacity }, #tag>
                }
            })
            .collect();
        let route_values: Vec<TokenStream> = routes.iter().map(|route| quote!(#route)).collect();
        let constructor_module = {
            let value = public_name.to_string();
            let mut snake = String::with_capacity(value.len() + 4);
            for (index, character) in value.chars().enumerate() {
                if character.is_uppercase() && index > 0 {
                    snake.push('_');
                }
                snake.push(character.to_ascii_lowercase());
            }
            format_ident!("__{}_constructor", snake)
        };
        let app_generic_def = quote! {
            <
                'd,
                __W: ::dope_net::wire::Wire,
                #( #futures, )*
                #( #makers, )*
            >
        };
        let generic_use = quote! {
            <'d, __W, #( #futures, )* #( #makers, )*>
        };
        let build_return = quote! {
            super::#name<'d, __W, #( #futures, )* #( #makers, )*>
        };
        let task_fields = if tasks.is_empty() {
            TokenStream::new()
        } else {
            quote! {
                tasks: ( #( #task_slab_types, )* ),
                task_producers: ( #( #makers, )* ),
                task_capacity: usize,
                active_tasks: usize,
            }
        };
        let task_initializers = if tasks.is_empty() {
            TokenStream::new()
        } else {
            quote! {
                tasks: ( #( { let _ = #capacities; ::sark::fiber::FixedSlab::new() }, )* ),
                task_producers: producers,
                task_capacity: config.task_capacity,
                active_tasks: 0,
            }
        };
        let producer_parameter = if tasks.is_empty() {
            TokenStream::new()
        } else {
            quote! { producers: ( #( #makers, )* ), }
        };
        let producer_argument = if tasks.is_empty() {
            TokenStream::new()
        } else {
            quote! { ( #( #producer_values, )* ), }
        };
        let task_count_assert = if tasks.is_empty() {
            TokenStream::new()
        } else {
            quote! {
                const _: () = assert!(
                    #task_count <= u16::MAX as usize,
                    "route task count must fit in u16",
                );
            }
        };

        quote! {
            #task_count_assert
            #( struct #task_tags; )*

            struct #core_ident #app_generic_def
            where
                #( #route_bounds )*
                #( #maker_bounds )*
                #( #kind_bounds, )*
            {
                response_cache: [
                    ::core::cell::OnceCell<::sark::dispatch::response_cache::Entry>;
                    #sync_count
                ],
                gzip: ::sark::sark_core::http::compress::Gzip,
                #task_fields
                timer: ::sark::Timer<'d>,
                routes: ( #( #routes, )* ),
                state: #state_ty,
                marker: ::core::marker::PhantomData<__W>,
                pin: ::core::marker::PhantomPinned,
            }

            struct #name #app_generic_def
            where
                #( #route_bounds )*
                #( #maker_bounds )*
                #( #kind_bounds, )*
            {
                core: #core_ident #generic_use,
                date: ::sark::date::Stamp,
            }

            impl #app_generic_def #name #generic_use
            where
                #( #route_bounds )*
                #( #maker_bounds )*
                #( #kind_bounds, )*
            {
                fn __project(
                    self: ::core::pin::Pin<&mut Self>,
                ) -> (
                    ::core::pin::Pin<&mut #core_ident #generic_use>,
                    ::core::pin::Pin<&mut ::sark::date::Stamp>,
                ) {
                    let this = unsafe { ::core::pin::Pin::into_inner_unchecked(self) };
                    unsafe {
                        (
                            ::core::pin::Pin::new_unchecked(&mut this.core),
                            ::core::pin::Pin::new_unchecked(&mut this.date),
                        )
                    }
                }
            }

            #vis struct #public_name;

            impl #public_name {
                #vis fn new<
                    'd,
                    __W: ::dope_net::wire::Wire,
                >(
                    state: #state_ty,
                    config: ::sark::app::Config,
                ) -> impl ::dope::manifold::listener::Application<
                        'd,
                        Conn = ::sark::dispatch::conn_state::ConnState,
                        Wire = __W,
                    >
                    + ::sark::date::DateHost
                    + ::sark::timer::TimerHost<'d>
                    + ::sark::dispatch::H1Project<'d, __W>
                    + ::sark::dispatch::Decode
                    + ::sark::dispatch::Routing<'d>
                    + 'd
                where
                    #( #route_bounds )*
                {
                    #constructor_module::build(
                        state,
                        config,
                        #producer_argument
                    )
                }
            }

            mod #constructor_module {
                use super::*;

                pub(super) fn build<
                    'd,
                    __W: ::dope_net::wire::Wire,
                    #( #futures, )*
                    #( #makers, )*
                >(
                    state: #state_ty,
                    config: ::sark::app::Config,
                    #producer_parameter
                ) -> #build_return
                where
                    #( #route_bounds )*
                    #( #maker_bounds )*
                    #( #kind_bounds, )*
                {
                    super::#name {
                        core: super::#core_ident {
                            response_cache: [
                                const { ::core::cell::OnceCell::new() };
                                #sync_count
                            ],
                            gzip: ::sark::sark_core::http::compress::Gzip::new(),
                            #task_initializers
                            timer: ::sark::Timer::with_capacity(config.timer_capacity),
                            routes: ( #( #route_values, )* ),
                            state,
                            marker: ::core::marker::PhantomData,
                            pin: ::core::marker::PhantomPinned,
                        },
                        date: ::sark::date::Stamp::new(),
                    }
                }
            }
        }
    }

    pub(super) fn handle_bytes(&self) -> TokenStream {
        let name = format_ident!("{}Inner", self.spec.name);
        let state_ty = &self.spec.state_ty;
        let routes = &self.spec.routes;
        let route_bounds = &self.spec.route_bounds;
        let indices = &self.spec.idx;
        let core_ident = format_ident!("{}Core", name);
        let tasks = task_specs(self.spec);
        let futures: Vec<_> = tasks.iter().map(|task| &task.future).collect();
        let makers: Vec<_> = tasks.iter().map(|task| &task.maker).collect();
        let task_types: Vec<TokenStream> = tasks.iter().map(TaskSpec::task_type).collect();
        let task_tags: Vec<_> = (0..tasks.len())
            .map(|slot| format_ident!("__{}TaskTag{:04}", self.spec.name, slot))
            .collect();
        let mut route_task_slots = vec![None; routes.len()];
        for task in &tasks {
            route_task_slots[task.route_index.index as usize] = Some(task.slot);
        }
        let mut route_cache_slots = vec![None; routes.len()];
        let mut cache_slot = 0usize;
        for (index, entry) in self.spec.route_specs.iter().enumerate() {
            if entry.kind == RouteKind::Sync {
                route_cache_slots[index] = Some(cache_slot);
                cache_slot += 1;
            }
        }
        let route_has_param: Vec<bool> = self
            .spec
            .route_specs
            .iter()
            .map(|entry| {
                Seg::segment(&entry.path.value())
                    .iter()
                    .any(|segment| matches!(segment, Seg::Param))
            })
            .collect();
        let wrap_before: Vec<TokenStream> = self
            .spec
            .route_specs
            .iter()
            .map(|entry| build_wrap_before_chain(&entry.wraps))
            .collect();
        let maker_bounds: Vec<TokenStream> = tasks
            .iter()
            .map(|task| {
                let route = task.route;
                let future = &task.future;
                let maker = &task.maker;
                quote! {
                    #future: ::sark::fiber::Fiber<
                            'd,
                            Output = <<#route as ::sark::service::RouteSpec>::Kind
                                as ::sark::service::manifold::InvokeKind<#route>>::Output,
                        > + 'd,
                    #maker: ::core::marker::Copy
                        + 'd
                        + ::core::ops::FnOnce(
                            &'d #route,
                            <#route as ::sark::service::RouteSpec>::Params<'d>,
                            ::sark::request::Ref<'d>,
                            <#route as ::sark::service::RouteSpec>::Headers<'d>,
                            <#route as ::sark::service::RouteSpec>::ParsedBody<'d>,
                            &'d #state_ty,
                            &'d ::sark::Timer<'d>,
                        ) -> #future,
                    <#route as ::sark::service::RouteSpec>::Kind:
                        ::sark::service::manifold::InvokeKind<#route>
                        + ::sark::service::manifold::Kind<
                            'd,
                            #route,
                            #future,
                            Owner = (),
                        >
                        + ::sark::dispatch::Dispatch<'d, #route, #state_ty, #future>
                        + ::sark::dispatch::Complete<'d, #route, #future>
                        + ::sark::dispatch::DecodeRoute<#route, #state_ty>,
                }
            })
            .collect();
        let decode_bounds: Vec<TokenStream> = self
            .spec
            .route_specs
            .iter()
            .zip(routes.iter())
            .filter(|(entry, _)| entry.kind == RouteKind::Sync)
            .map(|(_, route)| {
                quote! {
                    <#route as ::sark::service::RouteSpec>::Kind:
                        ::sark::dispatch::DecodeRoute<#route, #state_ty>,
                }
            })
            .collect();
        let generic_def = quote! {
            <
                'd,
                __W: ::dope_net::wire::Wire,
                #( #futures, )*
                #( #makers, )*
            >
        };
        let generic_use = quote! {
            <'d, __W, #( #futures, )* #( #makers, )*>
        };
        let dispatch_for = |index: usize, raw_params: TokenStream| {
            let route = &routes[index];
            let route_index = &indices[index];
            let middleware = &wrap_before[index];
            let setup = quote! {
                #middleware
                let route: &'d #route =
                    unsafe { &*(&this.routes.#route_index as *const #route) };
                let state: &'d #state_ty = state;
            };
            let Some(task_slot) = route_task_slots[index] else {
                let cache_index =
                    syn::Index::from(route_cache_slots[index].expect("sync route cache slot"));
                return quote! {
                    #setup
                    return ::sark::dispatch::SyncRoute::new(
                        &ctx,
                        date,
                        ::sark::dispatch::response_cache::Cache::new(
                            &this.response_cache[#cache_index],
                        ),
                        &mut this.gzip,
                        write,
                    ).dispatch::<#route, #state_ty>(
                        permit,
                        ::sark::dispatch::Matched {
                            route,
                            raw_params: #raw_params,
                        },
                        state,
                    );
                };
            };
            let task = &tasks[task_slot];
            let future = &task.future;
            let capacity = task.capacity;
            let task_type = &task_types[task_slot];
            let task_tag = &task_tags[task_slot];
            let task_index = syn::Index::from(task_slot);
            let task_route = task_slot as u16;
            quote! {
                #setup
                if this.active_tasks >= this.task_capacity {
                    return ::sark::dispatch::ConsumeOutcome::Close(::sark::CANNED_503);
                }
                let timer: &'d ::sark::Timer<'d> =
                    unsafe { &*(::sark::timer::TimerHost::timer(this) as *const _) };
                let producer = this.task_producers.#task_index;
                let outcome = <<#route as ::sark::service::RouteSpec>::Kind
                    as ::sark::dispatch::Dispatch<
                        'd,
                        #route,
                        #state_ty,
                        #future,
                    >>::dispatch::<#task_type, #task_tag, _, _, { #capacity }>(
                        permit,
                        scope,
                        ::sark::dispatch::Matched {
                        route,
                        raw_params: #raw_params,
                    },
                    unsafe {
                        ::core::pin::Pin::new_unchecked(&mut this.tasks.#task_index)
                    },
                    state,
                    &ctx,
                    timer,
                    conn,
                    date,
                    ::sark::dispatch::response_cache::Cache::empty(),
                    &mut this.gzip,
                    write,
                    producer,
                    |task, ()| task,
                );
                if conn.async_state.task.is_some() {
                    conn.async_state.task_route = #task_route;
                    this.active_tasks += 1;
                }
                return outcome;
            }
        };

        let mut static_routes = Vec::new();
        let mut param_routes = Vec::new();
        for (index, entry) in self.spec.route_specs.iter().enumerate() {
            let route = &routes[index];
            let path = entry.path.value();
            if route_has_param[index] {
                let segments = Seg::segment(&path);
                let captures: Vec<_> = (0..segments
                    .iter()
                    .filter(|segment| matches!(segment, Seg::Param))
                    .count())
                    .map(|capture| format_ident!("__cap{}", capture))
                    .collect();
                let captures = quote!(( #( #captures, )* ));
                let dispatch = dispatch_for(index, quote!(__raw));
                param_routes.push(ParamRoute {
                    method: entry.meta.method,
                    segs: segments,
                    body: quote! {
                        let ::core::option::Option::Some(__raw) =
                            <#route as ::sark::service::RouteSpec>::from_captures(
                                &ctx.slice_path,
                                #captures,
                            )
                        else {
                            return ::sark::dispatch::ConsumeOutcome::Close(
                                ::sark::CANNED_404,
                            );
                        };
                        #dispatch
                    },
                });
            } else {
                let raw = quote! {
                    <<#route as ::sark::service::RouteSpec>::RawParams
                        as ::core::default::Default>::default()
                };
                static_routes.push(StaticRoute {
                    method: entry.meta.method,
                    path: path.into_bytes(),
                    body: dispatch_for(index, raw),
                });
            }
        }
        let param_dfa = ParamRoute::compile(param_routes);
        let static_tree = StaticRoute::compile(static_routes);
        let context = if static_tree.is_empty() && param_dfa.is_empty() {
            quote!(let _ = method_key;)
        } else {
            quote! {
                let ctx = ::sark::dispatch::Ctx::parse_with_key(
                    req_bytes,
                    head,
                    method_key,
                );
            }
        };
        let method_path = if static_tree.is_empty() {
            TokenStream::new()
        } else {
            quote! {
                let __method = method_key;
                let __path = ctx.slice_path.bytes();
            }
        };
        let dispatch_body = quote! {
            let target = head.target;
            if target.first() != ::core::option::Option::Some(&b'/') {
                return ::sark::dispatch::ConsumeOutcome::Close(
                    if target == b"*" {
                        ::sark::CANNED_404
                    } else {
                        ::sark::CANNED_400
                    },
                );
            }
            #context
            #method_path
            #static_tree
            #param_dfa
            ::sark::dispatch::ConsumeOutcome::Close(::sark::CANNED_404)
        };

        let mut decoded_routes = Vec::new();
        for (index, entry) in self.spec.route_specs.iter().enumerate() {
            if route_has_param[index] {
                continue;
            }
            let route = &routes[index];
            let route_index = &indices[index];
            decoded_routes.push(StaticRoute {
                method: entry.meta.method,
                path: entry.path.value().into_bytes(),
                body: quote! {
                    let mut raw_headers =
                        <<#route as ::sark::service::RouteSpec>::RawHeaders
                            as ::core::default::Default>::default();
                    for &(name, ref range) in __headers {
                        if let ::core::option::Option::Some(slot) =
                            <<#route as ::sark::service::RouteSpec>::Request
                                as ::sark::service::RouteRequestImpl>::header_slot_bytes(name)
                        {
                            if <<#route as ::sark::service::RouteSpec>::Request
                                as ::sark::service::RouteRequestImpl>::set_header_raw(
                                &mut raw_headers,
                                slot,
                                &::sark::service::SliceValue::new(
                                    __head_bytes,
                                    ::core::clone::Clone::clone(range),
                                ),
                            )
                            .is_err()
                            {
                                return ::sark::dispatch::Decoded::Bad;
                            }
                        }
                    }
                    return <<#route as ::sark::service::RouteSpec>::Kind
                        as ::sark::dispatch::DecodeRoute<#route, #state_ty>>::decode(
                        &self.routes.#route_index,
                        <<#route as ::sark::service::RouteSpec>::RawParams
                            as ::core::default::Default>::default(),
                        raw_headers,
                        ::core::clone::Clone::clone(&__http_method),
                        __head_bytes,
                        __body_bytes,
                        &self.state,
                        __encoder,
                    );
                },
            });
        }
        let decoded_tree = StaticRoute::compile(decoded_routes);
        let decode_method = quote! {
            fn dispatch_decoded<__E: ::sark::dispatch::ResponseEncoder>(
                &self,
                __http_method: ::sark::sark_core::http::Method,
                __path: &[u8],
                __headers: &[(&[u8], ::core::ops::Range<usize>)],
                __head_bytes: &[u8],
                __body_bytes: &[u8],
                __encoder: &mut __E,
            ) -> ::sark::dispatch::Decoded {
                let __method = ::sark::service::Key::from_bytes(
                    ::sark::sark_core::http::Method::as_str(&__http_method).as_bytes(),
                );
                #decoded_tree
                ::sark::dispatch::Decoded::NotFound
            }
        };
        let pump_arms: Vec<TokenStream> = tasks
            .iter()
            .map(|task| {
                let route = task.route;
                let future = &task.future;
                let task_index = syn::Index::from(task.slot);
                let task_route = task.slot as u16;
                quote! {
                    #task_route => {
                        let task_runner = ::sark::dispatch::TaskRunner::new(&task_date);
                        let written = task_runner.poll(
                            unsafe {
                                ::core::pin::Pin::new_unchecked(&mut this.tasks.#task_index)
                            },
                            slot,
                            aux,
                            driver,
                            &project,
                            |output, task_slot, task_aux, task_driver, task_date, close| {
                                <<#route as ::sark::service::RouteSpec>::Kind
                                    as ::sark::dispatch::Complete<
                                        'd,
                                        #route,
                                        #future,
                                    >>::complete(
                                    output,
                                    task_slot,
                                    task_aux,
                                    task_driver,
                                    task_date,
                                    close,
                                )
                            },
                        );
                        if written > 0 {
                            let buffer = task_runner.write_buf(slot, aux);
                            let token = slot.token();
                            ::dope::manifold::listener::SlotEgress::submit_buffered(
                                slot,
                                buffer,
                                written,
                                token,
                                driver,
                            );
                        }
                    }
                }
            })
            .collect();
        let pump = quote! {
            let task_route = project(&mut slot.state.conn).async_state.task_route;
            let task_date = date.load();
            match task_route {
                #( #pump_arms )*
                _ => unsafe { ::core::hint::unreachable_unchecked() },
            }
            if !project(&mut slot.state.conn).async_state.has_task() {
                this.active_tasks -= 1;
            }
        };
        let send_body = if tasks.is_empty() {
            quote! {
                let mut host = ::sark::dispatch::H1Host::new(self, date);
                ::sark::dispatch::H1Driver::new(
                    ::core::pin::Pin::new(&mut host),
                ).send_complete_proj(
                    sent,
                    slot,
                    aux,
                    driver,
                    &project,
                );
            }
        } else {
            quote! {
                {
                    let this = unsafe { self.as_mut().get_unchecked_mut() };
                    if project(&mut slot.state.conn).async_state.task_stream
                        && project(&mut slot.state.conn).async_state.has_task()
                    {
                        #pump
                        if project(&mut slot.state.conn).async_state.has_task() {
                            return;
                        }
                    }
                }
                let mut host = ::sark::dispatch::H1Host::new(self, date);
                ::sark::dispatch::H1Driver::new(
                    ::core::pin::Pin::new(&mut host),
                ).send_complete_proj(
                    sent,
                    slot,
                    aux,
                    driver,
                    &project,
                );
            }
        };
        let wake_body = if tasks.is_empty() {
            quote! {
                let this = unsafe { self.as_mut().get_unchecked_mut() };
                let _ = ::sark::dispatch::HeadDeadline::new(this).poll_proj(
                    slot,
                    aux,
                    driver,
                    &project,
                );
            }
        } else {
            quote! {
                let this = unsafe { self.as_mut().get_unchecked_mut() };
                if ::sark::dispatch::HeadDeadline::new(this).poll_proj(
                    slot,
                    aux,
                    driver,
                    &project,
                ) {
                    return;
                }
                if !project(&mut slot.state.conn).async_state.has_task() {
                    return;
                }
                if project(&mut slot.state.conn).async_state.task_stream
                    && slot.is_send_inflight()
                {
                    return;
                }
                #pump
            }
        };
        let release_arms: Vec<TokenStream> = tasks
            .iter()
            .map(|task| {
                let task_index = syn::Index::from(task.slot);
                let task_route = task.slot as u16;
                let task_tag = &task_tags[task.slot];
                quote! {
                    #task_route => {
                        let slab = unsafe {
                            ::core::pin::Pin::new_unchecked(&mut this.tasks.#task_index)
                        };
                        slab.remove(
                            ::sark::fiber::TaskId::<#task_tag>::from_erased(task),
                        )
                    },
                }
            })
            .collect();
        let close_body = if tasks.is_empty() {
            quote! {
                let this = unsafe { self.get_unchecked_mut() };
                ::sark::dispatch::HeadDeadline::new(this).cancel_proj(
                    slot,
                    &project,
                );
            }
        } else {
            quote! {
                let this = unsafe { self.get_unchecked_mut() };
                ::sark::dispatch::HeadDeadline::new(this).cancel_proj(
                    slot,
                    &project,
                );
                if let ::core::option::Option::Some(task) =
                    project(&mut slot.state.conn).async_state.task.take()
                {
                    let removed = match project(&mut slot.state.conn).async_state.task_route {
                        #( #release_arms )*
                        _ => false,
                    };
                    debug_assert!(removed, "live task must be removable");
                    this.active_tasks -= 1;
                    let state = project(&mut slot.state.conn);
                    state.async_state.task_stream = false;
                }
            }
        };
        let projection_bounds = quote! {
            __C: ::core::default::Default + 'static,
            __PJ: ::core::ops::Fn(
                &mut __C,
            ) -> &mut ::sark::dispatch::conn_state::ConnState,
        };
        let projection_slot = quote! {
            ::dope_net::link::slot::Slot<
                'd,
                __W,
                ::dope::manifold::listener::State<__C>,
            >
        };
        quote! {
            impl #generic_def #core_ident #generic_use
            where
                #( #route_bounds )*
                #( #maker_bounds )*
            {
                fn chunk_proj<__C, __PJ>(
                    self: ::core::pin::Pin<&mut Self>,
                    date: &::sark::date::Stamp,
                    slot: &mut #projection_slot,
                    bytes: &[u8],
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::DriverContext<'_, 'd>,
                    project: __PJ,
                ) -> bool
                where
                    #projection_bounds
                {
                    let mut host = ::sark::dispatch::H1Host::new(self, date);
                    ::sark::dispatch::H1Driver::new(
                        ::core::pin::Pin::new(&mut host),
                    ).run_proj(
                        bytes,
                        slot,
                        aux,
                        driver,
                        project,
                    )
                }

                fn send_proj<__C, __PJ>(
                    mut self: ::core::pin::Pin<&mut Self>,
                    date: &::sark::date::Stamp,
                    slot: &mut #projection_slot,
                    project: __PJ,
                    sent: usize,
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::DriverContext<'_, 'd>,
                )
                where
                    #projection_bounds
                {
                    #send_body
                }

                fn activate_proj<__C, __PJ>(
                    mut self: ::core::pin::Pin<&mut Self>,
                    date: &::sark::date::Stamp,
                    slot: &mut #projection_slot,
                    project: __PJ,
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::DriverContext<'_, 'd>,
                )
                where
                    #projection_bounds
                {
                    #wake_body
                }

                fn close_proj<__C, __PJ>(
                    self: ::core::pin::Pin<&mut Self>,
                    slot: &mut #projection_slot,
                    project: __PJ,
                    _aux: &mut ::dope::manifold::listener::Aux,
                )
                where
                    #projection_bounds
                {
                    #close_body
                }
            }

            impl #generic_def ::sark::dispatch::H1Project<'d, __W>
                for #name #generic_use
            where
                #( #route_bounds )*
                #( #maker_bounds )*
            {
                fn chunk_proj<__C, __PJ>(
                    self: ::core::pin::Pin<&mut Self>,
                    slot: &mut #projection_slot,
                    bytes: &[u8],
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::DriverContext<'_, 'd>,
                    project: __PJ,
                ) -> bool
                where
                    #projection_bounds
                {
                    let (mut core, date) = self.__project();
                    core.as_mut().chunk_proj(
                        date.as_ref().get_ref(),
                        slot,
                        bytes,
                        aux,
                        driver,
                        project,
                    )
                }

                fn send_proj<__C, __PJ>(
                    self: ::core::pin::Pin<&mut Self>,
                    slot: &mut #projection_slot,
                    project: __PJ,
                    sent: usize,
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::DriverContext<'_, 'd>,
                )
                where
                    #projection_bounds
                {
                    let (mut core, date) = self.__project();
                    core.as_mut().send_proj(
                        date.as_ref().get_ref(),
                        slot,
                        project,
                        sent,
                        aux,
                        driver,
                    );
                }

                fn activate_proj<__C, __PJ>(
                    self: ::core::pin::Pin<&mut Self>,
                    slot: &mut #projection_slot,
                    project: __PJ,
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::DriverContext<'_, 'd>,
                )
                where
                    #projection_bounds
                {
                    let (mut core, date) = self.__project();
                    core.as_mut().activate_proj(
                        date.as_ref().get_ref(),
                        slot,
                        project,
                        aux,
                        driver,
                    );
                }

                fn close_proj<__C, __PJ>(
                    self: ::core::pin::Pin<&mut Self>,
                    slot: &mut #projection_slot,
                    project: __PJ,
                    aux: &mut ::dope::manifold::listener::Aux,
                )
                where
                    #projection_bounds
                {
                    let (mut core, _) = self.__project();
                    core.as_mut().close_proj(slot, project, aux);
                }
            }

            impl #generic_def #core_ident #generic_use
            where
                #( #route_bounds )*
                #( #maker_bounds )*
            {
                #[allow(clippy::too_many_arguments)]
                fn dispatch_request<'buf>(
                    self: ::core::pin::Pin<&mut Self>,
                    scope: ::sark::fiber::FiberScope<'d>,
                    permit: ::sark::dispatch::conn_state::DispatchPermit,
                    state: &'d #state_ty,
                    req_bytes: &'buf [u8],
                    head: &::sark::sark_core::http::codec::ParsedRequestHead<'buf>,
                    method_key: ::sark::service::Key,
                    date: &[u8; 29],
                    write: &mut [u8],
                    conn: &mut ::sark::dispatch::conn_state::ConnState,
                ) -> ::sark::dispatch::ConsumeOutcome {
                    let this = unsafe { self.get_unchecked_mut() };
                    #dispatch_body
                }
            }

            impl #generic_def ::sark::dispatch::Decode for #core_ident #generic_use
            where
                #( #route_bounds )*
                #( #maker_bounds )*
                #( #decode_bounds )*
            {
                #decode_method
            }

            impl #generic_def ::sark::dispatch::Decode for #name #generic_use
            where
                #( #route_bounds )*
                #( #maker_bounds )*
                #( #decode_bounds )*
            {
                fn dispatch_decoded<__E: ::sark::dispatch::ResponseEncoder>(
                    &self,
                    method: ::sark::sark_core::http::Method,
                    path: &[u8],
                    headers: &[(&[u8], ::core::ops::Range<usize>)],
                    head_bytes: &[u8],
                    body_bytes: &[u8],
                    encoder: &mut __E,
                ) -> ::sark::dispatch::Decoded {
                    ::sark::dispatch::Decode::dispatch_decoded(
                        &self.core,
                        method,
                        path,
                        headers,
                        head_bytes,
                        body_bytes,
                        encoder,
                    )
                }
            }

            impl #generic_def ::sark::dispatch::RouteCore<'d> for #core_ident #generic_use
            where
                #( #route_bounds )*
                #( #maker_bounds )*
            {
                fn timer(&self) -> &::sark::Timer<'d> {
                    &self.timer
                }

                fn try_consume(
                    self: ::core::pin::Pin<&mut Self>,
                    stamp: &::sark::date::Stamp,
                    scope: ::sark::fiber::FiberScope<'d>,
                    permit: ::sark::dispatch::conn_state::DispatchPermit,
                    bytes: &[u8],
                    write: &mut [u8],
                    conn: &mut ::sark::dispatch::conn_state::ConnState,
                ) -> ::sark::dispatch::ConsumeOutcome {
                    let ::core::option::Option::Some(fused) =
                        ::sark::framer::FusedHead::parse(bytes)
                    else {
                        return ::sark::dispatch::ConsumeOutcome::NeedMore {
                            permit,
                            state: ::sark::dispatch::conn_state::NeedMore::Head,
                        };
                    };
                    let date = stamp.load();
                    let state: &'d #state_ty = unsafe {
                        &*(&self.as_ref().get_ref().state as *const #state_ty)
                    };
                    #core_ident::dispatch_request(
                        self,
                        scope,
                        permit,
                        state,
                        bytes,
                        &fused.head,
                        fused.method_key,
                        &date,
                        write,
                        conn,
                    )
                }
            }

            impl #generic_def ::sark::dispatch::Routing<'d> for #name #generic_use
            where
                #( #route_bounds )*
                #( #maker_bounds )*
            {
                fn try_consume(
                    self: ::core::pin::Pin<&mut Self>,
                    scope: ::sark::fiber::FiberScope<'d>,
                    permit: ::sark::dispatch::conn_state::DispatchPermit,
                    bytes: &[u8],
                    write: &mut [u8],
                    conn: &mut ::sark::dispatch::conn_state::ConnState,
                ) -> ::sark::dispatch::ConsumeOutcome {
                    let (core, date) = self.__project();
                    let mut host = ::sark::dispatch::H1Host::new(
                        core,
                        date.as_ref().get_ref(),
                    );
                    ::sark::dispatch::Routing::try_consume(
                        ::core::pin::Pin::new(&mut host),
                        scope,
                        permit,
                        bytes,
                        write,
                        conn,
                    )
                }
            }

            impl #generic_def ::sark::date::DateHost for #name #generic_use
            where
                #( #route_bounds )*
                #( #maker_bounds )*
            {
                fn stamp(
                    self: ::core::pin::Pin<&Self>,
                ) -> ::core::pin::Pin<&::sark::date::Stamp> {
                    unsafe { self.map_unchecked(|this| &this.date) }
                }
            }

            impl #generic_def ::sark::timer::TimerHost<'d> for #name #generic_use
            where
                #( #route_bounds )*
                #( #maker_bounds )*
            {
                fn timer(&self) -> &::sark::Timer<'d> {
                    &self.core.timer
                }
            }

            impl #generic_def ::sark::timer::TimerHost<'d> for #core_ident #generic_use
            where
                #( #route_bounds )*
                #( #maker_bounds )*
            {
                fn timer(&self) -> &::sark::Timer<'d> {
                    &self.timer
                }
            }

            impl #generic_def ::dope::manifold::listener::Application<'d>
                for #name #generic_use
            where
                #( #route_bounds )*
                #( #maker_bounds )*
            {
                type Conn = ::sark::dispatch::conn_state::ConnState;
                type Wire = __W;

                fn chunk<__R: ::sark::o3::buffer::RetainBytes>(
                    self: ::core::pin::Pin<&mut Self>,
                    slot: &mut ::dope_net::link::slot::Slot<
                        'd,
                        Self::Wire,
                        ::dope::manifold::listener::State<Self::Conn>,
                    >,
                    chunk: __R,
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::DriverContext<'_, 'd>,
                ) -> ::dope::manifold::Outcome {
                    if <Self as ::sark::dispatch::H1Project<'d, __W>>::chunk_proj(
                        self,
                        slot,
                        chunk.as_slice(),
                        aux,
                        driver,
                        ::sark::dispatch::identity_mut,
                    ) {
                        ::dope::manifold::Outcome::Overrun
                    } else {
                        ::dope::manifold::Outcome::Ok
                    }
                }

                fn send(
                    self: ::core::pin::Pin<&mut Self>,
                    slot: &mut ::dope_net::link::slot::Slot<
                        'd,
                        Self::Wire,
                        ::dope::manifold::listener::State<Self::Conn>,
                    >,
                    sent: usize,
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::DriverContext<'_, 'd>,
                ) {
                    <Self as ::sark::dispatch::H1Project<'d, __W>>::send_proj(
                        self,
                        slot,
                        ::sark::dispatch::identity_mut,
                        sent,
                        aux,
                        driver,
                    );
                }

                fn activate(
                    self: ::core::pin::Pin<&mut Self>,
                    slot: &mut ::dope_net::link::slot::Slot<
                        'd,
                        Self::Wire,
                        ::dope::manifold::listener::State<Self::Conn>,
                    >,
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::DriverContext<'_, 'd>,
                ) {
                    <Self as ::sark::dispatch::H1Project<'d, __W>>::activate_proj(
                        self,
                        slot,
                        ::sark::dispatch::identity_mut,
                        aux,
                        driver,
                    );
                }

                fn close(
                    self: ::core::pin::Pin<&mut Self>,
                    slot: &mut ::dope_net::link::slot::Slot<
                        'd,
                        Self::Wire,
                        ::dope::manifold::listener::State<Self::Conn>,
                    >,
                    aux: &mut ::dope::manifold::listener::Aux,
                ) {
                    <Self as ::sark::dispatch::H1Project<'d, __W>>::close_proj(
                        self,
                        slot,
                        ::sark::dispatch::identity_mut,
                        aux,
                    );
                }
            }
        }
    }
}

fn build_wrap_before_chain(wraps: &[syn::TypePath]) -> TokenStream {
    if wraps.is_empty() {
        return quote!();
    }
    let calls: Vec<TokenStream> = wraps
        .iter()
        .map(|w| {
            quote! {
                if <#w as ::sark::middleware::Middleware>::before(
                    &mut __mw_ctx, state, &mut __mw_capture,
                ) {
                    return ::sark::dispatch::ConsumeOutcome::Close(
                        __mw_capture.reason(),
                    );
                }
            }
        })
        .collect();
    quote! {
        let __mw_method = ::http::Method::from_bytes(head.method)
            .unwrap_or_else(|_| ::http::Method::GET);
        let mut __mw_ctx = ::sark::middleware::Ctx {
            method: &__mw_method,
            head_bytes: req_bytes,
            head,
            date,
        };
        let mut __mw_capture = ::sark::middleware::Capture::new();
        #( #calls )*
        let _ = __mw_ctx;
        let _ = __mw_capture;
    }
}
