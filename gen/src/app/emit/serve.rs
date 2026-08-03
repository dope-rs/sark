use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use sark_protocol::RequestHeadSemantic;
use syn::LitByteStr;

use super::super::spec::{Gen, RouteKind};
use crate::route_compiler::Seg;
use crate::route_compiler::param_dfa::ParamRoute;
use crate::route_compiler::static_tree::StaticRoute;

fn head_name_literal(semantic: RequestHeadSemantic) -> LitByteStr {
    LitByteStr::new(semantic.wire_name(), Span::call_site())
}

struct TaskSpec<'a> {
    route: &'a syn::TypePath,
    route_index: &'a syn::Index,
    kind: &'a RouteKind,
    capacity: &'a syn::Expr,
    slot: usize,
    producer: Option<ProducerSpec>,
}

struct ProducerSpec {
    slot: usize,
    future: syn::Ident,
    maker: syn::Ident,
}

impl TaskSpec<'_> {
    fn task_type(&self) -> TokenStream {
        let route = self.route;
        match self.kind {
            RouteKind::Fiber => {
                let future = &self
                    .producer
                    .as_ref()
                    .expect("fiber route task producer")
                    .future;
                quote!(#future)
            }
            RouteKind::Stream => {
                quote!(<#route as ::sark::service::RouteSpec>::Stream)
            }
            RouteKind::Sync => unreachable!("sync routes have no task slot"),
        }
    }

    fn producer_output(&self) -> TokenStream {
        let route = self.route;
        debug_assert!(matches!(self.kind, RouteKind::Fiber));
        quote!(
            ::core::result::Result<
                <#route as ::sark::service::RouteSpec>::AsyncResponse,
                &'static [u8],
            >
        )
    }

    fn slab_output(&self) -> TokenStream {
        match self.kind {
            RouteKind::Fiber => self.producer_output(),
            RouteKind::Stream => quote! {
                ::core::option::Option<::sark::o3::buffer::Shared>
            },
            RouteKind::Sync => unreachable!("sync routes have no task slot"),
        }
    }
}

fn task_specs(spec: &Gen) -> Vec<TaskSpec<'_>> {
    let mut tasks = Vec::new();
    let mut producer_slot = 0;
    for ((entry, route), route_index) in spec
        .route_specs
        .iter()
        .zip(spec.routes.iter())
        .zip(spec.idx.iter())
    {
        if entry.kind == RouteKind::Sync {
            continue;
        }
        let producer = (entry.kind == RouteKind::Fiber).then(|| {
            let slot = producer_slot;
            producer_slot += 1;
            ProducerSpec {
                slot,
                future: format_ident!("__F{:04}", slot),
                maker: format_ident!("__MK{:04}", slot),
            }
        });
        tasks.push(TaskSpec {
            route,
            route_index,
            kind: &entry.kind,
            capacity: entry.capacity.as_ref().expect("async route capacity"),
            slot: tasks.len(),
            producer,
        });
    }
    tasks
}

fn producer_idents<'a>(tasks: &'a [TaskSpec<'_>]) -> (Vec<&'a syn::Ident>, Vec<&'a syn::Ident>) {
    tasks
        .iter()
        .filter_map(|task| task.producer.as_ref())
        .map(|producer| (&producer.future, &producer.maker))
        .unzip()
}

fn producer_bounds(tasks: &[TaskSpec<'_>], state_ty: &syn::Type) -> Vec<TokenStream> {
    tasks
        .iter()
        .filter_map(|task| {
            let producer = task.producer.as_ref()?;
            let route = task.route;
            let future = &producer.future;
            let maker = &producer.maker;
            let output = task.producer_output();
            Some(quote! {
                #future: ::sark::fiber::Fiber<
                        'd,
                        Output = #output,
                    > + 'env,
                #maker: ::core::marker::Copy
                    + 'env
                    + ::core::ops::FnOnce(
                        ::sark::request::RequestStorage,
                        <#route as ::sark::service::RouteSpec>::RawParams,
                        <#route as ::sark::service::RouteSpec>::RawHeaders,
                        ::core::ops::Range<usize>,
                        &'env #state_ty,
                        &'env ::sark::Timer<'d>,
                    ) -> #future,
            })
        })
        .collect()
}

fn task_storage_bounds(tasks: &[TaskSpec<'_>]) -> Vec<TokenStream> {
    tasks
        .iter()
        .map(|task| {
            let task_type = task.task_type();
            let output = task.slab_output();
            quote! {
                #task_type: ::sark::fiber::FixedSlabFiber<'d, #output>,
            }
        })
        .collect()
}

fn stream_shape_bounds(tasks: &[TaskSpec<'_>]) -> Vec<TokenStream> {
    tasks
        .iter()
        .filter(|task| matches!(task.kind, RouteKind::Stream))
        .map(|task| {
            let route = task.route;
            quote! {
                for<'__req> <<#route as ::sark::service::RouteSpec>::Response<'__req>
                    as ::sark::sark_core::http::Shape<'__req>>::Metadata:
                    ::sark::sark_core::http::ShapeMetadata<
                        Stream = <#route as ::sark::service::RouteSpec>::Stream,
                    >,
            }
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

    pub(super) fn app(&self) -> TokenStream {
        let vis = &self.spec.vis;
        let public_name = &self.spec.name;
        let name = format_ident!("{}Inner", public_name);
        let core_ident = format_ident!("{}Core", name);
        let state_ty = &self.spec.state_ty;
        let sync_count = self
            .spec
            .route_specs
            .iter()
            .filter(|entry| entry.kind == RouteKind::Sync)
            .count();
        let route_bounds = &self.spec.route_bounds;
        let tasks = task_specs(self.spec);
        let task_count = tasks.len();
        let (futures, makers) = producer_idents(&tasks);
        let maker_bounds = producer_bounds(&tasks, state_ty);
        let slab_bounds = task_storage_bounds(&tasks);
        let stream_bounds = stream_shape_bounds(&tasks);
        let producer_values: Vec<TokenStream> = tasks
            .iter()
            .filter_map(|task| {
                task.producer.as_ref()?;
                let route = task.route;
                Some(quote! {
                    |
                        storage: ::sark::request::RequestStorage,
                        raw_params: <#route as ::sark::service::RouteSpec>::RawParams,
                        raw_headers: <#route as ::sark::service::RouteSpec>::RawHeaders,
                        target: ::core::ops::Range<usize>,
                        state: &'env #state_ty,
                        timer: &'env ::sark::Timer<'d>,
                    | {
                        <#route as ::sark::service::manifold::TaskRoute<'d, #state_ty>>::invoke_task(
                            storage,
                            raw_params,
                            raw_headers,
                            target,
                            state,
                            timer,
                        )
                    }
                })
            })
            .collect();
        let task_types: Vec<TokenStream> = tasks.iter().map(TaskSpec::task_type).collect();
        let capacities: Vec<_> = tasks.iter().map(|task| task.capacity).collect();
        let task_tags: Vec<_> = (0..task_count)
            .map(|slot| format_ident!("__{}TaskTag{:04}", public_name, slot))
            .collect();
        let task_field_names: Vec<_> = (0..task_count)
            .map(|slot| format_ident!("__task_slot_{slot:04}"))
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
                'env,
                'd: 'env,
                __W: ::dope_net::wire::Wire,
                #( #futures, )*
                #( #makers, )*
            >
        };
        let generic_use = quote! {
            <'env, 'd, __W, #( #futures, )* #( #makers, )*>
        };
        let build_return = quote! {
            super::#name<'env, 'd, __W, #( #futures, )* #( #makers, )*>
        };
        let producer_field = if makers.is_empty() {
            TokenStream::new()
        } else {
            quote! { task_producers: ( #( #makers, )* ), }
        };
        let producer_initializer = if makers.is_empty() {
            TokenStream::new()
        } else {
            quote! { task_producers: producers, }
        };
        let task_fields = if tasks.is_empty() {
            TokenStream::new()
        } else {
            quote! {
                #( #[pin] #task_field_names: #task_slab_types, )*
                #producer_field
                task_capacity: usize,
                active_tasks: usize,
            }
        };
        let task_initializers = if tasks.is_empty() {
            TokenStream::new()
        } else {
            quote! {
                #(
                    #task_field_names: {
                        let _ = #capacities;
                        ::sark::fiber::FixedSlab::new()
                    },
                )*
                #producer_initializer
                task_capacity: config.task_capacity,
                active_tasks: 0,
            }
        };
        let producer_parameter = if makers.is_empty() {
            TokenStream::new()
        } else {
            quote! { producers: ( #( #makers, )* ), }
        };
        let producer_argument = if makers.is_empty() {
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

            ::sark::__pin_project! {
                struct #core_ident #app_generic_def
                where
                    #( #slab_bounds )*
                {
                    response_cache: [
                        ::core::cell::OnceCell<::sark::dispatch::response_cache::Entry>;
                        #sync_count
                    ],
                    gzip: ::sark::sark_core::http::compress::Gzip,
                    #task_fields
                    timer: &'env ::sark::Timer<'d>,
                    state: &'env #state_ty,
                    marker: ::core::marker::PhantomData<__W>,
                    #[pin]
                    pin: ::core::marker::PhantomPinned,
                }
            }

            ::sark::__pin_project! {
                struct #name #app_generic_def
                where
                    #( #slab_bounds )*
                {
                    #[pin]
                    core: #core_ident #generic_use,
                    #[pin]
                    date: ::sark::date::Stamp,
                }
            }

            impl #app_generic_def #name #generic_use
            where
                #( #route_bounds )*
                #( #maker_bounds )*
                #( #slab_bounds )*
            {
                fn __project(
                    self: ::core::pin::Pin<&mut Self>,
                ) -> (
                    ::core::pin::Pin<&mut #core_ident #generic_use>,
                    ::core::pin::Pin<&mut ::sark::date::Stamp>,
                ) {
                    let this = self.project();
                    (this.core, this.date)
                }
            }

            #vis struct #public_name;

            impl #public_name {
                #vis fn new<
                    'env,
                    'd: 'env,
                    __W: ::dope_net::wire::Wire,
                >(
                    state: &'env #state_ty,
                    timer: &'env ::sark::Timer<'d>,
                    config: ::sark::app::Config,
                ) -> impl ::dope::manifold::listener::application::Application<
                        'd,
                        Conn = ::sark::dispatch::conn_state::ConnState,
                        Wire = __W,
                    >
                    + ::sark::date::DateHost
                    + ::sark::timer::TimerHost<'d>
                    + ::sark::dispatch::H1Project<'d, __W>
                    + ::sark::dispatch::Decode
                    + ::sark::dispatch::Routing<'d>
                    + 'env
                where
                    #( #route_bounds )*
                    #( #stream_bounds )*
                {
                    #constructor_module::build(
                        state,
                        timer,
                        config,
                        #producer_argument
                    )
                }
            }

            mod #constructor_module {
                use super::*;

                pub(super) fn build<
                    'env,
                    'd: 'env,
                    __W: ::dope_net::wire::Wire,
                    #( #futures, )*
                    #( #makers, )*
                >(
                    state: &'env #state_ty,
                    timer: &'env ::sark::Timer<'d>,
                    config: ::sark::app::Config,
                    #producer_parameter
                ) -> #build_return
                where
                    #( #route_bounds )*
                    #( #maker_bounds )*
                    #( #slab_bounds )*
                    #( #stream_bounds )*
                {
                    super::#name {
                        core: super::#core_ident {
                            response_cache: [
                                const { ::core::cell::OnceCell::new() };
                                #sync_count
                            ],
                            gzip: ::sark::sark_core::http::compress::Gzip::new(),
                            #task_initializers
                            timer,
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
        let method_head_name = head_name_literal(RequestHeadSemantic::METHOD);
        let path_head_name = head_name_literal(RequestHeadSemantic::PATH);
        let content_length_head_name = head_name_literal(RequestHeadSemantic::CONTENT_LENGTH);
        let name = format_ident!("{}Inner", self.spec.name);
        let state_ty = &self.spec.state_ty;
        let routes = &self.spec.routes;
        let route_bounds = &self.spec.route_bounds;
        let core_ident = format_ident!("{}Core", name);
        let tasks = task_specs(self.spec);
        let (futures, makers) = producer_idents(&tasks);
        let task_types: Vec<TokenStream> = tasks.iter().map(TaskSpec::task_type).collect();
        let slab_bounds = task_storage_bounds(&tasks);
        let stream_bounds = stream_shape_bounds(&tasks);
        let task_tags: Vec<_> = (0..tasks.len())
            .map(|slot| format_ident!("__{}TaskTag{:04}", self.spec.name, slot))
            .collect();
        let task_field_names: Vec<_> = (0..tasks.len())
            .map(|slot| format_ident!("__task_slot_{slot:04}"))
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
            .map(|entry| build_wrap_before_chain(&entry.wraps, entry.meta.method))
            .collect();
        let mut methods = Vec::new();
        for entry in &self.spec.route_specs {
            if !methods.contains(&entry.meta.method) {
                methods.push(entry.meta.method);
            }
        }
        methods.sort_by_key(|method| method.ord());
        let method_bits: Vec<TokenStream> =
            methods.iter().map(|method| method.bit_token()).collect();
        let method_mask = quote!(0u8 #( | #method_bits )*);
        let decoded_method_checks: Vec<TokenStream> = methods
            .iter()
            .map(|method| {
                let http = method.http_token();
                let key = method.key_token();
                quote! {
                    if __http_method == #http {
                        ::core::option::Option::Some(#key)
                    }
                }
            })
            .collect();
        let decoded_method_key = quote! {
            let decoded_method_key = #( #decoded_method_checks else )*
            {
                ::core::option::Option::None
            };
        };
        let head_method_checks: Vec<TokenStream> = methods
            .iter()
            .map(|method| {
                let bytes = method.bytes_token();
                let key = method.key_token();
                quote! {
                    if __method_bytes == #bytes {
                        ::core::option::Option::Some(#key)
                    }
                }
            })
            .collect();
        let head_method_key = quote! {
            let __method_key = #( #head_method_checks else )*
            {
                ::core::option::Option::None
            };
        };
        let maker_bounds = producer_bounds(&tasks, state_ty);
        let completion_bounds: Vec<TokenStream> = tasks
            .iter()
            .zip(task_types.iter())
            .map(|(task, task_type)| {
                let route = task.route;
                quote! {
                    <#route as ::sark::service::RouteSpec>::Kind:
                        ::sark::dispatch::Complete<'d, #route, #task_type>,
                }
            })
            .collect();
        let decode_bounds: Vec<TokenStream> = self
            .spec
            .routes
            .iter()
            .map(|route| {
                quote! {
                    <#route as ::sark::service::RouteSpec>::Kind:
                        ::sark::dispatch::DecodeRoute<#route, #state_ty>,
                }
            })
            .collect();
        let base_bounds = quote! {
            #( #route_bounds )*
            #( #maker_bounds )*
            #( #slab_bounds )*
            #( #stream_bounds )*
        };
        let pump_bounds = quote! {
            #base_bounds
            #( #completion_bounds )*
        };
        let decoded_bounds = quote! {
            #base_bounds
            #( #decode_bounds )*
        };
        let generic_def = quote! {
            <
                'env,
                'd: 'env,
                __W: ::dope_net::wire::Wire,
                #( #futures, )*
                #( #makers, )*
            >
        };
        let generic_use = quote! {
            <'env, 'd, __W, #( #futures, )* #( #makers, )*>
        };
        let dispatch_for = |index: usize, raw_params: TokenStream| {
            let route = &routes[index];
            let middleware = &wrap_before[index];
            let setup = quote! {
                if <<#route as ::sark::service::RouteSpec>::Request
                        as ::sark::service::RouteRequestImpl>::FULL
                    && !::sark::sark_core::http::scan::request_target_is_valid(
                        head.target,
                    )
                {
                    return ::sark::dispatch::ConsumeOutcome::Close(
                        ::sark::CANNED_400,
                    );
                }
                let ctx = ::sark::dispatch::Ctx::routed(
                    req_bytes,
                    head,
                    __path_end,
                );
                #middleware
                let state: &'env #state_ty = state;
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
                        this.gzip,
                        write,
                    ).dispatch::<#route, #state_ty>(
                        permit,
                        ::sark::dispatch::Matched {
                            raw_params: #raw_params,
                        },
                        state,
                    );
                };
            };
            let task = &tasks[task_slot];
            let capacity = task.capacity;
            let task_tag = &task_tags[task_slot];
            let task_field = &task_field_names[task_slot];
            let task_route = task_slot as u16;
            let matched = quote! {
                ::sark::dispatch::Matched {
                    raw_params: #raw_params,
                }
            };
            let dispatch = match task.kind {
                RouteKind::Fiber => {
                    let producer = task.producer.as_ref().expect("fiber route task producer");
                    let future = &producer.future;
                    let producer_index = syn::Index::from(producer.slot);
                    quote! {
                        {
                            let timer: &'env ::sark::Timer<'d> = *this.timer;
                            let producer = this.task_producers.#producer_index;
                            ::sark::dispatch::FiberRoute::new(
                                &ctx,
                                state,
                                timer,
                                conn,
                            ).dispatch::<#route, #future, #task_tag, _, { #capacity }>(
                                permit,
                                #matched,
                                this.#task_field.as_mut(),
                                producer,
                            )
                        }
                    }
                }
                RouteKind::Stream => quote! {
                    ::sark::dispatch::StreamRoute::new(
                        &ctx,
                        write,
                        date,
                        conn,
                    ).dispatch::<#route, #state_ty, #task_tag, { #capacity }>(
                        permit,
                        #matched,
                        this.#task_field.as_mut(),
                        state,
                    )
                },
                RouteKind::Sync => unreachable!("sync routes have no task slot"),
            };
            quote! {
                #setup
                if *this.active_tasks >= *this.task_capacity {
                    return ::sark::dispatch::ConsumeOutcome::Close(::sark::CANNED_503);
                }
                let outcome = #dispatch;
                if conn.async_state.task.is_some() {
                    conn.async_state.task_route = #task_route;
                    *this.active_tasks += 1;
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
                                &__route_path,
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
        let static_tree = StaticRoute::compile_target(static_routes);
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
            let __method = method;
            let __target = target;
            let __route_path = ::sark::service::TargetPath::new(target);
            #static_tree
            #param_dfa
            ::sark::dispatch::ConsumeOutcome::Close(::sark::CANNED_404)
        };

        let vis = &self.spec.vis;
        let prepared_ident = format_ident!("{}PreparedRequest", self.spec.name);
        let head_plan_ident = format_ident!("{}HeadPlan", self.spec.name);
        let head_route_ident = format_ident!("{}HeadRoute", self.spec.name);
        let head_selection_ident = format_ident!("{}HeadSelection", self.spec.name);
        let prepared_variants: Vec<_> = (0..routes.len())
            .map(|index| format_ident!("R{index:04}"))
            .collect();
        let prepared_variant_defs: Vec<TokenStream> = routes
            .iter()
            .zip(prepared_variants.iter())
            .map(|(route, variant)| {
                quote! {
                    #variant {
                        fields: ::sark::sark_core::http::HeadBytes,
                        raw_params: <#route as ::sark::service::RouteSpec>::RawParams,
                        raw_headers: <#route as ::sark::service::RouteSpec>::RawHeaders,
                        target: ::core::ops::Range<usize>,
                        method: ::sark::sark_core::http::Method,
                        content_length: ::core::option::Option<usize>,
                    },
                }
            })
            .collect();
        let prepared_plan_arms: Vec<TokenStream> = routes
            .iter()
            .zip(prepared_variants.iter())
            .map(|(route, variant)| {
                quote! {
                    Self::#variant { content_length, .. } => ::sark::dispatch::BodyPlan {
                        policy:
                            <<#route as ::sark::service::RouteSpec>::Request
                                as ::sark::service::RouteRequestImpl>::BODY_POLICY,
                        max_body: <#route as ::sark::service::RouteSpec>::MAX_BODY,
                        content_length: *content_length,
                    },
                }
            })
            .collect();
        let finish_for = |index: usize, raw_params: TokenStream| {
            let route = &routes[index];
            let variant = &prepared_variants[index];
            quote! {
                if <<#route as ::sark::service::RouteSpec>::Request
                        as ::sark::service::RouteRequestImpl>::FULL
                    && !::sark::sark_core::http::scan::request_target_is_valid(
                        __request_target,
                    )
                {
                    return ::core::result::Result::Err(
                        ::sark::dispatch::Decoded::Bad,
                    );
                }
                let __query_range =
                    (__path_end < __request_target.len()).then(|| {
                        (__target_range.start + __path_end + 1)
                            ..__target_range.end
                    });
                if let ::core::option::Option::Some(__query) = __query_range.clone()
                    && <<#route as ::sark::service::RouteSpec>::Request
                        as ::sark::service::RouteRequestImpl>::parse_query_raw(
                        &mut __raw_headers,
                        __head_bytes,
                        __query,
                    )
                    .is_err()
                {
                    return ::core::result::Result::Err(
                        ::sark::dispatch::Decoded::Bad,
                    );
                }
                return ::core::result::Result::Ok(#prepared_ident::#variant {
                    fields: ::sark::sark_core::http::HeadBlock::into_bytes(__fields),
                    raw_params: #raw_params,
                    raw_headers: __raw_headers,
                    target: __target_range,
                    method: __http_method,
                    content_length: __content_length,
                });
            }
        };
        let parse_content_length = |value: TokenStream| {
            quote! {
                let ::core::result::Result::Ok(__length) =
                    ::sark::sark_core::http::codec::Header::content_length(#value)
                else {
                    return ::core::result::Result::Err(::sark::dispatch::Decoded::Bad);
                };
                if __content_length.is_some_and(|__previous| __previous != __length) {
                    return ::core::result::Result::Err(::sark::dispatch::Decoded::Bad);
                }
                __content_length = ::core::option::Option::Some(__length);
            }
        };
        let set_header = |route: &syn::TypePath| {
            quote! {
                if <<#route as ::sark::service::RouteSpec>::Request
                    as ::sark::service::RouteRequestImpl>::set_header_raw(
                        &mut __raw_headers,
                        __slot,
                        &::sark::service::SliceValue::new(__head_bytes, __range),
                    )
                    .is_err()
                {
                    return ::core::result::Result::Err(
                        ::sark::dispatch::Decoded::Bad,
                    );
                }
            }
        };
        let prepare_for = |index: usize, raw_params: TokenStream| {
            let route = &routes[index];
            let finish = finish_for(index, raw_params);
            let content_length = parse_content_length(quote!(__field.value));
            let set_header = set_header(route);
            quote! {
                let mut __raw_headers =
                    <<#route as ::sark::service::RouteSpec>::RawHeaders
                        as ::core::default::Default>::default();
                let mut __content_length = ::core::option::Option::None;
                for (__field, __range) in
                    __first_regular.into_iter().chain(__field_iter)
                {
                    if __field.name.first() == ::core::option::Option::Some(&b':') {
                        return ::core::result::Result::Err(
                            ::sark::dispatch::Decoded::Bad,
                        );
                    }
                    if __field.name == #content_length_head_name {
                        #content_length
                    }
                    if let ::core::option::Option::Some(__slot) =
                        <<#route as ::sark::service::RouteSpec>::Request
                            as ::sark::service::RouteRequestImpl>::header_slot_bytes(
                                __field.name,
                            )
                    {
                        #set_header
                    }
                }
                #finish
            }
        };
        let prepare_tagged_for = |index: usize, raw_params: TokenStream| {
            let route = &routes[index];
            let finish = finish_for(index, raw_params);
            let content_length = parse_content_length(quote!(&__head_bytes[__range]));
            let set_header = set_header(route);
            quote! {
                let mut __raw_headers =
                    <<#route as ::sark::service::RouteSpec>::RawHeaders
                        as ::core::default::Default>::default();
                let mut __content_length = ::core::option::Option::None;
                for (__tag, __range) in __field_iter {
                    if __tag == ::sark::sark_core::http::HeadTag::CONTENT_LENGTH {
                        #content_length
                        continue;
                    }
                    let ::core::option::Option::Some(__slot) = __tag.user_slot() else {
                        return ::core::result::Result::Err(
                            ::sark::dispatch::Decoded::Bad,
                        );
                    };
                    let ::core::option::Option::Some(__slot) =
                        <<#route as ::sark::service::RouteSpec>::HeaderSlot
                            as ::sark::service::HeaderSlot>::from_tag(__slot)
                    else {
                        return ::core::result::Result::Err(
                            ::sark::dispatch::Decoded::Bad,
                        );
                    };
                    #set_header
                }
                #finish
            }
        };
        let param_for = |index: usize, prepared: TokenStream| {
            let entry = &self.spec.route_specs[index];
            let route = &routes[index];
            let segments = Seg::segment(&entry.path.value());
            let captures: Vec<_> = (0..segments
                .iter()
                .filter(|segment| matches!(segment, Seg::Param))
                .count())
                .map(|capture| format_ident!("__cap{}", capture))
                .collect();
            let captures = quote!(( #( #captures, )* ));
            ParamRoute {
                method: entry.meta.method,
                segs: segments,
                body: quote! {
                    let ::core::option::Option::Some(__raw) =
                        <#route as ::sark::service::RouteSpec>::from_captures(
                            &__route_path,
                            #captures,
                        )
                    else {
                        return ::core::result::Result::Err(
                            ::sark::dispatch::Decoded::NotFound,
                        );
                    };
                    #prepared
                },
            }
        };
        let decoded_param_for = |index: usize| param_for(index, prepare_for(index, quote!(__raw)));
        let planned_param_for =
            |index: usize| param_for(index, prepare_tagged_for(index, quote!(__raw)));
        let mut full_head_static_routes = Vec::new();
        let mut full_head_param_routes = Vec::new();
        for (index, entry) in self.spec.route_specs.iter().enumerate() {
            let route = &routes[index];
            let path = entry.path.value();
            if route_has_param[index] {
                full_head_param_routes.push(decoded_param_for(index));
            } else {
                let raw = quote! {
                    <<#route as ::sark::service::RouteSpec>::RawParams
                        as ::core::default::Default>::default()
                };
                full_head_static_routes.push(StaticRoute {
                    method: entry.meta.method,
                    path: path.into_bytes(),
                    body: prepare_for(index, raw),
                });
            }
        }
        let full_head_static_tree = StaticRoute::compile_target(full_head_static_routes);
        let full_head_param_dfa = ParamRoute::compile(full_head_param_routes);
        let planned_route_arms: Vec<TokenStream> = self
            .spec
            .route_specs
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let variant = &prepared_variants[index];
                let http_method = entry.meta.method.http_token();
                if route_has_param[index] {
                    let method = entry.meta.method.key_token();
                    let dfa = ParamRoute::compile(vec![planned_param_for(index)]);
                    quote! {
                        #head_route_ident::#variant => {
                            let __method = #method;
                            let __http_method = #http_method;
                            let __route_path =
                                ::sark::service::TargetPath::new(__request_target);
                            #dfa
                            ::core::result::Result::Err(
                                ::sark::dispatch::Decoded::Bad,
                            )
                        }
                    }
                } else {
                    let path_end = entry.path.value().len();
                    let route = &routes[index];
                    let raw = quote! {
                        <<#route as ::sark::service::RouteSpec>::RawParams
                            as ::core::default::Default>::default()
                    };
                    let prepared = prepare_tagged_for(index, raw);
                    quote! {
                        #head_route_ident::#variant => {
                            let __http_method = #http_method;
                            let __path_end = #path_end;
                            #prepared
                        }
                    }
                }
            })
            .collect();
        let mut head_static_routes = Vec::new();
        let mut head_param_routes = Vec::new();
        for (index, entry) in self.spec.route_specs.iter().enumerate() {
            let variant = &prepared_variants[index];
            let body = quote! {
                return ::core::option::Option::Some(#head_route_ident::#variant);
            };
            if route_has_param[index] {
                head_param_routes.push(ParamRoute {
                    method: entry.meta.method,
                    segs: Seg::segment(&entry.path.value()),
                    body,
                });
            } else {
                head_static_routes.push(StaticRoute {
                    method: entry.meta.method,
                    path: entry.path.value().into_bytes(),
                    body,
                });
            }
        }
        let head_static_tree = StaticRoute::compile_target(head_static_routes);
        let head_param_dfa = ParamRoute::compile(head_param_routes);
        let head_disposition_arms: Vec<TokenStream> = routes
            .iter()
            .zip(prepared_variants.iter())
            .map(|(route, variant)| {
                quote! {
                    #head_route_ident::#variant => {
                        let ::core::option::Option::Some(__slot) =
                            <<#route as ::sark::service::RouteSpec>::Request
                                as ::sark::service::RouteRequestImpl>::header_slot_bytes(
                                    __name,
                                )
                        else {
                            return ::sark::sark_core::http::HeadDisposition::Discard;
                        };
                        let __slot =
                            <<#route as ::sark::service::RouteSpec>::HeaderSlot
                                as ::sark::service::HeaderSlot>::into_tag(__slot);
                        match ::sark::sark_core::http::HeadTag::user(__slot) {
                            ::core::option::Option::Some(__tag) =>
                                ::sark::sark_core::http::HeadDisposition::Tagged(__tag),
                            ::core::option::Option::None =>
                                ::sark::sark_core::http::HeadDisposition::Discard,
                        }
                    }
                }
            })
            .collect();
        let head_plan = quote! {
            #[doc(hidden)]
            enum #head_route_ident {
                Pending,
                Missing,
                #( #prepared_variants, )*
            }

            #[doc(hidden)]
            #vis struct #head_plan_ident {
                method: ::core::option::Option<::sark::service::Key>,
                target: ::core::option::Option<::core::ops::Range<usize>>,
                route: #head_route_ident,
            }

            #[doc(hidden)]
            #vis struct #head_selection_ident {
                route: #head_route_ident,
                target: ::core::option::Option<::core::ops::Range<usize>>,
            }

            impl #head_plan_ident {
                #[allow(unused_variables)]
                fn select(
                    __method: ::sark::service::Key,
                    __target: &[u8],
                ) -> ::core::option::Option<#head_route_ident> {
                    if __target.first() != ::core::option::Option::Some(&b'/') {
                        return ::core::option::Option::None;
                    }
                    let __route_path = ::sark::service::TargetPath::new(__target);
                    #head_static_tree
                    #head_param_dfa
                    ::core::option::Option::None
                }

                fn resolve(&mut self, __retained: &[u8]) {
                    if !matches!(self.route, #head_route_ident::Pending) {
                        return;
                    }
                    let (
                        ::core::option::Option::Some(__method),
                        ::core::option::Option::Some(__target),
                    ) = (self.method, self.target.clone())
                    else {
                        return;
                    };
                    self.route = Self::select(__method, &__retained[__target])
                        .unwrap_or(#head_route_ident::Missing);
                }
            }

            impl ::core::default::Default for #head_plan_ident {
                fn default() -> Self {
                    Self {
                        method: ::core::option::Option::None,
                        target: ::core::option::Option::None,
                        route: #head_route_ident::Pending,
                    }
                }
            }

            impl ::sark::sark_core::http::HeadPlan for #head_plan_ident {
                type Selection = #head_selection_ident;
                type Block = ::sark::sark_core::http::PlannedFields;
                const INSPECT_DISCARDED: bool = false;

                fn disposition(
                    &mut self,
                    __name: &[u8],
                    __known: ::sark::sark_core::http::KnownHeadName,
                    __retained: &[u8],
                ) -> ::sark::sark_core::http::HeadDisposition {
                    match __known {
                        ::sark::sark_core::http::KnownHeadName::Method =>
                            return ::sark::sark_core::http::HeadDisposition::Discard,
                        ::sark::sark_core::http::KnownHeadName::Path => {
                            return ::sark::sark_core::http::HeadDisposition::Tagged(
                                ::sark::sark_core::http::HeadTag::PATH,
                            );
                        }
                        ::sark::sark_core::http::KnownHeadName::ContentLength => {
                            return ::sark::sark_core::http::HeadDisposition::Tagged(
                                ::sark::sark_core::http::HeadTag::CONTENT_LENGTH,
                            );
                        }
                        ::sark::sark_core::http::KnownHeadName::Te
                        | ::sark::sark_core::http::KnownHeadName::Regular => {}
                        _ => return ::sark::sark_core::http::HeadDisposition::Discard,
                    }
                    match self.route {
                        #( #head_disposition_arms )*
                        #head_route_ident::Pending | #head_route_ident::Missing =>
                            ::sark::sark_core::http::HeadDisposition::Discard,
                    }
                }

                fn decoded(
                    &mut self,
                    __field: ::sark::sark_core::http::Field<'_>,
                    __known: ::sark::sark_core::http::KnownHeadName,
                    __retained: &[u8],
                ) {
                    match __known {
                        ::sark::sark_core::http::KnownHeadName::Method => {
                            let __method_bytes = __field.value;
                            #head_method_key
                            self.method = __method_key;
                            self.resolve(__retained);
                        }
                        ::sark::sark_core::http::KnownHeadName::Path => {
                            if let ::core::option::Option::Some(__method) = self.method
                            {
                                self.route = Self::select(__method, __field.value)
                                    .unwrap_or(#head_route_ident::Missing);
                            }
                        }
                        _ => {}
                    }
                }

                fn committed(
                    &mut self,
                    __disposition: ::sark::sark_core::http::HeadDisposition,
                    __value: ::core::ops::Range<usize>,
                    _retained: &[u8],
                ) {
                    if __disposition == ::sark::sark_core::http::HeadDisposition::Tagged(
                            ::sark::sark_core::http::HeadTag::PATH,
                        )
                    {
                        self.target = ::core::option::Option::Some(__value);
                    }
                }

                fn finish(self) -> Self::Selection {
                    #head_selection_ident {
                        route: self.route,
                        target: self.target,
                    }
                }
            }
        };
        let dispatch_prepared_arms: Vec<TokenStream> = routes
            .iter()
            .zip(prepared_variants.iter())
            .map(|(route, variant)| {
                quote! {
                    #prepared_ident::#variant {
                        fields,
                        raw_params,
                        raw_headers,
                        target,
                        method,
                        content_length: _,
                    } => {
                        let __declared_body_len =
                            ::sark::dispatch::BodySource::body_len(&__body);
                        let __body_bytes =
                            <<<#route as ::sark::service::RouteSpec>::Request
                                as ::sark::service::RouteRequestImpl>::BodyMode
                                as ::sark::service::BodyMode>::bytes(&mut __body);
                        <<#route as ::sark::service::RouteSpec>::Kind
                            as ::sark::dispatch::DecodeRoute<#route, #state_ty>>::decode(
                            raw_params,
                            raw_headers,
                            method,
                            target,
                            fields.as_bytes(),
                            __body_bytes,
                            __declared_body_len,
                            self.state,
                            __encoder,
                        )
                    }
                }
            })
            .collect();
        let prepare_body = |dispatch: TokenStream| {
            quote! {
                let __head_bytes = __fields.as_bytes();
                let mut __http_method = ::core::option::Option::None;
                let mut __target = ::core::option::Option::None;
                let mut __field_iter = __fields.iter_with_value_ranges();
                let __first_regular = loop {
                    let ::core::option::Option::Some((__field, __range)) =
                        __field_iter.next()
                    else {
                        break ::core::option::Option::None;
                    };
                    if __field.name.first() != ::core::option::Option::Some(&b':') {
                        break ::core::option::Option::Some((__field, __range));
                    }
                    match __field.name {
                        #method_head_name => {
                            if __http_method.is_some() {
                                return ::core::result::Result::Err(
                                    ::sark::dispatch::Decoded::Bad,
                                );
                            }
                            let ::core::result::Result::Ok(__method) =
                                ::sark::sark_core::http::Method::from_bytes(__field.value)
                            else {
                                return ::core::result::Result::Err(
                                    ::sark::dispatch::Decoded::Bad,
                                );
                            };
                            __http_method = ::core::option::Option::Some(__method);
                        }
                        #path_head_name => {
                            if __target.replace(__range).is_some() {
                                return ::core::result::Result::Err(
                                    ::sark::dispatch::Decoded::Bad,
                                );
                            }
                        }
                        _ => {}
                    }
                };
                let (
                    ::core::option::Option::Some(__http_method),
                    ::core::option::Option::Some(__target_range),
                ) = (__http_method, __target)
                else {
                    return ::core::result::Result::Err(
                        ::sark::dispatch::Decoded::Bad,
                    );
                };
                let __request_target = &__head_bytes[__target_range.clone()];
                if __request_target.first() != ::core::option::Option::Some(&b'/') {
                    return ::core::result::Result::Err(
                        ::sark::dispatch::Decoded::Bad,
                    );
                }
                #dispatch
            }
        };
        let decoded_route_path = route_has_param.iter().any(|has_param| *has_param).then(|| {
            quote! {
                let __route_path = ::sark::service::TargetPath::new(__request_target);
            }
        });
        let decoded_dispatch = quote! {
            #decoded_method_key
            let ::core::option::Option::Some(__method) = decoded_method_key else {
                return ::core::result::Result::Err(
                    ::sark::dispatch::Decoded::NotFound,
                );
            };
            let __target = __request_target;
            #decoded_route_path
            #full_head_static_tree
            #full_head_param_dfa
            ::core::result::Result::Err(::sark::dispatch::Decoded::NotFound)
        };
        let planned_dispatch = quote! {
            match __planned_route {
                #( #planned_route_arms, )*
                #head_route_ident::Pending => {
                    ::core::result::Result::Err(::sark::dispatch::Decoded::Bad)
                }
                #head_route_ident::Missing => {
                    ::core::result::Result::Err(::sark::dispatch::Decoded::NotFound)
                }
            }
        };
        let prepare_full_head_body = prepare_body(decoded_dispatch);
        let prepare_planned_head_body = quote! {
            let __head_bytes = __fields.as_bytes();
            let ::core::option::Option::Some(__target_range) = __planned_target
            else {
                return ::core::result::Result::Err(::sark::dispatch::Decoded::Bad);
            };
            let __request_target = &__head_bytes[__target_range.clone()];
            if __request_target.first() != ::core::option::Option::Some(&b'/') {
                return ::core::result::Result::Err(::sark::dispatch::Decoded::Bad);
            }
            let mut __field_iter =
                __fields.iter_from(__target_range.end);
            #planned_dispatch
        };
        let decode_method = quote! {
            type Prepared = #prepared_ident;
            type Plan = #head_plan_ident;

            fn prepare_full_head(
                &self,
                __fields: ::sark::sark_core::http::DecodedFieldBlock,
            ) -> ::core::result::Result<Self::Prepared, ::sark::dispatch::Decoded> {
                #prepare_full_head_body
            }

            fn prepare_planned_head(
                &self,
                __head: ::sark::sark_core::http::PlannedHead<#head_plan_ident>,
            ) -> ::core::result::Result<Self::Prepared, ::sark::dispatch::Decoded> {
                let (
                    __fields,
                    #head_selection_ident {
                        route: __planned_route,
                        target: __planned_target,
                    },
                ) = __head.into_parts();
                #prepare_planned_head_body
            }

            fn dispatch_prepared<
                __B: ::sark::dispatch::BodySource,
                __E: ::sark::dispatch::ResponseEncoder,
            >(
                &self,
                __prepared: Self::Prepared,
                mut __body: __B,
                __encoder: &mut __E,
            ) -> ::sark::dispatch::Decoded {
                match __prepared {
                    #( #dispatch_prepared_arms, )*
                }
            }
        };
        let pump_arms: Vec<TokenStream> = tasks
            .iter()
            .map(|task| {
                let route = task.route;
                let task_type = &task_types[task.slot];
                let task_field = &task_field_names[task.slot];
                let task_route = task.slot as u16;
                quote! {
                    #task_route => {
                        let task_runner = ::sark::dispatch::TaskRunner::new(&task_date);
                        let written = task_runner.poll(
                            this.#task_field.as_mut(),
                            slot,
                            egress,
                            driver,
                            &project,
                            |output, task_slot, task_egress, task_driver, task_date, close| {
                                <<#route as ::sark::service::RouteSpec>::Kind
                                    as ::sark::dispatch::Complete<
                                        'd,
                                        #route,
                                        #task_type,
                                    >>::complete(
                                    output,
                                    task_slot,
                                    task_egress,
                                    task_driver,
                                    task_date,
                                    close,
                                )
                            },
                        );
                        if written > 0 {
                            let buffer = task_runner.write_buf(slot, egress);
                            let token = slot.token();
                            ::dope::manifold::listener::egress::SlotEgress::submit_buffered(
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
                _ => unreachable!("active task route is outside the generated route set"),
            }
            if !project(&mut slot.state.conn).async_state.has_task() {
                *this.active_tasks -= 1;
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
                    egress,
                    driver,
                    &project,
                );
            }
        } else {
            quote! {
                {
                    let mut this = self.as_mut().project();
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
                    egress,
                    driver,
                    &project,
                );
            }
        };
        let wake_body = if tasks.is_empty() {
            quote! {
                let _ = ::sark::dispatch::HeadDeadline::new(
                    self.as_ref().get_ref(),
                ).poll_proj(
                    slot,
                    egress,
                    driver,
                );
            }
        } else {
            quote! {
                if ::sark::dispatch::HeadDeadline::new(
                    self.as_ref().get_ref(),
                ).poll_proj(
                    slot,
                    egress,
                    driver,
                ) {
                    return;
                }
                let mut this = self.project();
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
                let task_field = &task_field_names[task.slot];
                let task_route = task.slot as u16;
                let task_tag = &task_tags[task.slot];
                quote! {
                    #task_route => {
                        let slab = this.#task_field.as_mut();
                        slab.remove(
                            ::sark::fiber::TaskId::<#task_tag>::from_erased(task),
                        )
                    },
                }
            })
            .collect();
        let close_body = if tasks.is_empty() {
            quote! {
                ::sark::dispatch::HeadDeadline::new(
                    self.as_ref().get_ref(),
                ).cancel_proj(
                    slot,
                );
            }
        } else {
            quote! {
                ::sark::dispatch::HeadDeadline::new(
                    self.as_ref().get_ref(),
                ).cancel_proj(
                    slot,
                );
                let mut this = self.project();
                if let ::core::option::Option::Some(task) =
                    project(&mut slot.state.conn).async_state.task.take()
                {
                    let removed = match project(&mut slot.state.conn).async_state.task_route {
                        #( #release_arms )*
                        _ => false,
                    };
                    debug_assert!(removed, "live task must be removable");
                    *this.active_tasks -= 1;
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
                ::dope::manifold::listener::state::State<__C>,
            >
        };
        quote! {
            #head_plan

            #[doc(hidden)]
            #vis enum #prepared_ident {
                #( #prepared_variant_defs )*
            }

            impl ::sark::dispatch::PreparedRequest for #prepared_ident {
                fn body_plan(&self) -> ::sark::dispatch::BodyPlan {
                    match self {
                        #( #prepared_plan_arms )*
                    }
                }
            }

            impl #generic_def #core_ident #generic_use
            where
                #pump_bounds
            {
                fn chunk_proj<__C, __PJ>(
                    self: ::core::pin::Pin<&mut Self>,
                    date: &::sark::date::Stamp,
                    slot: &mut #projection_slot,
                    bytes: &[u8],
                    egress: &mut ::dope::manifold::listener::state::EgressCtx<'_, 'd, '_>,
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
                        egress,
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
                    egress: &mut ::dope::manifold::listener::state::EgressCtx<'_, 'd, '_>,
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
                    egress: &mut ::dope::manifold::listener::state::EgressCtx<'_, 'd, '_>,
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
                    _egress: &mut ::dope::manifold::listener::state::EgressCtx<'_, 'd, '_>,
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
                #pump_bounds
            {
                fn chunk_proj<__C, __PJ>(
                    self: ::core::pin::Pin<&mut Self>,
                    slot: &mut #projection_slot,
                    bytes: &[u8],
                    egress: &mut ::dope::manifold::listener::state::EgressCtx<'_, 'd, '_>,
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
                        egress,
                        driver,
                        project,
                    )
                }

                fn send_proj<__C, __PJ>(
                    self: ::core::pin::Pin<&mut Self>,
                    slot: &mut #projection_slot,
                    project: __PJ,
                    sent: usize,
                    egress: &mut ::dope::manifold::listener::state::EgressCtx<'_, 'd, '_>,
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
                        egress,
                        driver,
                    );
                }

                fn activate_proj<__C, __PJ>(
                    self: ::core::pin::Pin<&mut Self>,
                    slot: &mut #projection_slot,
                    project: __PJ,
                    egress: &mut ::dope::manifold::listener::state::EgressCtx<'_, 'd, '_>,
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
                        egress,
                        driver,
                    );
                }

                fn close_proj<__C, __PJ>(
                    self: ::core::pin::Pin<&mut Self>,
                    slot: &mut #projection_slot,
                    project: __PJ,
                    egress: &mut ::dope::manifold::listener::state::EgressCtx<'_, 'd, '_>,
                )
                where
                    #projection_bounds
                {
                    let (mut core, _) = self.__project();
                    core.as_mut().close_proj(slot, project, egress);
                }
            }

            impl #generic_def #core_ident #generic_use
            where
                #base_bounds
            {
                #[allow(clippy::too_many_arguments)]
                fn dispatch_request<'buf>(
                    self: ::core::pin::Pin<&mut Self>,
                    permit: ::sark::dispatch::conn_state::DispatchPermit,
                    state: &'env #state_ty,
                    req_bytes: &'buf [u8],
                    head: &::sark::sark_core::http::codec::RequestLine<'buf>,
                    method: ::sark::service::Key,
                    date: &[u8; 29],
                    write: &mut [u8],
                    conn: &mut ::sark::dispatch::conn_state::ConnState,
                ) -> ::sark::dispatch::ConsumeOutcome {
                    let mut this = self.project();
                    #dispatch_body
                }
            }

            impl #generic_def ::sark::dispatch::Decode for #core_ident #generic_use
            where
                #decoded_bounds
            {
                #decode_method
            }

            impl #generic_def ::sark::dispatch::Decode for #name #generic_use
            where
                #decoded_bounds
            {
                type Prepared = #prepared_ident;
                type Plan = #head_plan_ident;

                fn prepare_full_head(
                    &self,
                    fields: ::sark::sark_core::http::DecodedFieldBlock,
                ) -> ::core::result::Result<
                    Self::Prepared,
                    ::sark::dispatch::Decoded,
                > {
                    ::sark::dispatch::Decode::prepare_full_head(
                        &self.core,
                        fields,
                    )
                }

                fn prepare_planned_head(
                    &self,
                    head: ::sark::sark_core::http::PlannedHead<Self::Plan>,
                ) -> ::core::result::Result<
                    Self::Prepared,
                    ::sark::dispatch::Decoded,
                > {
                    ::sark::dispatch::Decode::prepare_planned_head(
                        &self.core,
                        head,
                    )
                }

                fn dispatch_prepared<
                    __B: ::sark::dispatch::BodySource,
                    __E: ::sark::dispatch::ResponseEncoder,
                >(
                    &self,
                    prepared: Self::Prepared,
                    body: __B,
                    encoder: &mut __E,
                ) -> ::sark::dispatch::Decoded {
                    ::sark::dispatch::Decode::dispatch_prepared(
                        &self.core,
                        prepared,
                        body,
                        encoder,
                    )
                }
            }

            impl #generic_def ::sark::dispatch::RouteCore<'d> for #core_ident #generic_use
            where
                #base_bounds
            {
                fn timer(&self) -> &::sark::Timer<'d> {
                    self.timer
                }

                fn try_consume(
                    self: ::core::pin::Pin<&mut Self>,
                    stamp: &::sark::date::Stamp,
                    permit: ::sark::dispatch::conn_state::DispatchPermit,
                    bytes: &[u8],
                    write: &mut [u8],
                    conn: &mut ::sark::dispatch::conn_state::ConnState,
                ) -> ::sark::dispatch::ConsumeOutcome {
                    let mut method = ::core::option::Option::None;
                    let head = match
                        ::sark::sark_core::http::codec::RequestLine::parse_for::<
                            { #method_mask },
                        >(bytes, &mut method)
                    {
                        ::core::result::Result::Ok(
                            ::core::option::Option::Some(head),
                        ) => head,
                        ::core::result::Result::Ok(::core::option::Option::None) => {
                            return ::sark::dispatch::ConsumeOutcome::NeedMore {
                                permit,
                                state: ::sark::dispatch::conn_state::NeedMore::Head,
                            };
                        }
                        ::core::result::Result::Err(_) => {
                            return ::sark::dispatch::ConsumeOutcome::Close(
                                ::sark::CANNED_400,
                            );
                        }
                    };
                    let ::core::option::Option::Some(method) = method else {
                        return ::sark::dispatch::ConsumeOutcome::Close(
                            ::sark::CANNED_404,
                        );
                    };
                    let date = stamp.load();
                    let state: &'env #state_ty = self.as_ref().get_ref().state;
                    #core_ident::dispatch_request(
                        self,
                        permit,
                        state,
                        bytes,
                        &head,
                        method,
                        &date,
                        write,
                        conn,
                    )
                }
            }

            impl #generic_def ::sark::dispatch::Routing<'d> for #name #generic_use
            where
                #base_bounds
            {
                fn try_consume(
                    self: ::core::pin::Pin<&mut Self>,
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
                        permit,
                        bytes,
                        write,
                        conn,
                    )
                }
            }

            impl #generic_def ::sark::date::DateHost for #name #generic_use
            where
                #base_bounds
            {
                fn stamp(
                    self: ::core::pin::Pin<&Self>,
                ) -> ::core::pin::Pin<&::sark::date::Stamp> {
                    self.project_ref().date
                }
            }

            impl #generic_def ::sark::timer::TimerHost<'d> for #name #generic_use
            where
                #base_bounds
            {
                fn timer(&self) -> &::sark::Timer<'d> {
                    self.core.timer
                }
            }

            impl #generic_def ::sark::timer::TimerHost<'d> for #core_ident #generic_use
            where
                #base_bounds
            {
                fn timer(&self) -> &::sark::Timer<'d> {
                    self.timer
                }
            }

            impl #generic_def ::dope::manifold::listener::application::Application<'d>
                for #name #generic_use
            where
                #base_bounds
            {
                type Conn = ::sark::dispatch::conn_state::ConnState;
                type Wire = __W;
                type Hooks = ::sark::dispatch::H1Hooks;
            }
        }
    }
}

fn build_wrap_before_chain(
    wraps: &[syn::TypePath],
    method: crate::route_compiler::Method,
) -> TokenStream {
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
    let method = method.http_token();
    quote! {
        let __mw_method = #method;
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
