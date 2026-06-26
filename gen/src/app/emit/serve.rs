use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::super::spec::Gen;
use crate::model::RouteKind;
use crate::route_compiler::Seg;
use crate::route_compiler::param_dfa::ParamRoute;
use crate::route_compiler::static_tree::StaticRoute;

struct SlabSpec {
    kind: RouteKind,
    f_ident: syn::Ident,
    mk_ident: syn::Ident,
    route_ty: TokenStream,
    routes_idx: syn::Index,
    cap: TokenStream,
    sub_idx: usize,
}

impl SlabSpec {
    fn build(
        kind: RouteKind,
        slot: usize,
        route_ty: TokenStream,
        routes_idx: syn::Index,
        cap: TokenStream,
    ) -> Self {
        let (f_ident, mk_ident) = match kind {
            RouteKind::Fiber => (
                format_ident!("__F{:04}", slot),
                format_ident!("__MK{:04}", slot),
            ),
            RouteKind::Stream => (
                format_ident!("__SF{:04}", slot),
                format_ident!("__SMK{:04}", slot),
            ),
            RouteKind::Sync => unreachable!(),
        };
        Self {
            kind,
            f_ident,
            mk_ident,
            route_ty,
            routes_idx,
            cap,
            sub_idx: slot,
        }
    }

    fn stream_inner_ty(&self) -> TokenStream {
        let route_ty = &self.route_ty;
        quote! {
            <<#route_ty as ::sark::service::RouteSpec>::Response<'static>
                as ::sark::sark_core::http::Shape<'static>>::StreamInner
        }
    }

    fn slab_ty(&self) -> TokenStream {
        let cap = &self.cap;
        match self.kind {
            RouteKind::Fiber => {
                let f = &self.f_ident;
                quote! {
                    ::sark::fiber::Slab<
                        'd,
                        #f,
                        { #cap },
                    >
                }
            }
            RouteKind::Stream => {
                let inner = self.stream_inner_ty();
                quote! {
                    ::sark::fiber::Slab<
                        'd,
                        #inner,
                        { #cap },
                    >
                }
            }
            RouteKind::Sync => unreachable!(),
        }
    }

    fn make_fn_param_tys(&self, state_ty: &syn::Type) -> Vec<TokenStream> {
        let route_ty = &self.route_ty;
        vec![
            quote! { &'d #route_ty },
            quote! { <#route_ty as ::sark::service::RouteSpec>::Params<'static> },
            quote! { ::sark::Request },
            quote! { <#route_ty as ::sark::service::RouteSpec>::Headers<'static> },
            quote! { <#route_ty as ::sark::service::RouteSpec>::ParsedBody<'static> },
            quote! { &'d #state_ty },
            quote! { ::sark::Timer<'d> },
        ]
    }

    fn f_bound(&self) -> TokenStream {
        let f = &self.f_ident;
        let route_ty = &self.route_ty;
        quote! {
            #f: ::std::future::Future<
                Output = <#route_ty as ::sark::service::RouteSpec>::Response<'static>,
            > + 'd
        }
    }

    fn mk_bound(&self, state_ty: &syn::Type) -> TokenStream {
        let mk = &self.mk_ident;
        let f = &self.f_ident;
        let tys = self.make_fn_param_tys(state_ty);
        quote! {
            #mk: ::std::marker::Copy + 'd + ::std::ops::FnOnce( #( #tys ),* )
                -> ::sark::fiber::Fiber<'d, #f>
        }
    }

    fn route_id_lit(&self, fiber_total: usize) -> u8 {
        match self.kind {
            RouteKind::Fiber => self.sub_idx as u8,
            RouteKind::Stream => (fiber_total + self.sub_idx) as u8,
            RouteKind::Sync => unreachable!(),
        }
    }

    fn dispatch_setup(&self, state_ty: &syn::Type, wrap_before: &TokenStream) -> TokenStream {
        let route_ty = &self.route_ty;
        let routes_idx = &self.routes_idx;
        let sub_idx = syn::Index::from(self.sub_idx);
        let slab_field = match self.kind {
            RouteKind::Fiber => quote! { fiber_slabs },
            RouteKind::Stream => quote! { stream_slabs },
            RouteKind::Sync => unreachable!(),
        };
        let timer_bind = match self.kind {
            RouteKind::Fiber => quote! {
                let __fiber_timer =
                    ::sark::timer::TimerHost::timer(self);
            },
            RouteKind::Stream => quote! {},
            RouteKind::Sync => unreachable!(),
        };
        let producer = match self.kind {
            RouteKind::Fiber => quote! {
                let producer = self.fiber_producers.#sub_idx;
            },
            RouteKind::Stream => quote! {},
            RouteKind::Sync => unreachable!(),
        };
        quote! {
            #wrap_before
            let route: &'d #route_ty =
                // SAFETY: `routes` is owned by the manifold and outlives the `'d` dispatch borrow; the reborrow only extends to that lifetime.
                unsafe { &*(&self.routes.#routes_idx as *const #route_ty) };
            let state_static: &'d #state_ty = state;
            #timer_bind
            let slab = &mut self.#slab_field.#sub_idx;
            #producer
        }
    }

    fn dispatch_call(
        &self,
        state_ty: &syn::Type,
        has_param: bool,
        fiber_total: usize,
    ) -> TokenStream {
        let route_ty = &self.route_ty;
        let cap = &self.cap;
        let route_id_lit = self.route_id_lit(fiber_total);
        let raw_params_expr = if has_param {
            quote! { __raw }
        } else {
            quote! {
                <<#route_ty as ::sark::service::RouteSpec>::RawParams
                    as ::core::default::Default>::default()
            }
        };
        match self.kind {
            RouteKind::Fiber => {
                quote! {
                    ::sark::dispatch::Pipeline::route_fiber::<#route_ty, #state_ty, _, _, _, { #cap }, { #route_id_lit }>(
                        permit,
                        ::sark::dispatch::Matched { route, raw_params: #raw_params_expr },
                        slab, state_static, &ctx,
                        __fiber_timer, conn, producer,
                    )
                }
            }
            RouteKind::Stream => {
                quote! {
                    ::sark::dispatch::Pipeline::route_sync_stream::<#route_ty, #state_ty, _, { #cap }, { #route_id_lit }>(
                        permit,
                        ::sark::dispatch::Matched { route, raw_params: #raw_params_expr },
                        slab, state_static, &ctx, write, date, conn,
                    )
                }
            }
            RouteKind::Sync => unreachable!(),
        }
    }

    fn static_dispatch_body(
        &self,
        state_ty: &syn::Type,
        wrap_before: &TokenStream,
        fiber_total: usize,
    ) -> TokenStream {
        let setup = self.dispatch_setup(state_ty, wrap_before);
        let call = self.dispatch_call(state_ty, false, fiber_total);
        quote! {
            #setup
            return #call;
        }
    }

    fn param_dispatch_body(
        &self,
        state_ty: &syn::Type,
        wrap_before: &TokenStream,
        fiber_total: usize,
        caps: &TokenStream,
    ) -> TokenStream {
        let setup = self.dispatch_setup(state_ty, wrap_before);
        let call = self.dispatch_call(state_ty, true, fiber_total);
        let route_ty = &self.route_ty;
        quote! {
            #setup
            let ::std::option::Option::Some(__raw) =
                <#route_ty as ::sark::service::RouteSpec>::from_captures(
                    &ctx.slice_path,
                    #caps,
                )
            else {
                return ::sark::dispatch::ConsumeOutcome::Close(
                    ::sark::CANNED_404,
                );
            };
            return #call;
        }
    }
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
                    _key: ::sark::http::head::Known,
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
        let name = &self.spec.name;
        let ctor_mod = {
            let s = name.to_string();
            let mut snake = String::with_capacity(s.len() + 4);
            for (i, ch) in s.chars().enumerate() {
                if ch.is_uppercase() && i > 0 {
                    snake.push('_');
                }
                snake.push(ch.to_ascii_lowercase());
            }
            format_ident!("{}", snake)
        };
        let routes = &self.spec.routes;
        let route_bounds = &self.spec.route_bounds;
        let state_ty = &self.spec.state_ty;
        let mut slab_specs: Vec<SlabSpec> = Vec::new();
        let mut fiber_slot: usize = 0;
        let mut stream_slot: usize = 0;
        for (i, entry) in self.spec.route_specs.iter().enumerate() {
            let cap = match &entry.capacity {
                Some(lit) => quote!(#lit),
                None => quote!(::sark::fiber::DEFAULT_CAPACITY),
            };
            let route_ty = &self.spec.routes[i];
            let routes_idx = self.spec.idx[i].clone();
            match entry.kind {
                RouteKind::Fiber => {
                    slab_specs.push(SlabSpec::build(
                        RouteKind::Fiber,
                        fiber_slot,
                        quote! { #route_ty },
                        routes_idx,
                        cap,
                    ));
                    fiber_slot += 1;
                }
                RouteKind::Stream => {
                    slab_specs.push(SlabSpec::build(
                        RouteKind::Stream,
                        stream_slot,
                        quote! { #route_ty },
                        routes_idx,
                        cap,
                    ));
                    stream_slot += 1;
                }
                RouteKind::Sync => continue,
            }
        }
        let fiber_specs: Vec<&SlabSpec> = slab_specs
            .iter()
            .filter(|s| matches!(s.kind, RouteKind::Fiber))
            .collect();
        let stream_specs: Vec<&SlabSpec> = slab_specs
            .iter()
            .filter(|s| matches!(s.kind, RouteKind::Stream))
            .collect();
        let fiber_slab_tys: Vec<TokenStream> = fiber_specs.iter().map(|s| s.slab_ty()).collect();
        let stream_slab_tys: Vec<TokenStream> = stream_specs.iter().map(|s| s.slab_ty()).collect();
        let fiber_mk_idents: Vec<&syn::Ident> = fiber_specs.iter().map(|s| &s.mk_ident).collect();
        let fiber_invoke_exprs: Vec<TokenStream> = fiber_specs
            .iter()
            .map(|s| {
                let route_ty = &s.route_ty;
                quote! { <#route_ty as ::sark::fiber::Route<#state_ty>>::invoke }
            })
            .collect();
        let app_generics_def = if fiber_specs.is_empty() {
            quote! { <'d, __W: ::dope::transport::wire::Wire> }
        } else {
            let mut bounds: Vec<TokenStream> = Vec::new();
            for s in &fiber_specs {
                bounds.push(s.f_bound());
                bounds.push(s.mk_bound(state_ty));
            }
            quote! { < 'd, __W: ::dope::transport::wire::Wire, #( #bounds ),* > }
        };
        let fiber_slab_news: Vec<TokenStream> = fiber_specs
            .iter()
            .map(|_| quote! { ::sark::fiber::Slab::new() })
            .collect();
        let stream_slab_news: Vec<TokenStream> = stream_specs
            .iter()
            .map(|_| quote! { ::sark::fiber::Slab::new() })
            .collect();
        let new_outer_ty = if fiber_specs.is_empty() {
            quote! { super::#name<'d, __W> }
        } else {
            quote! {
                impl ::dope::manifold::listener::Application<
                    Conn = ::sark::dispatch::conn_state::ConnState,
                    Wire = __W,
                > + ::sark::date::DateHost
                  + ::sark::timer::TimerHost<'d>
                  + ::sark::dispatch::H1Project<__W> + 'd
            }
        };
        quote! {
            #vis struct #name #app_generics_def {
                fiber_slabs: ( #( #fiber_slab_tys, )* ),
                stream_slabs: ( #( #stream_slab_tys, )* ),
                fiber_producers: ( #( #fiber_mk_idents, )* ),
                date: ::core::ptr::NonNull<::sark::date::Stamp>,
                timer: ::std::cell::Cell<::std::option::Option<::sark::Timer<'d>>>,
                routes: ( #( #routes, )* ),
                state: &'d #state_ty,
                _marker: ::std::marker::PhantomData<(&'d (), __W)>,
            }

            #vis mod #ctor_mod {
                use super::*;

                pub fn new<'d, __W: ::dope::transport::wire::Wire>(state: &'d #state_ty) -> #new_outer_ty
                where #( #route_bounds )*
                {
                    super::#name {
                        fiber_slabs: ( #( #fiber_slab_news, )* ),
                        stream_slabs: ( #( #stream_slab_news, )* ),
                        fiber_producers: ( #( #fiber_invoke_exprs, )* ),
                        date: ::core::ptr::NonNull::from(::std::boxed::Box::leak(
                            ::std::boxed::Box::new(::sark::date::Stamp::new()),
                        )),
                        timer: ::std::cell::Cell::new(::std::option::Option::None),
                        routes: ( #( #routes, )* ),
                        state,
                        _marker: ::std::marker::PhantomData,
                    }
                }
            }
        }
    }

    pub(super) fn handle_bytes(&self) -> TokenStream {
        let name = &self.spec.name;
        let state_ty = &self.spec.state_ty;
        let routes = &self.spec.routes;
        let route_bounds = &self.spec.route_bounds;
        let idx = &self.spec.idx;
        let route_has_param: Vec<bool> = self
            .spec
            .route_specs
            .iter()
            .map(|entry| {
                Seg::segment(&entry.path.value())
                    .iter()
                    .any(|s| matches!(s, Seg::Param))
            })
            .collect();
        let wrap_before_chains: Vec<TokenStream> = self
            .spec
            .route_specs
            .iter()
            .map(|entry| build_wrap_before_chain(&entry.wraps))
            .collect();
        let generic_specs: Vec<SlabSpec> = {
            let mut out: Vec<SlabSpec> = Vec::new();
            let mut fiber_slot: usize = 0;
            let mut stream_slot: usize = 0;
            for (i, entry) in self.spec.route_specs.iter().enumerate() {
                let route_ty = &routes[i];
                let cap = match &entry.capacity {
                    Some(lit) => quote!(#lit),
                    None => quote!(::sark::fiber::DEFAULT_CAPACITY),
                };
                match entry.kind {
                    RouteKind::Fiber => {
                        out.push(SlabSpec::build(
                            RouteKind::Fiber,
                            fiber_slot,
                            quote! { #route_ty },
                            idx[i].clone(),
                            cap,
                        ));
                        fiber_slot += 1;
                    }
                    RouteKind::Stream => {
                        out.push(SlabSpec::build(
                            RouteKind::Stream,
                            stream_slot,
                            quote! { #route_ty },
                            idx[i].clone(),
                            cap,
                        ));
                        stream_slot += 1;
                    }
                    RouteKind::Sync => {}
                }
            }
            out
        };
        let fiber_gspecs: Vec<&SlabSpec> = generic_specs
            .iter()
            .filter(|s| matches!(s.kind, RouteKind::Fiber))
            .collect();
        let f_idents_use = if fiber_gspecs.is_empty() {
            quote! { <'d, __W> }
        } else {
            let parts: Vec<TokenStream> = fiber_gspecs
                .iter()
                .map(|s| {
                    let f = &s.f_ident;
                    let mk = &s.mk_ident;
                    quote! { #f, #mk }
                })
                .collect();
            quote! { < 'd, __W, #( #parts ),* > }
        };
        let f_idents_def = if fiber_gspecs.is_empty() {
            quote! { <'d, __W: ::dope::transport::wire::Wire> }
        } else {
            let mut bounds: Vec<TokenStream> = Vec::new();
            for s in &fiber_gspecs {
                bounds.push(s.f_bound());
                bounds.push(s.mk_bound(state_ty));
            }
            quote! { < 'd, __W: ::dope::transport::wire::Wire, #( #bounds ),* > }
        };
        let name_upper = upper_snake_case(&name.to_string());
        let cache_array_ident = quote::format_ident!("__PRESER_CACHE_{}", name_upper);
        let route_count = routes.len();
        let cache_indices: Vec<usize> = (0..route_count).collect();
        let cache_decl = quote! {
            ::std::thread_local! {
                static #cache_array_ident: [::std::cell::OnceCell<::sark::dispatch::preser::Content>; #route_count] =
                    const {
                        [
                            #( { let _ = #cache_indices; ::std::cell::OnceCell::new() }, )*
                        ]
                    };
            }
        };
        let mut static_routes: Vec<StaticRoute> = Vec::new();
        let mut param_routes: Vec<ParamRoute> = Vec::new();
        let mut fiber_slot: usize = 0;
        let fiber_total = self
            .spec
            .route_specs
            .iter()
            .filter(|e| matches!(e.kind, crate::model::RouteKind::Fiber))
            .count();
        let mut stream_slot: usize = 0;
        for (i, entry) in self.spec.route_specs.iter().enumerate() {
            let route_ty = &routes[i];
            let routes_idx = idx[i].clone();
            let has_param = route_has_param[i];
            let wrap_before = &wrap_before_chains[i];
            let path = entry.path.value();
            let segs = if has_param {
                Seg::segment(&path)
            } else {
                Vec::new()
            };
            let caps_tuple: TokenStream = {
                let caps: Vec<proc_macro2::Ident> =
                    (0..segs.iter().filter(|s| matches!(s, Seg::Param)).count())
                        .map(|n| format_ident!("__cap{}", n))
                        .collect();
                quote! { ( #( #caps, )* ) }
            };
            if matches!(entry.kind, RouteKind::Fiber) || matches!(entry.kind, RouteKind::Stream) {
                let cap = match &entry.capacity {
                    Some(lit) => quote!(#lit),
                    None => quote!(::sark::fiber::DEFAULT_CAPACITY),
                };
                let spec = match entry.kind {
                    RouteKind::Fiber => SlabSpec::build(
                        RouteKind::Fiber,
                        fiber_slot,
                        quote! { #route_ty },
                        routes_idx,
                        cap,
                    ),
                    RouteKind::Stream => SlabSpec::build(
                        RouteKind::Stream,
                        stream_slot,
                        quote! { #route_ty },
                        routes_idx,
                        cap,
                    ),
                    RouteKind::Sync => unreachable!(),
                };
                if has_param {
                    param_routes.push(ParamRoute {
                        method: entry.meta.method,
                        segs,
                        body: spec.param_dispatch_body(
                            state_ty,
                            wrap_before,
                            fiber_total,
                            &caps_tuple,
                        ),
                    });
                } else {
                    static_routes.push(StaticRoute {
                        method: entry.meta.method,
                        path: path.into_bytes(),
                        body: spec.static_dispatch_body(state_ty, wrap_before, fiber_total),
                    });
                }
                match entry.kind {
                    RouteKind::Fiber => fiber_slot += 1,
                    RouteKind::Stream => stream_slot += 1,
                    _ => {}
                }
            } else if has_param {
                param_routes.push(ParamRoute {
                    method: entry.meta.method,
                    segs,
                    body: quote! {
                        #wrap_before
                        let ::std::option::Option::Some(__raw) =
                            <#route_ty as ::sark::service::RouteSpec>::from_captures(
                                &ctx.slice_path,
                                #caps_tuple,
                            )
                        else {
                            return ::sark::dispatch::ConsumeOutcome::Close(
                                ::sark::CANNED_404,
                            );
                        };
                        return #cache_array_ident.with(|arr| {
                            ::sark::dispatch::Pipeline::route_manifold::<#route_ty, #state_ty>(
                                permit,
                                ::sark::dispatch::Matched {
                                    route: &self.routes.#routes_idx,
                                    raw_params: __raw,
                                },
                                state, &ctx, date,
                                ::sark::dispatch::preser::Slot::new(&arr[#routes_idx]),
                                write,
                            )
                        });
                    },
                });
            } else {
                static_routes.push(StaticRoute {
                    method: entry.meta.method,
                    path: path.into_bytes(),
                    body: quote! {
                        #wrap_before
                        return #cache_array_ident.with(|arr| {
                            ::sark::dispatch::Pipeline::route_manifold::<#route_ty, #state_ty>(
                                permit,
                                ::sark::dispatch::Matched {
                                    route: &self.routes.#routes_idx,
                                    raw_params:
                                        <<#route_ty as ::sark::service::RouteSpec>::RawParams
                                            as ::core::default::Default>::default(),
                                },
                                state, &ctx, date,
                                ::sark::dispatch::preser::Slot::new(&arr[#routes_idx]),
                                write,
                            )
                        });
                    },
                });
            }
        }
        let param_dfa = ParamRoute::compile(param_routes);
        let static_tree = StaticRoute::compile(static_routes);

        let mut agnostic_static_routes: Vec<StaticRoute> = Vec::new();
        for (i, entry) in self.spec.route_specs.iter().enumerate() {
            if route_has_param[i] {
                continue;
            }
            let route_ty = &routes[i];
            let routes_idx = idx[i].clone();
            let path = entry.path.value();
            let body = match entry.kind {
                crate::model::RouteKind::Sync => quote! {
                    let mut __rh = <<#route_ty as ::sark::service::RouteSpec>::RawHeaders
                        as ::core::default::Default>::default();
                    for &(__hn, ref __hr) in __headers {
                        if let ::std::option::Option::Some(__slot) =
                            <<#route_ty as ::sark::service::RouteSpec>::Request
                                as ::sark::service::RouteRequestImpl>::header_slot_bytes(__hn)
                        {
                            if <<#route_ty as ::sark::service::RouteSpec>::Request
                                as ::sark::service::RouteRequestImpl>::set_header_raw(
                                &mut __rh,
                                __slot,
                                &::sark::service::SliceValue::new(
                                    __head_bytes,
                                    ::core::clone::Clone::clone(__hr),
                                ),
                            )
                            .is_err()
                            {
                                return ::sark::dispatch::Decoded::Bad;
                            }
                        }
                    }
                    let __raw_params = <<#route_ty as ::sark::service::RouteSpec>::RawParams
                        as ::core::default::Default>::default();
                    return match ::sark::dispatch::Pipeline::build_and_invoke::<#route_ty, #state_ty>(
                        &self.routes.#routes_idx,
                        __raw_params,
                        __rh,
                        ::core::clone::Clone::clone(&__http_method),
                        0..0,
                        __head_bytes,
                        __body_bytes,
                        self.state,
                    ) {
                        ::std::result::Result::Ok(__resp) => {
                            ::sark::dispatch::ResponseEncoder::emit(
                                __encoder,
                                ::sark::sark_core::http::Shape::status(&__resp),
                                ::core::convert::AsRef::as_ref(
                                    &::sark::sark_core::http::Shape::headers_wire(&__resp),
                                ),
                                ::sark::sark_core::http::Shape::body_bytes(&__resp),
                            );
                            ::sark::dispatch::Decoded::Emitted
                        }
                        ::std::result::Result::Err(_) => ::sark::dispatch::Decoded::Bad,
                    };
                },
                _ => quote! {
                    return ::sark::dispatch::Decoded::Unsupported;
                },
            };
            agnostic_static_routes.push(StaticRoute {
                method: entry.meta.method,
                path: path.into_bytes(),
                body,
            });
        }
        let agnostic_static_tree = StaticRoute::compile(agnostic_static_routes);
        let decode_dispatch_method = quote! {
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
                #agnostic_static_tree
                ::sark::dispatch::Decoded::NotFound
            }
        };
        let ctx_build = if static_tree.is_empty() && param_dfa.is_empty() {
            quote! { let _ = method_key; }
        } else {
            quote! {
                let ctx = ::sark::dispatch::Ctx::parse_with_key(req_bytes, head, method_key);
            }
        };
        let static_method_path = if static_tree.is_empty() {
            quote! {}
        } else {
            quote! {
                let __method = method_key;
                let __path = ctx.slice_path.bytes();
            }
        };
        let dispatch_body = quote! {
            let __target = head.target;
            if __target.first() != ::std::option::Option::Some(&b'/') {
                return ::sark::dispatch::ConsumeOutcome::Close(
                    if __target == b"*" {
                        ::sark::CANNED_404
                    } else {
                        ::sark::CANNED_400
                    },
                );
            }
            #ctx_build
            #static_method_path
            #static_tree
            #param_dfa
            ::sark::dispatch::ConsumeOutcome::Close(::sark::CANNED_404)
        };

        let mut on_wake_arms: Vec<TokenStream> = Vec::new();
        let mut stream_pump_arms: Vec<TokenStream> = Vec::new();
        let mut on_close_arms: Vec<TokenStream> = Vec::new();
        {
            let mut fb_slot: usize = 0;
            let mut st_slot: usize = 0;
            for (i, entry) in self.spec.route_specs.iter().enumerate() {
                match entry.kind {
                    RouteKind::Fiber => {
                        let route_ty = &routes[i];
                        let slot_lit = fb_slot as u8;
                        let slab_idx = syn::Index::from(fb_slot);
                        on_close_arms.push(quote! {
                            #slot_lit => {
                                if let ::std::option::Option::Some((_, __tok)) =
                                    state.async_state.pending_wake.take()
                                {
                                    self.fiber_slabs.#slab_idx.release(__tok);
                                }
                            }
                        });
                        on_wake_arms.push(quote! {
                            #slot_lit => {
                                if let ::std::option::Option::Some(__resp) =
                                    ::sark::dispatch::Pipeline::fiber_wake_proj(
                                        &mut self.fiber_slabs.#slab_idx, slot, driver, &project,
                                    )
                                {
                                    let __date = *::sark::date::DateHost::date_stamp(self).buf();
                                    let __deferred_close =
                                        project(&mut slot.state.conn).deferred_close;
                                    ::sark::dispatch::Pipeline::finish_pending::<#route_ty, _, _>(
                                        __resp, slot, aux, driver, &__date, __deferred_close,
                                    );
                                    project(&mut slot.state.conn).recv.unfreeze();
                                }
                            }
                        });
                        fb_slot += 1;
                    }
                    RouteKind::Stream => {
                        let slot_lit = (fiber_total + st_slot) as u8;
                        let slab_idx = syn::Index::from(st_slot);
                        on_close_arms.push(quote! {
                            #slot_lit => {
                                if let ::std::option::Option::Some((_, __tok)) =
                                    state.async_state.stream_slot.take()
                                {
                                    self.stream_slabs.#slab_idx.release(__tok);
                                }
                            }
                        });
                        let pump = quote! {
                            let __written = ::sark::dispatch::Pipeline::stream_coalesce_proj(
                                &mut self.stream_slabs.#slab_idx,
                                slot,
                                aux,
                                driver,
                                &project,
                            );
                            if __written > 0 {
                                let __buf = ::sark::dispatch::Pipeline::reborrow_write_buf(slot, aux);
                                let __ud = slot.token();
                                slot.submit_buffered(__buf, __written, __ud, driver);
                            }
                        };
                        stream_pump_arms.push(quote! {
                            #slot_lit => { #pump }
                        });
                        on_wake_arms.push(quote! {
                            #slot_lit => {
                                if !slot.core.is_send_inflight() {
                                    #pump
                                }
                            }
                        });
                        st_slot += 1;
                    }
                    RouteKind::Sync => {}
                }
            }
        }

        let on_send_complete_body = if stream_pump_arms.is_empty() {
            quote! {
                ::sark::dispatch::pipeline::Pipeline::on_send_complete_proj(
                    self, sent, slot, aux, driver, &project,
                );
            }
        } else {
            quote! {
                if let ::std::option::Option::Some(route_id) =
                    project(&mut slot.state.conn).async_state.stream_slot.as_ref().map(|__p| __p.0)
                {
                    match route_id {
                        #( #stream_pump_arms )*
                        _ => {}
                    }
                    if project(&mut slot.state.conn).async_state.stream_slot.is_some() {
                        return;
                    }
                }
                ::sark::dispatch::pipeline::Pipeline::on_send_complete_proj(
                    self, sent, slot, aux, driver, &project,
                );
            }
        };

        let proj_bound = quote! {
            __C: ::core::default::Default + 'static,
            __PJ: ::core::ops::Fn(&mut __C) -> &mut ::sark::dispatch::conn_state::ConnState,
        };
        let proj_slot_ty = quote! {
            ::dope::transport::link::Slot<__W, ::dope::manifold::listener::State<__C>>
        };
        let on_wake_proj_body = quote! {
            if ::sark::dispatch::pipeline::Pipeline::poll_head_deadline_proj(
                self, slot, aux, driver, &project,
            ) {
                return;
            }
            let route_id = match project(&mut slot.state.conn).async_state.wake_route_id() {
                ::std::option::Option::Some(__id) => __id,
                ::std::option::Option::None => return,
            };
            match route_id {
                #( #on_wake_arms )*
                _ => {}
            }
        };
        let on_close_proj_body = quote! {
            ::sark::dispatch::pipeline::Pipeline::cancel_head_deadline_proj(self, slot, &project);
            let state = project(&mut slot.state.conn);
            let route_id = match state.async_state.wake_route_id() {
                ::std::option::Option::Some(__id) => __id,
                ::std::option::Option::None => return,
            };
            match route_id {
                #( #on_close_arms )*
                _ => {}
            }
        };

        quote! {
            #cache_decl

            impl #f_idents_def ::sark::dispatch::H1Project<__W> for #name #f_idents_use
            where #( #route_bounds )* {
                fn on_chunk_proj<__C, __PJ>(
                    &mut self,
                    slot: &mut #proj_slot_ty,
                    bytes: &[u8],
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::Driver,
                    project: __PJ,
                ) -> bool where #proj_bound {
                    ::sark::dispatch::pipeline::Pipeline::run_proj(
                        self, bytes, slot, aux, driver, project,
                    )
                }

                #[allow(clippy::too_many_arguments)]
                fn on_send_proj<__C, __PJ>(
                    &mut self,
                    slot: &mut #proj_slot_ty,
                    project: __PJ,
                    sent: usize,
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::Driver,
                ) where #proj_bound {
                    #on_send_complete_body
                }

                fn on_wake_proj<__C, __PJ>(
                    &mut self,
                    slot: &mut #proj_slot_ty,
                    project: __PJ,
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::Driver,
                ) where #proj_bound {
                    #on_wake_proj_body
                }

                fn on_close_proj<__C, __PJ>(
                    &mut self,
                    slot: &mut #proj_slot_ty,
                    project: __PJ,
                    _aux: &mut ::dope::manifold::listener::Aux,
                ) where #proj_bound {
                    #on_close_proj_body
                }
            }

            impl #f_idents_def #name #f_idents_use where #( #route_bounds )* {
                #[allow(clippy::too_many_arguments)]
                pub fn dispatch_request<'buf>(
                    &mut self,
                    permit: ::sark::dispatch::conn_state::DispatchPermit,
                    state: &'d #state_ty,
                    req_bytes: &'buf [u8],
                    head: &::sark::framer::ParsedHead<'buf>,
                    method_key: ::sark::service::Key,
                    date: &[u8; 29],
                    write: &mut [u8],
                    conn: &mut ::sark::dispatch::conn_state::ConnState,
                ) -> ::sark::dispatch::ConsumeOutcome {
                    #dispatch_body
                }
            }

            impl #f_idents_def ::sark::dispatch::Decode for #name #f_idents_use where #( #route_bounds )* {
                #decode_dispatch_method
            }

            impl #f_idents_def ::sark::dispatch::Routing for #name #f_idents_use where #( #route_bounds )* {
                fn try_consume(
                    &mut self,
                    permit: ::sark::dispatch::conn_state::DispatchPermit,
                    bytes: &[u8],
                    write: &mut [u8],
                    conn: &mut ::sark::dispatch::conn_state::ConnState,
                ) -> ::sark::dispatch::ConsumeOutcome {
                    let ::std::option::Option::Some(__fused) =
                        ::sark::framer::Http::parse_head_fused(bytes)
                    else {
                        return ::sark::dispatch::ConsumeOutcome::NeedMore { permit, content_length: ::std::option::Option::None };
                    };
                    let head = __fused.head;
                    let method_key = __fused.method_key;
                    let date = *::sark::date::DateHost::date_stamp(self).buf();
                    let state_ref: &'d #state_ty = self.state;
                    Self::dispatch_request(
                        self, permit, state_ref, bytes, &head, method_key, &date, write, conn,
                    )
                }
            }

            impl #f_idents_def ::sark::date::DateHost for #name #f_idents_use where #( #route_bounds )* {
                fn date_stamp(&self) -> &::sark::date::Stamp {
                    // SAFETY: `date` points at a per-core leaked `Stamp` that lives for the process; never moved, never freed.
                    unsafe { self.date.as_ref() }
                }
            }

            impl #f_idents_def ::sark::timer::TimerHost<'d> for #name #f_idents_use where #( #route_bounds )* {
                fn timer_cell(
                    &self,
                ) -> &::std::cell::Cell<::std::option::Option<::sark::Timer<'d>>> {
                    &self.timer
                }
            }

            impl #f_idents_def ::dope::manifold::listener::Application for #name #f_idents_use where #( #route_bounds )* {
                type Conn = ::sark::dispatch::conn_state::ConnState;
                type Wire = __W;

                fn on_chunk(
                    &mut self,
                    slot: &mut ::dope::transport::link::Slot<
                        Self::Wire,
                        ::dope::manifold::listener::State<Self::Conn>,
                    >,
                    chunk: ::dope::transport::wire::RecvChunk<'_>,
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::Driver,
                ) -> ::dope::manifold::Outcome {
                    let bytes = chunk.as_slice();
                    let overrun = ::sark::dispatch::pipeline::Pipeline::run(self, bytes, slot, aux, driver);
                    if overrun {
                        ::dope::manifold::Outcome::Overrun
                    } else {
                        ::dope::manifold::Outcome::Ok
                    }
                }

                fn on_send(
                    &mut self,
                    slot: &mut ::dope::transport::link::Slot<
                        Self::Wire,
                        ::dope::manifold::listener::State<Self::Conn>,
                    >,
                    sent: usize,
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::Driver,
                ) {
                    <Self as ::sark::dispatch::H1Project<__W>>::on_send_proj(
                        self, slot, ::sark::dispatch::identity_mut, sent, aux, driver,
                    );
                }

                fn on_wake(
                    &mut self,
                    slot: &mut ::dope::transport::link::Slot<
                        Self::Wire,
                        ::dope::manifold::listener::State<Self::Conn>,
                    >,
                    aux: &mut ::dope::manifold::listener::Aux,
                    driver: &mut ::dope::Driver,
                ) {
                    <Self as ::sark::dispatch::H1Project<__W>>::on_wake_proj(
                        self, slot, ::sark::dispatch::identity_mut, aux, driver,
                    );
                }

                fn on_close(
                    &mut self,
                    slot: &mut ::dope::transport::link::Slot<
                        Self::Wire,
                        ::dope::manifold::listener::State<Self::Conn>,
                    >,
                    _aux: &mut ::dope::manifold::listener::Aux,
                ) {
                    <Self as ::sark::dispatch::H1Project<__W>>::on_close_proj(
                        self, slot, ::sark::dispatch::identity_mut, _aux,
                    );
                }
            }
        }
    }
}

fn upper_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    let mut prev_lower = false;
    for ch in s.chars() {
        if ch.is_ascii_uppercase() {
            if prev_lower {
                out.push('_');
            }
            out.push(ch);
            prev_lower = false;
        } else if ch == '_' {
            out.push('_');
            prev_lower = false;
        } else {
            out.extend(ch.to_uppercase());
            prev_lower = true;
        }
    }
    out
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
