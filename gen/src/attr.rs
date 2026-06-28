use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, ItemFn, ItemStruct, Result};

use crate::codegen::route_spec;
use crate::codegen::runtime::Runtime;
use crate::model::{RequestKind, ResponseKind};
use crate::parse::{
    infer_request_body_kind, infer_response_body_kind, parse_struct_route_cfg, take_route_cfg_attrs,
};
use crate::util::TypeExt;

pub(super) fn attr_fn(mut fun: ItemFn) -> Result<TokenStream> {
    let name = fun.sig.ident.clone();
    let vis = fun.vis.clone();
    let hidden_fn = format_ident!("__{}_fn", name);
    let output = fun.sig.output.clone();
    let output_ty = match &output {
        syn::ReturnType::Type(_, ty) => Some((**ty).clone()),
        syn::ReturnType::Default => None,
    };
    fun.sig.ident = hidden_fn.clone();
    let (
        state_ty_attr,
        body_ty,
        raw_body,
        request_ty_attr,
        static_response,
        response_body_kind_attr,
        request_body_kind_attr,
        max_body_attr,
        head_skip,
    ) = take_route_cfg_attrs(&mut fun.attrs)?;
    let response_body_kind =
        response_body_kind_attr.unwrap_or_else(|| infer_response_body_kind(&output));
    let request_body_kind =
        request_body_kind_attr.unwrap_or_else(|| infer_request_body_kind(&fun.sig.inputs));
    let is_async = fun.sig.asyncness.is_some();
    let wants_timer = fun.sig.inputs.len() == 3;
    if fun.sig.inputs.len() != 2 && !(is_async && wants_timer) {
        return Err(syn::Error::new_spanned(
            &fun.sig.inputs,
            "function #[route(...)] requires signature `(request, state)` \
             or, for `async` handlers, `(request, state, timer)`",
        ));
    }
    if wants_timer && !is_async {
        return Err(syn::Error::new_spanned(
            &fun.sig.inputs,
            "function #[route(...)] timer argument is only valid on `async` handlers",
        ));
    }
    let request_arg_ty = match fun.sig.inputs.first() {
        Some(FnArg::Typed(pat)) => (*pat.ty).clone(),
        other => {
            return Err(syn::Error::new_spanned(
                other,
                "function #[route(...)] request argument must be typed",
            ));
        }
    };
    let state_arg_ty = match fun.sig.inputs.iter().nth(1) {
        Some(FnArg::Typed(pat)) => (*pat.ty).clone(),
        other => {
            return Err(syn::Error::new_spanned(
                other,
                "function #[route(...)] state argument must be typed",
            ));
        }
    };
    let state_arg_inner = match &state_arg_ty {
        syn::Type::Reference(r) => (*r.elem).clone(),
        other => {
            return Err(syn::Error::new_spanned(
                other,
                "function #[route(...)] state argument must be a reference (`&T`)",
            ));
        }
    };
    let request_ty = match request_ty_attr {
        Some(ty) => {
            if quote::quote!(#request_arg_ty).to_string() != quote::quote!(#ty).to_string() {
                return Err(syn::Error::new_spanned(
                    &request_arg_ty,
                    "function request argument type must match #[request(Type)]",
                ));
            }
            ty
        }
        None => request_arg_ty.clone(),
    };
    let state_ty = match state_ty_attr {
        Some(ty) => {
            if quote::quote!(#state_arg_inner).to_string() != quote::quote!(#ty).to_string() {
                return Err(syn::Error::new_spanned(
                    &state_arg_ty,
                    "function state argument type must match #[state(Type)]",
                ));
            }
            ty
        }
        None => state_arg_inner,
    };

    let state_has_lifetime = StateLifetimes::any(&state_ty);
    let state_ty = StateLifetimes::normalize(state_ty);
    let state_lt_def: TokenStream = if state_has_lifetime {
        quote! { 'state, }
    } else {
        TokenStream::new()
    };
    let state_lt_use: TokenStream = if state_has_lifetime {
        quote! { <'state> }
    } else {
        TokenStream::new()
    };

    let user_opted_borrowed = {
        let output_str = quote::quote!(#output).to_string();
        output_str.contains("'req")
    };
    {
        let request_ident = request_ty.type_ident()?;
        let request_inner = format_ident!("{}Inner", request_ident);
        if !fun
            .sig
            .generics
            .params
            .iter()
            .any(|p| matches!(p, syn::GenericParam::Lifetime(lt) if lt.lifetime.ident == "req"))
        {
            fun.sig.generics.params.insert(0, syn::parse_quote!('req));
        }
        if let Some(FnArg::Typed(pat)) = fun.sig.inputs.first_mut() {
            *pat.ty = syn::parse_quote!(#request_inner<'req>);
        }
        if state_has_lifetime {
            fun.sig.generics.params.insert(0, syn::parse_quote!('state));
            if let Some(FnArg::Typed(pat)) = fun.sig.inputs.iter_mut().nth(1) {
                *pat.ty = syn::parse_quote!(&'state #state_ty);
            }
        }
        if wants_timer && let Some(FnArg::Typed(pat)) = fun.sig.inputs.iter_mut().nth(2) {
            *pat.ty = syn::parse_quote!(::sark::Timer<'_>);
        }
    }

    let mut attrs: Vec<syn::Attribute> = Vec::new();
    attrs.push(syn::parse_quote!(#[allow(non_camel_case_types)]));
    attrs.push(syn::parse_quote!(#[state(#state_ty)]));
    if let Some(body_ty) = &body_ty {
        attrs.push(syn::parse_quote!(#[body(#body_ty)]));
    }
    if raw_body {
        attrs.push(syn::parse_quote!(#[raw_body]));
    }
    if static_response {
        attrs.push(syn::parse_quote!(#[static_response]));
    }
    match response_body_kind {
        ResponseKind::Inline => {}
        ResponseKind::Static => {
            attrs.push(syn::parse_quote!(#[response_body(Static)]));
        }
        ResponseKind::Stream => {
            attrs.push(syn::parse_quote!(#[response_body(Stream)]));
        }
    }
    match request_body_kind {
        RequestKind::Inline => {}
        RequestKind::Spilled => {
            attrs.push(syn::parse_quote!(#[request_body(Spilled)]));
        }
    }
    attrs.push(syn::parse_quote!(#[request(#request_ty)]));
    if let Some(expr) = &max_body_attr {
        attrs.push(syn::parse_quote!(#[max_body(#expr)]));
    }
    match (head_skip.date, head_skip.server) {
        (false, false) => {}
        (true, false) => attrs.push(syn::parse_quote!(#[skip(date)])),
        (false, true) => attrs.push(syn::parse_quote!(#[skip(server)])),
        (true, true) => attrs.push(syn::parse_quote!(#[skip(date, server)])),
    }
    let st: ItemStruct = syn::parse_quote! {
        #(#attrs)*
        #vis struct #name {}
    };
    let stream_response_ty = if matches!(response_body_kind, ResponseKind::Stream) {
        output_ty.as_ref().map(|ty| quote!(#ty))
    } else {
        None
    };
    let route_tokens = attr(
        st,
        fun.sig.asyncness.is_some(),
        user_opted_borrowed,
        stream_response_ty.as_ref(),
    )?;
    let timer_param = if wants_timer {
        quote! { timer: ::sark::Timer<'a>, }
    } else {
        TokenStream::new()
    };
    let timer_call_arg = if wants_timer {
        quote! { , timer }
    } else {
        TokenStream::new()
    };
    let handle_impl = {
        let request_inner = {
            let ident = request_ty.type_ident()?;
            format_ident!("{}Inner", ident)
        };
        if fun.sig.asyncness.is_some() {
            quote! {
                #[allow(unreachable_code)]
                impl #name {
                    async fn handle<#state_lt_def 'a, 'req>(
                        &self,
                        request: #request_inner<'req>,
                        state: &'a #state_ty,
                        #timer_param
                    ) #output {
                        #hidden_fn(request, state #timer_call_arg).await
                    }
                }
            }
        } else {
            quote! {
                #[allow(unreachable_code)]
                impl #name {
                    fn handle<#state_lt_def 'req>(
                        &self,
                        request: #request_inner<'req>,
                        state: &#state_ty,
                    ) #output {
                        #hidden_fn(request, state)
                    }
                }
            }
        }
    };
    let request_inner_ident = {
        let ident = request_ty.type_ident()?;
        format_ident!("{}Inner", ident)
    };
    let hidden_fn_ref = format_ident!("__{}_fn_ref", name);
    let response_body_static = matches!(response_body_kind, ResponseKind::Static);
    let response_body_stream = matches!(response_body_kind, ResponseKind::Stream);
    let stream_output_ty = output_ty
        .as_ref()
        .map(|ty| quote!(#ty))
        .unwrap_or_else(|| quote!(::sark::sark_core::http::Stream));
    let shim_output_ty = if response_body_stream {
        stream_output_ty.clone()
    } else if user_opted_borrowed && !response_body_static {
        quote!(::sark::sark_core::http::FixedResponseInner<'req>)
    } else {
        quote!(::sark::sark_core::http::ServeInner<'req>)
    };
    let shim_body = {
        let call = if fun.sig.asyncness.is_some() {
            quote! { #hidden_fn(request, state #timer_call_arg).await }
        } else {
            quote! { #hidden_fn(request, state) }
        };
        if response_body_stream {
            quote! {
                async move {
                    let __resp: #stream_output_ty = #call;
                    __resp
                }
            }
        } else if response_body_static {
            quote! {
                async move {
                    let __resp = #call;
                    ::sark::sark_core::http::IntoServeResponseStatic::into_serve_response_static(__resp)
                }
            }
        } else if user_opted_borrowed {
            quote! {
                async move {
                    let __resp = #call;
                    ::core::convert::Into::into(__resp)
                }
            }
        } else {
            quote! {
                async move {
                    let __resp = #call;
                    ::sark::sark_core::http::IntoServeResponse::into_serve_response(__resp)
                }
            }
        }
    };
    let sync_shim = if fun.sig.asyncness.is_none() {
        let hidden_fn_sync = format_ident!("{}_sync", hidden_fn_ref);
        let sync_body = if response_body_stream {
            quote! {
                let __resp: #stream_output_ty =
                    #hidden_fn(request, state);
                __resp
            }
        } else if response_body_static {
            quote! {
                let __resp = #hidden_fn(request, state);
                ::sark::sark_core::http::IntoServeResponseStatic::into_serve_response_static(__resp)
            }
        } else if user_opted_borrowed {
            quote! {
                let __resp = #hidden_fn(request, state);
                ::core::convert::Into::into(__resp)
            }
        } else {
            quote! {
                let __resp = #hidden_fn(request, state);
                ::sark::sark_core::http::IntoServeResponse::into_serve_response(__resp)
            }
        };
        quote! {
            #[allow(
                dead_code,
                non_snake_case,
                unused_variables,
                unreachable_code,
                clippy::diverging_sub_expression
            )]
            fn #hidden_fn_sync<#state_lt_def 'a, 'req>(
                request: #request_inner_ident<'req>,
                state: &'a #state_ty,
            ) -> #shim_output_ty
            where
                #state_ty: 'a,
                'req: 'a,
            {
                #sync_body
            }
        }
    } else {
        TokenStream::new()
    };
    let shim = quote! {
        #[allow(
            dead_code,
            non_snake_case,
            unused_variables,
            unreachable_code,
            clippy::manual_async_fn,
            clippy::diverging_sub_expression
        )]
        fn #hidden_fn_ref<#state_lt_def 'a, 'req>(
            request: #request_inner_ident<'req>,
            state: &'a #state_ty,
            #timer_param
        ) -> impl ::core::future::Future<
            Output = #shim_output_ty,
        > + 'a
        where
            #state_ty: 'a,
            'req: 'a,
        {
            #shim_body
        }
        #sync_shim
    };
    let fiber_impl = if fun.sig.asyncness.is_some() {
        let from_parts_call = if body_ty.is_some() {
            quote! {
                #request_inner_ident::<'static>::from_parts(params, headers, parsed_body, raw_body)
            }
        } else {
            quote! {
                #request_inner_ident::<'static>::from_parts(params, headers, raw_body)
            }
        };
        let hidden_fn_ref_turbofish = if state_has_lifetime {
            quote! { #hidden_fn_ref::<'state, 'd, 'static> }
        } else {
            quote! { #hidden_fn_ref::<'d, 'static> }
        };
        let state_outlives_d: TokenStream = if state_has_lifetime {
            quote! { 'state: 'd, }
        } else {
            TokenStream::new()
        };
        quote! {
            impl #state_lt_use ::sark::fiber::Route<#state_ty> for #name {
                fn invoke<'d>(
                    self: &'d Self,
                    params: <Self as ::sark::service::RouteSpec>::Params<'static>,
                    req: ::sark::Request,
                    headers: <Self as ::sark::service::RouteSpec>::Headers<'static>,
                    parsed_body: <Self as ::sark::service::RouteSpec>::ParsedBody<'static>,
                    state: &'d #state_ty,
                    timer: ::sark::Timer<'d>,
                ) -> ::sark::fiber::Fiber<'d, impl ::core::future::Future<
                    Output = <Self as ::sark::service::RouteSpec>::Response<'static>,
                > + 'd>
                where
                    #state_ty: 'd,
                    #state_outlives_d
                {
                    let _ = self;
                    let _ = timer;
                    let raw_body = req.into_body();
                    let request = #from_parts_call;
                    ::sark::fiber::Fiber::new(
                        #hidden_fn_ref_turbofish(request, state #timer_call_arg)
                    )
                }
            }
        }
    } else {
        TokenStream::new()
    };
    Ok(quote! {
        #[allow(unreachable_code)]
        #fun
        #route_tokens
        #handle_impl
        #shim
        #fiber_impl
    })
}

fn attr(
    st: ItemStruct,
    handler_async: bool,
    user_opted_borrowed: bool,
    stream_response_ty: Option<&TokenStream>,
) -> Result<TokenStream> {
    let is_fixed_response = user_opted_borrowed;
    let mut st = st;
    let (
        _headers,
        _queries,
        _paths,
        state_ty,
        body_ty,
        raw_body,
        request_ty,
        static_response,
        response_body_kind,
        request_body_kind,
        max_body,
        head_skip,
    ) = parse_struct_route_cfg(&mut st)?;
    let name = st.ident.clone();
    let request_ty = request_ty.ok_or_else(|| {
        syn::Error::new_spanned(
            &name,
            "#[sark_gen::handler] internal: missing synthesized #[request]",
        )
    })?;
    let request_ident = request_ty.type_ident()?;

    let request_headers_ident = format_ident!("{}Headers", request_ident);
    let request_raw_headers_ident = format_ident!("{}HeadersRaw", request_ident);
    let request_params_ident = format_ident!("{}Params", request_ident);
    let request_raw_params_ident = format_ident!("{}ParamsRaw", request_ident);
    let request_params_inner_ident = format_ident!("{}ParamsInner", request_ident);
    let request_headers_inner_ident = format_ident!("{}HeadersInner", request_ident);
    let request_header_slot_ident = format_ident!("{}HeaderSlot", request_ident);
    let header_slot_ty = quote!(#request_header_slot_ident);

    let response_ty_override = if matches!(response_body_kind, ResponseKind::Static) {
        None
    } else if matches!(response_body_kind, ResponseKind::Stream) {
        Some(
            stream_response_ty
                .cloned()
                .unwrap_or_else(|| quote!(sark_core::http::Stream)),
        )
    } else if is_fixed_response {
        Some(quote!(sark_core::http::FixedResponseInner<'__req>))
    } else {
        None
    };
    let (parsed_body_ty_tokens, parse_body_body_tokens): (
        Option<TokenStream>,
        Option<TokenStream>,
    ) = if let Some(body_ty) = &body_ty {
        let parse_body_impl = if raw_body {
            quote! {
                <#body_ty as sark::json::JsonScan>::scan_json(
                    ::core::iter::once(raw),
                )
            }
        } else {
            quote! {
                <#body_ty as sark::json::JsonDecode>::decode_json_borrowed(raw)
            }
        };
        (Some(quote!(#body_ty)), Some(parse_body_impl))
    } else {
        (None, None)
    };
    let route_spec_impl = route_spec::Cfg {
        name: &name,
        request_ident: &request_ident,
        params_ident: &request_params_ident,
        raw_params_ident: &request_raw_params_ident,
        headers_ident: &request_headers_ident,
        raw_headers_ident: &request_raw_headers_ident,
        params_has_lifetime: true,
        headers_has_lifetime: true,
        params_inner_ident: Some(&request_params_inner_ident),
        headers_inner_ident: Some(&request_headers_inner_ident),
        header_slot_ty: &header_slot_ty,
        static_response,
        response_body_kind,
        request_body_kind,
        response_ty_override: response_ty_override.as_ref(),
        parsed_body_ty: parsed_body_ty_tokens.as_ref(),
        parse_body_body: parse_body_body_tokens.as_ref(),
        max_body: max_body.as_ref(),
        head_skip,
    }
    .build();
    let body_call = if body_ty.is_some() {
        quote!(self.handle(#request_ty::from_parts(params, headers, parsed_body, raw_body), state))
    } else {
        quote!(self.handle(#request_ty::from_parts(params, headers, raw_body), state))
    };
    let map_into = if matches!(response_body_kind, ResponseKind::Static) {
        quote!(sark::http::IntoServeResponseStatic::into_serve_response_static)
    } else if matches!(response_body_kind, ResponseKind::Stream) {
        quote!(::core::convert::identity)
    } else if is_fixed_response {
        quote!(sark::http::FixedResponseInner::from)
    } else {
        quote!(sark::http::IntoServeResponse::into_serve_response)
    };
    let body_try = quote! {
        {
            let raw_body = req.into_body();
            #map_into(#body_call)
        }
    };
    let borrowed_call_request_inner_ident = format_ident!("{}Inner", request_ident);
    let borrowed_call_hidden_fn_ref = format_ident!("__{}_fn_ref", name);
    let is_stream = matches!(response_body_kind, ResponseKind::Stream);
    let runtime_tokens = Runtime::build(
        &name,
        &state_ty,
        &request_params_ident,
        &request_headers_ident,
        Some(&body_try),
        Some((
            &borrowed_call_request_inner_ident,
            &borrowed_call_hidden_fn_ref,
        )),
        !handler_async,
        is_stream,
    );

    Ok(quote! {
        #st

        #route_spec_impl

        #runtime_tokens
    })
}

struct StateLifetimes;

impl StateLifetimes {
    fn any(ty: &syn::Type) -> bool {
        struct Detect(bool);
        impl syn::visit::Visit<'_> for Detect {
            fn visit_lifetime(&mut self, _: &syn::Lifetime) {
                self.0 = true;
            }
        }
        let mut d = Detect(false);
        syn::visit::Visit::visit_type(&mut d, ty);
        d.0
    }

    fn normalize(mut ty: syn::Type) -> syn::Type {
        struct Rewrite;
        impl syn::visit_mut::VisitMut for Rewrite {
            fn visit_lifetime_mut(&mut self, lt: &mut syn::Lifetime) {
                lt.ident = format_ident!("state");
            }
        }
        syn::visit_mut::VisitMut::visit_type_mut(&mut Rewrite, &mut ty);
        ty
    }
}
