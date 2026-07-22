use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, ItemFn, Result};

use crate::codegen::route_spec;
use crate::parse::{RouteConfig, take_route_config};
use crate::util::TypeExt;

pub(super) fn attr_fn(mut fun: ItemFn) -> Result<TokenStream> {
    fun.modifiers.require_empty()?;
    if let syn::Safety::Unsafe(unsafe_token) = &fun.sig.safety {
        return Err(syn::Error::new_spanned(
            unsafe_token,
            "#[sark_gen::handler] does not support unsafe functions",
        ));
    }
    if fun.sig.inputs.len() == 1 {
        let request_ident =
            format_ident!("__Sark{}Request", upper_camel(&fun.sig.ident.to_string()));
        let request = crate::request::Mode::empty().expand(syn::parse_quote! {
            struct #request_ident {}
        })?;
        fun.sig
            .inputs
            .insert(0, syn::parse_quote!(_request: #request_ident));
        let handler = attr_fn(fun)?;
        return Ok(quote! {
            #request
            #handler
        });
    }
    let name = fun.sig.ident.clone();
    let vis = fun.vis.clone();
    let hidden_fn = format_ident!("__{}_fn", name);
    let output_ty = match &fun.sig.output {
        syn::ReturnType::Type(_, ty) => (**ty).clone(),
        syn::ReturnType::Default => syn::parse_quote!(()),
    };
    fun.sig.ident = hidden_fn.clone();

    let RouteConfig {
        static_response,
        max_body,
        head_skip,
    } = take_route_config(&mut fun.attrs)?;
    let is_async = fun.sig.asyncness.is_some();
    let wants_timer = fun.sig.inputs.len() == 3;

    if is_async && has_non_static_lifetime(&output_ty) {
        return Err(syn::Error::new_spanned(
            &output_ty,
            "async handler responses must own request-derived data",
        ));
    }
    if fun.sig.inputs.len() != 2 && !(is_async && wants_timer) {
        return Err(syn::Error::new_spanned(
            &fun.sig.inputs,
            "#[sark_gen::handler] requires `(state)`, `(request, state)`, or async `(request, state, timer)`",
        ));
    }
    if wants_timer && !is_async {
        return Err(syn::Error::new_spanned(
            &fun.sig.inputs,
            "#[sark_gen::handler] timer argument is only valid on `async` handlers",
        ));
    }

    let request_arg_ty = match fun.sig.inputs.first() {
        Some(FnArg::Typed(pat)) => (*pat.ty).clone(),
        other => {
            return Err(syn::Error::new_spanned(
                other,
                "#[sark_gen::handler] request argument must be typed",
            ));
        }
    };
    let state_arg_ty = match fun.sig.inputs.iter().nth(1) {
        Some(FnArg::Typed(pat)) => (*pat.ty).clone(),
        other => {
            return Err(syn::Error::new_spanned(
                other,
                "#[sark_gen::handler] state argument must be typed",
            ));
        }
    };
    let state_arg_inner = match &state_arg_ty {
        syn::Type::Reference(reference) => (*reference.elem).clone(),
        other => {
            return Err(syn::Error::new_spanned(
                other,
                "#[sark_gen::handler] state argument must be a reference (`&T`)",
            ));
        }
    };

    let request_ty = request_arg_ty;
    let state_ty = state_arg_inner;

    let request_ident = request_ty.type_ident()?;
    let request_inner_ident = format_ident!("{}View", request_ident);
    let request_raw_headers_ident = format_ident!("{}HeadersRaw", request_ident);
    let request_raw_params_ident = format_ident!("{}ParamsRaw", request_ident);
    let request_params_inner_ident = format_ident!("{}Params", request_ident);
    let request_headers_inner_ident = format_ident!("{}Headers", request_ident);
    let request_header_slot_ident = format_ident!("{}HeaderSlot", request_ident);
    let header_slot_ty = quote!(#request_header_slot_ident);

    let state_has_lifetime = StateLifetimes::any(&state_ty);
    let state_ty_state = StateLifetimes::normalize_to(state_ty.clone(), "state");
    let state_ty_d = StateLifetimes::normalize_to(state_ty.clone(), "d");
    let state_lt_use = state_has_lifetime.then(|| quote!(<'state>));
    let state_outlives = state_has_lifetime.then(|| quote!('state: 'a,));
    let hidden_state_ty = if is_async {
        &state_ty_d
    } else {
        &state_ty_state
    };

    if !fun
        .sig
        .generics
        .params
        .iter()
        .any(|param| matches!(param, syn::GenericParam::Lifetime(lt) if lt.lifetime.ident == "req"))
    {
        fun.sig.generics.params.insert(0, syn::parse_quote!('req));
    }
    if is_async
        && !fun.sig.generics.params.iter().any(
            |param| matches!(param, syn::GenericParam::Lifetime(lt) if lt.lifetime.ident == "d"),
        )
    {
        fun.sig.generics.params.insert(0, syn::parse_quote!('d));
    }
    if state_has_lifetime && !is_async {
        fun.sig.generics.params.insert(0, syn::parse_quote!('state));
    }
    if let Some(FnArg::Typed(pat)) = fun.sig.inputs.first_mut() {
        *pat.ty = syn::parse_quote!(#request_inner_ident<'req>);
    }
    if let Some(FnArg::Typed(pat)) = fun.sig.inputs.iter_mut().nth(1) {
        *pat.ty = if is_async {
            syn::parse_quote!(&'req #hidden_state_ty)
        } else {
            syn::parse_quote!(&#hidden_state_ty)
        };
    }
    if wants_timer && let Some(FnArg::Typed(pat)) = fun.sig.inputs.iter_mut().nth(2) {
        *pat.ty = syn::parse_quote!(&'req ::sark::Timer<'d>);
    }
    if is_async {
        let where_clause = fun.sig.generics.make_where_clause();
        where_clause
            .predicates
            .push(syn::parse_quote!(#hidden_state_ty: 'req));
        where_clause.predicates.push(syn::parse_quote!('d: 'req));
        fun.attrs.push(syn::parse_quote!(#[::sark::fiber_fn('d)]));
    }

    let parsed_body_ty = quote! {
        <#request_ident as sark::service::RouteRequestImpl>::ParsedBody<'__req>
    };
    let parse_body = quote! {
        <#request_ident as sark::service::RouteRequestImpl>::parse_body(raw)
    };

    let output_ty_req = StateLifetimes::normalize_to(output_ty.clone(), "__req");
    let output_ty_static = StateLifetimes::normalize_to(output_ty.clone(), "static");
    let kind_ty = if is_async {
        quote!(sark::service::manifold::NativeFiber)
    } else {
        quote! {
            <#output_ty_static as sark::service::manifold::NativeResponse<'static>>::Kind
        }
    };
    let native_response_ty = (!is_async).then_some(&output_ty_req);
    let native_response_ty_static = (!is_async).then_some(&output_ty_static);

    let route_spec_impl = route_spec::Config {
        name: &name,
        request_ident: &request_ident,
        raw_params_ident: &request_raw_params_ident,
        raw_headers_ident: &request_raw_headers_ident,
        params_inner_ident: &request_params_inner_ident,
        headers_inner_ident: &request_headers_inner_ident,
        header_slot_ty: &header_slot_ty,
        static_response,
        kind_ty: &kind_ty,
        native_response_ty,
        native_response_ty_static,
        async_response_ty: is_async.then_some(&output_ty),
        parsed_body_ty: Some(&parsed_body_ty),
        parse_body_body: Some(&parse_body),
        max_body: max_body.as_ref(),
        head_skip,
    }
    .build();

    let native_impl = if is_async {
        TokenStream::new()
    } else {
        quote! {
            impl #state_lt_use ::sark::service::manifold::Route<#state_ty_state> for #name {
                fn invoke<'req, 'a>(
                    &'a self,
                    params: <Self as ::sark::service::RouteSpec>::Params<'req>,
                    req: &::sark::request::Ref<'req>,
                    headers: <Self as ::sark::service::RouteSpec>::Headers<'req>,
                    parsed_body: <Self as ::sark::service::RouteSpec>::ParsedBody<'req>,
                    state: &'a #state_ty_state,
                ) -> <Self as ::sark::service::RouteSpec>::Response<'req>
                where
                    'req: 'a,
                    #state_outlives
                {
                    let request = #request_inner_ident::<'req>::from_parts(
                        params,
                        headers,
                        parsed_body,
                        req,
                    );
                    let response = #hidden_fn(request, state);
                    ::sark::service::manifold::NativeResponse::into_route_response(response)
                }
            }
        }
    };

    let timer_call = wants_timer.then(|| quote!(, timer));
    let borrowed_request = quote! {
        #request_inner_ident::<'req>::from_parts(
            params,
            headers,
            parsed_body,
            &req,
        )
    };
    let task_impl = if is_async {
        quote! {
            impl<'d> ::sark::service::manifold::TaskRoute<'d, #state_ty_d> for #name {
                fn invoke_task<'req>(
                    &'req self,
                    params: <Self as ::sark::service::RouteSpec>::Params<'req>,
                    req: ::sark::request::Ref<'req>,
                    headers: <Self as ::sark::service::RouteSpec>::Headers<'req>,
                    parsed_body: <Self as ::sark::service::RouteSpec>::ParsedBody<'req>,
                    state: &'req #state_ty_d,
                    timer: &'req ::sark::Timer<'d>,
                ) -> impl ::sark::fiber::Fiber<'d, Output = #output_ty> + 'req
                where
                    #state_ty_d: 'req,
                    'd: 'req,
                {
                    let _ = self;
                    let request = #borrowed_request;
                    #hidden_fn(request, state #timer_call)
                }
            }
        }
    } else {
        quote! {
            impl<'d> ::sark::service::manifold::TaskRoute<'d, #state_ty_d> for #name {
                fn invoke_task<'req>(
                    &'req self,
                    _params: <Self as ::sark::service::RouteSpec>::Params<'req>,
                    _req: ::sark::request::Ref<'req>,
                    _headers: <Self as ::sark::service::RouteSpec>::Headers<'req>,
                    _parsed_body: <Self as ::sark::service::RouteSpec>::ParsedBody<'req>,
                    _state: &'req #state_ty_d,
                    _timer: &'req ::sark::Timer<'d>,
                ) -> impl ::sark::fiber::Fiber<'d, Output = ()> + 'req
                where
                    #state_ty_d: 'req,
                    'd: 'req,
                {
                    ::sark::service::manifold::ready()
                }
            }
        }
    };

    Ok(quote! {
        #[allow(unreachable_code)]
        #fun

        #[allow(non_camel_case_types)]
        #vis struct #name;

        #route_spec_impl
        #native_impl
        #task_impl
    })
}

fn upper_camel(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for part in value.split('_').filter(|part| !part.is_empty()) {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            output.extend(first.to_uppercase());
            output.extend(chars);
        }
    }
    output
}

fn has_non_static_lifetime(ty: &syn::Type) -> bool {
    struct Detect(bool);

    impl<'ast> syn::visit::Visit<'ast> for Detect {
        fn visit_lifetime(&mut self, lifetime: &'ast syn::Lifetime) {
            self.0 |= lifetime.ident != "static";
        }

        fn visit_type_reference(&mut self, reference: &'ast syn::TypeReference) {
            self.0 |= reference
                .lifetime
                .as_ref()
                .is_none_or(|lifetime| lifetime.ident != "static");
            syn::visit::visit_type_reference(self, reference);
        }
    }

    let mut detect = Detect(false);
    syn::visit::Visit::visit_type(&mut detect, ty);
    detect.0
}

struct StateLifetimes;

impl StateLifetimes {
    fn any(ty: &syn::Type) -> bool {
        struct Detect(bool);
        impl<'ast> syn::visit::Visit<'ast> for Detect {
            fn visit_lifetime(&mut self, _lt: &'ast syn::Lifetime) {
                self.0 = true;
            }
        }
        let mut detect = Detect(false);
        syn::visit::Visit::visit_type(&mut detect, ty);
        detect.0
    }

    fn normalize_to(mut ty: syn::Type, ident: &str) -> syn::Type {
        struct Rewrite<'a>(&'a str);
        impl syn::visit_mut::VisitMut for Rewrite<'_> {
            fn visit_lifetime_mut(&mut self, lifetime: &mut syn::Lifetime) {
                lifetime.ident = format_ident!("{}", self.0);
            }
        }
        syn::visit_mut::VisitMut::visit_type_mut(&mut Rewrite(ident), &mut ty);
        ty
    }
}
