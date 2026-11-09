use crate::{handler::inputs::Inputs, runtime::paths::Paths};

pub(super) mod args;
mod configs;
mod inputs;
pub(crate) mod names;

use std::mem;

use proc_macro2::TokenStream;
use syn::{ext::IdentExt, visit::Visit};

pub(super) enum Entry {
    Handler,
    Duplex,
}

#[derive(Clone, Copy)]
enum Mode {
    Native,
    Task,
    Duplex,
}

pub(super) struct Handler {
    fun: syn::ItemFn,
    entry: Entry,
    args: args::Args,
}

impl Handler {
    pub(super) fn new(args: args::Args, fun: syn::ItemFn, entry: Entry) -> syn::Result<Self> {
        use syn::Safety;
        if let (Entry::Duplex, Some(request)) = (&entry, &args.request) {
            return Err(syn::Error::new_spanned(
                request,
                "duplex requires a request argument",
            ));
        }
        fun.modifiers.require_empty()?;
        Inputs::validate(&fun.sig)?;
        if let Safety::Unsafe(token) = &fun.sig.safety {
            return Err(syn::Error::new_spanned(
                token,
                "#[sark_gen::handler] does not support unsafe functions",
            ));
        }
        if !names::is_function_name(&fun.sig.ident) {
            return Err(syn::Error::new_spanned(
                &fun.sig.ident,
                "handler function names must use snake_case",
            ));
        }
        Ok(Self { fun, entry, args })
    }
    /// Retains named state borrows separately from request/driver invocation proofs.
    /// Task state and response placeholders select the driver; missing `'req`/`'d`
    /// binders and invocation-only outlives proofs remain on the generated function.
    pub(super) fn expand(
        mut self,
        paths: &Paths,
        h1: crate::runtime::H1,
    ) -> syn::Result<TokenStream> {
        let ns_core = &paths.core;
        let ns_http = &paths.http;
        let ns_fiber = &paths.fiber;
        use syn::{FnArg, ReturnType};

        use crate::types::{Generics, Lifetimes};
        let configs::Attrs {
            cache,
            max_body: max,
            head_skip: skips,
        } = configs::Attrs::take(&mut self.fun)?;
        let Self {
            mut fun,
            entry,
            args: args::Args { request, vis },
        } = self;
        let source = fun.sig.ident.clone();
        let name = names::marker(&source);
        let vis = vis.unwrap_or_else(|| fun.vis.clone());
        let inner_vis = match &vis {
            syn::Visibility::Inherited => quote::quote!(pub(super)),
            syn::Visibility::Restricted(scope) => {
                let mut path = scope.path.clone();
                if let Some(first) = path.segments.first_mut() {
                    if first.ident == "self" {
                        first.ident = syn::Ident::new("super", first.ident.span());
                    } else if first.ident == "super" {
                        path.segments.insert(0, syn::parse_quote!(super));
                    }
                }
                quote::quote!(pub(in #path))
            }
            vis => quote::quote!(#vis),
        };
        let declared = mem::replace(&mut fun.sig.output, ReturnType::Default);
        let (arrow, output) = match declared {
            ReturnType::Type(arrow, ty) => (Some(arrow), *ty),
            ReturnType::Default => (None, syn::parse_quote!(())),
        };
        let (mode, request) = match (entry, fun.sig.asyncness.as_ref()) {
            (Entry::Handler, None) => (Mode::Native, request),
            (Entry::Handler, Some(_)) => (Mode::Task, request),
            (Entry::Duplex, None) => (Mode::Duplex, None),
            (Entry::Duplex, Some(token)) => {
                return Err(syn::Error::new_spanned(
                    token,
                    "duplex session construction must be synchronous",
                ));
            }
        };
        let (request, schema, schema_import) = if request.is_none()
            && !matches!(mode, Mode::Duplex)
            && fun.sig.inputs.first().is_none_or(|argument| {
                let FnArg::Typed(argument) = argument else {
                    return false;
                };
                match argument.ty.as_ref() {
                    syn::Type::Reference(_) => true,
                    syn::Type::Path(path) if matches!(mode, Mode::Task) => path
                        .path
                        .segments
                        .iter()
                        .any(|segment| !matches!(segment.arguments, syn::PathArguments::None)),
                    _ => false,
                }
            }) {
            let request = quote::format_ident!("{}Request", name);
            let schema = crate::request::build(
                paths,
                syn::parse_quote!(),
                syn::parse_quote!(#[doc(hidden)] #inner_vis struct #request {}),
            )?;
            (Some(syn::parse_quote!(#request)), schema, Some(request))
        } else {
            (request, TokenStream::new(), None)
        };
        let output = match mode {
            Mode::Task => Lifetimes::new(&output).anonymous("d"),
            Mode::Native | Mode::Duplex => output,
        };
        let life = Lifetimes::new(&output);
        let input = Inputs::parse(&mut fun, request)?;
        use inputs::{Request, State, Timer};
        let state_lt = match (mode, &input.request, &input.state) {
            (Mode::Duplex, Request::Argument { .. }, State::Argument { lifetime, .. }) => {
                lifetime.as_ref()
            }
            (Mode::Native | Mode::Task, _, _) => None,
            (Mode::Duplex, _, _) => {
                return Err(syn::Error::new_spanned(
                    &fun.sig.inputs,
                    "duplex constructors require request and state arguments",
                ));
            }
        };
        let req = match &input.request {
            Request::Declared(ty) | Request::Argument { ty, .. } => ty,
        };
        let state = match &input.state {
            State::Generic(ty) | State::Argument { ty, .. } => ty,
        };
        match mode {
            Mode::Task if !life.is_within(&Lifetimes::new(state).named()) => {
                let message = "async handler responses must retain driver or named state lifetimes";
                let error = match &arrow {
                    Some(arrow) => syn::Error::new_spanned(arrow, message),
                    None => syn::Error::new_spanned(&fun.sig.ident, message),
                };
                return Err(error);
            }
            Mode::Native | Mode::Task | Mode::Duplex => {}
        }
        if Generics::bind_driver(&mut fun.sig.generics).is_some() && !matches!(mode, Mode::Task) {
            return Err(syn::Error::new_spanned(
                &fun.sig.generics,
                "driver placeholders in state bounds require an async handler",
            ));
        }
        let (state, anon) = match mode {
            Mode::Task => {
                let lives = Lifetimes::new(state);
                if lives
                    .named()
                    .iter()
                    .any(|lifetime| lifetime.ident.unraw() == "d")
                {
                    return Err(syn::Error::new_spanned(
                        state,
                        "write the driver lifetime as `'_`; `d` is reserved for the expansion",
                    ));
                }
                match lives.placeholders() {
                    0 => (state.clone(), Vec::new()),
                    1 => (Lifetimes::new(state).anonymous("d"), Vec::new()),
                    _ => {
                        return Err(syn::Error::new_spanned(
                            state,
                            "a state type carries at most one elided lifetime: the driver",
                        ));
                    }
                }
            }
            Mode::Native | Mode::Duplex => Lifetimes::new(state).distinct("__sark_state_data"),
        };
        let mut generics = Generics::new(fun.sig.generics.clone());
        generics
            .lifetimes
            .retain(|lifetime| lifetime.ident.unraw() != "req" && lifetime.ident.unraw() != "d");
        let state_lives = Lifetimes::new(&state).named();
        generics.include(state_lives.iter().cloned());
        generics.reserve().visit_signature(&fun.sig);
        generics.reserve().visit_type(&output);
        generics.reserve().visit_type(req);
        let call = generics.ident(&format!("__{}_fn", source.unraw()));
        fun.sig.ident = call.clone();
        generics.predicates.retain(|bound| {
            let syn::WherePredicate::Lifetime(bound) = bound else {
                return true;
            };
            let is_call = |lifetime: &syn::Lifetime| {
                lifetime.ident.unraw() == "req"
                    || (lifetime.ident.unraw() == "d" && !matches!(mode, Mode::Task))
            };
            !is_call(&bound.lifetime) && !bound.bounds.iter().any(is_call)
        });
        let inv_req = generics.lifetime("req");
        let inv_d = generics.lifetime("d");
        let borrow = generics.lifetime("a");
        let env = generics.lifetime("env");
        let out_req = generics.lifetime("__req");
        let out_d = generics.lifetime("__d");
        let [v_params, v_req, v_heads, v_body] =
            generics.locals(["params", "req", "headers", "parsed_body"]);
        let [v_state, v_rqst, v_input] = generics.locals(["state", "request", "input"]);
        let lts = &generics.lifetimes;
        let params = &generics.parameters;
        let bounds = &generics.predicates;
        let task_lts = lts
            .iter()
            .filter(|lifetime| lifetime.ident.unraw() != "d")
            .collect::<Vec<_>>();
        let state_ty = match &input.state {
            State::Generic(_) => quote::quote!(__SarkState,),
            State::Argument { .. } => TokenStream::new(),
        };
        let impl_lts = match (&input.state, lts.is_empty() && params.is_empty()) {
            (State::Argument { .. }, true) => TokenStream::new(),
            (State::Generic(_) | State::Argument { .. }, false) | (State::Generic(_), true) => {
                quote::quote!(<#( #lts, )* #( #params, )* #state_ty>)
            }
        };
        if !fun.sig.generics.params.iter().any(
            |param| matches!(param, syn::GenericParam::Lifetime(lt) if lt.lifetime.ident.unraw() == "req"),
        ) {
            fun.sig.generics.params.insert(0, syn::parse_quote!('req));
        }
        if !fun.sig.generics.params.iter().any(
            |param| matches!(param, syn::GenericParam::Lifetime(lt) if lt.lifetime.ident.unraw() == "d"),
        ) {
            fun.sig.generics.params.insert(0, syn::parse_quote!('d));
        }
        for lifetime in anon.into_iter().rev() {
            fun.sig.generics.params.insert(
                0,
                syn::GenericParam::Lifetime(syn::LifetimeParam::new(lifetime)),
            );
        }
        let request = matches!(&input.request, Request::Argument { .. });
        let state_at = match &input.state {
            State::Argument { index, .. } => Some(*index),
            State::Generic(_) => None,
        };
        let mut types = Vec::new();
        let mut patterns = Vec::new();
        for (position, argument) in mem::take(&mut fun.sig.inputs).into_iter().enumerate() {
            let FnArg::Typed(mut argument) = argument else {
                return Err(syn::Error::new_spanned(
                    argument,
                    "handler arguments must be typed",
                ));
            };
            if request && position == 0 {
                *argument.ty = match mode {
                    Mode::Task => syn::parse_quote!(<#req as #ns_core::route::Own>::Task<'d>),
                    Mode::Native | Mode::Duplex => syn::parse_quote!(
                        <#req as #ns_core::route::Project>::View<'req, 'd>
                    ),
                };
            } else if state_at == Some(position) {
                *argument.ty = match mode {
                    Mode::Task => syn::parse_quote!(&'req #state),
                    Mode::Duplex => match state_lt {
                        Some(lifetime) => syn::parse_quote!(&#lifetime #state),
                        None => syn::parse_quote!(&#state),
                    },
                    Mode::Native => syn::parse_quote!(&#state),
                };
            }
            types.push(argument.ty);
            patterns.push(argument.pat);
        }
        fun.sig.inputs = syn::parse_quote!(#call: (#(#types,)*));
        fun.sig
            .generics
            .make_where_clause()
            .predicates
            .push(syn::parse_quote!('d: 'req));
        let body = fun.block;
        fun.block = Box::new(syn::parse_quote!({
            let (#(#patterns,)*) = #call;
            #body
        }));
        fun.sig.output = match arrow {
            Some(arrow) => ReturnType::Type(arrow, Box::new(output.clone())),
            None => ReturnType::Default,
        };
        if matches!(mode, Mode::Task) {
            fun.attrs.push(syn::parse_quote!(
                #[#ns_fiber::dope_gen::fiber_fn('d, crate = #ns_fiber)]
            ));
        }
        use configs::Response;
        let response = match mode {
            Mode::Native => Response::Native {
                output: &output,
                cache,
            },
            Mode::Task => Response::Plain {
                kind: quote::quote!(#ns_fiber::manifold::runtime::NativeFiber),
                cache,
            },
            Mode::Duplex => Response::Plain {
                kind: quote::quote!(#ns_core::duplex::Shape),
                cache,
            },
        };
        let spec = response.build(paths, &name, req, max.as_ref(), skips, h1);
        let (outer, inner) = match mode {
            Mode::Native => {
                let req_out = life.response(&out_req, Some(&out_d), &state_lives);
                let output = life.response(&inv_req, Some(&inv_d), &state_lives);
                let request_arg = request.then(|| quote::quote!(#v_input.0,));
                let state_arg = state_at.map(|_| quote::quote!(#v_input.1,));
                let projected = quote::quote!(
                    <Self::Request as #ns_core::route::Project>::from_parts(
                        #v_input.0, #v_input.2, #v_input.3, #v_input.1,
                    )
                );
                let projected = if request {
                    projected
                } else {
                    quote::quote!({ #projected; })
                };
                let outer = quote::quote! {
                    impl #impl_lts #ns_core::manifold::Route<#state> for #name
                    where #( #bounds, )*
                    {
                        type Response<#out_req, #out_d> =
                            <#req_out as #ns_http::payload::response::dispatch::IntoOutput<#out_req, #out_d>>::Output
                        where #out_d: #out_req;
                        #[inline(always)]
                        fn invoke<#inv_req, #inv_d, #borrow>(
                            #v_input: #ns_core::manifold::Args<
                                '_, #inv_req, #inv_d, #borrow, Self::Request, #state,
                            >,
                        ) -> Self::Response<#inv_req, #inv_d>
                        where #inv_req: #borrow, #inv_d: #inv_req
                        {
                            let #v_input = (#projected, #v_input.4);
                            <#output as #ns_http::payload::response::dispatch::IntoOutput<#inv_req, #inv_d>>::into_output(
                                #call((#request_arg #state_arg)),
                            )
                        }
                    }
                };
                (outer, TokenStream::new())
            }
            Mode::Task => {
                let state_arg = state_at.map(|_| quote::quote!(#v_input.1,));
                let timer_arg =
                    matches!(input.timer, Timer::Present).then(|| quote::quote!(#v_input.2,));
                let outer = quote::quote! {
                    impl<#( #task_lts, )* 'd, #( #params, )* #state_ty>
                        #ns_core::route::Task<'d, #state> for #name
                    where #( #bounds, )*
                    {
                        type Output = #output;
                    }
                    impl<#( #task_lts, )* 'd, #( #params, )* #state_ty>
                        #ns_fiber::manifold::runtime::TaskRoute<'d, #state> for #name
                    where #( #bounds, )*
                    {
                        #[inline(always)]
                        fn invoke_task<#inv_req>(
                            #v_input: (
                                #ns_core::manifold::task::request::Input<'d, Self>,
                                &#inv_req #state,
                                #ns_fiber::driver::schedule::clock::timer::Timer<'d>,
                            ),
                        ) -> impl #ns_fiber::Fiber<
                            'd, Output = #ns_core::manifold::task::response::Output<'d, Self, #state>,
                        > + #inv_req
                        where 'd: #inv_req
                        {
                            #call((#v_input.0, #state_arg #timer_arg))
                        }
                    }
                };
                let extra_types = &types[usize::from(request)..];
                fun.sig.inputs = syn::parse_quote!(
                    #call: (#ns_core::manifold::task::request::Input<'d, #name>, #( #extra_types, )*)
                );
                if matches!(input.state, State::Generic(_)) {
                    fun.sig.generics.params.push(syn::parse_quote!(__SarkState));
                }
                fun.sig
                    .generics
                    .make_where_clause()
                    .predicates
                    .push(syn::parse_quote!(#state: 'req));
                fun.sig.output = syn::parse_quote!(
                    -> #ns_core::manifold::task::response::Output<'d, #name, #state>
                );
                let extra = (1..=usize::from(state_at.is_some())
                    + usize::from(matches!(input.timer, Timer::Present)))
                    .map(syn::Index::from)
                    .collect::<Vec<_>>();
                let admitted_request = request.then(|| quote::quote!(#call.0.1,));
                let (ready, id) = if request {
                    (quote::quote!(#call.into_parts()), quote::quote!(#call.0.0))
                } else {
                    (quote::quote!(#call.into_parts().0), quote::quote!(#call.0))
                };
                fun.block = Box::new(syn::parse_quote!({
                    let #call = (
                        match #call::start::<#state>(#call.0) {
                            ::core::result::Result::Ok(::core::ops::ControlFlow::Break(#call)) =>
                                #ready,
                            ::core::result::Result::Ok(::core::ops::ControlFlow::Continue(mut #call)) => {
                                loop {
                                    #call = match #call::step::<#state>(#call) {
                                        ::core::result::Result::Ok(::core::ops::ControlFlow::Break(#call)) =>
                                            break #ready,
                                        ::core::result::Result::Ok(::core::ops::ControlFlow::Continue(#call)) => #call,
                                        ::core::result::Result::Err(#call) => return #call,
                                    };
                                    #call::yield_once().await;
                                }
                            }
                            ::core::result::Result::Err(#call) => return #call,
                        },
                        #( #call.#extra, )*
                    );
                    #ns_core::manifold::task::response::Output::response(#id, async {
                        let (#( #patterns, )*) = (#admitted_request #( #call.#extra, )*);
                        #body
                    }.await)
                }));
                let [v_proof, v_credit, v_req, v_id] =
                    generics.locals(["__sark_evidence", "released", "request", "id"]);
                let [v_task, v_error, v_output, v_reject] =
                    generics.locals(["build", "error", "output", "reject"]);
                let [v_old, v_yield, v_cx] = generics.locals(["_build", "yielded", "cx"]);
                let released = quote::quote! {
                    let mut #v_proof = #ns_core::body::credit::Disabled::new();
                    #ns_core::body::credit::Evidence::released(
                        &mut #v_proof, #v_credit, #ns_core::body::credit::Disposition::Admitted,
                    );
                };
                let builder = quote::quote!(
                    <<#name as #ns_core::route::Spec>::Request as #ns_core::route::Own>::TaskBuilder<'d>
                );
                let admitted = quote::quote!(::core::result::Result<
                    ::core::ops::ControlFlow<
                        #ns_core::route::Built<'d,
                            <<#name as #ns_core::route::Spec>::Request as #ns_core::route::Own>::Task<'d>>,
                        (#ns_http::request::Id<'d>, #builder),
                    >,
                    #ns_core::manifold::task::response::Output<'d, #name, State>,
                >);
                let inner = quote::quote! {
                    #[inline(always)]
                    pub(super) fn start<'d, State>(
                        #v_input: #ns_core::manifold::task::request::Input<'d, #name>,
                    ) -> #admitted where #name: #ns_core::route::Task<'d, State>
                    {
                        match #v_input {
                            #ns_core::manifold::task::request::Input::Ready { request: #v_req } =>
                                ::core::result::Result::Ok(::core::ops::ControlFlow::Break(#v_req)),
                            #ns_core::manifold::task::request::Input::Buffered(#v_req) => {
                                let #v_id = #v_req.owner().id();
                                match <<#name as #ns_core::route::Spec>::Request as #ns_core::route::Own>::start_task(#v_req) {
                                    ::core::result::Result::Ok(#v_task) =>
                                        ::core::result::Result::Ok(::core::ops::ControlFlow::Continue((#v_id, #v_task))),
                                    ::core::result::Result::Err(#v_error) =>
                                        ::core::result::Result::Err(#ns_core::manifold::task::response::Output::rejected(
                                            #v_id,
                                            <<#name as #ns_core::route::Spec>::Request as #ns_core::route::Own>::reject(#v_error),
                                        )),
                                }
                            }
                        }
                    }
                    #[inline(always)]
                    pub(super) fn step<'d, State>((#v_id, #v_task): (#ns_http::request::Id<'d>, #builder))
                        -> #admitted where #name: #ns_core::route::Task<'d, State>
                    {
                        match #ns_core::body::Build::step(#v_task, #ns_core::body::DEFAULT_BUILD_STEP_BUDGET) {
                            #ns_core::body::Step::Ready { output: #v_output, released: #v_credit } => {
                                #released
                                ::core::result::Result::Ok(::core::ops::ControlFlow::Break(#v_output))
                            }
                            #ns_core::body::Step::Failed { build: #v_task, error: #v_error, released: #v_credit } => {
                                #released
                                let #v_reject = {
                                    let #v_old = #v_task;
                                    <#builder as #ns_core::route::TaskBuild>::reject(#v_error)
                                };
                                ::core::result::Result::Err(#ns_core::manifold::task::response::Output::rejected(#v_id, #v_reject))
                            }
                            #ns_core::body::Step::NeedInput { build: #v_task, released: #v_credit }
                            | #ns_core::body::Step::Pending { build: #v_task, released: #v_credit } => {
                                #released
                                ::core::result::Result::Ok(::core::ops::ControlFlow::Continue((#v_id, #v_task)))
                            }
                        }
                    }
                    #[inline(always)]
                    pub(super) fn yield_once<'d>() -> impl #ns_fiber::Fiber<'d, Output = ()> {
                        let mut #v_yield = false;
                        #ns_fiber::abi::poll_fn(move |#v_cx| {
                            if #v_yield {
                                ::core::task::Poll::Ready(())
                            } else {
                                #v_yield = true;
                                #v_cx.wake();
                                ::core::task::Poll::Pending
                            }
                        })
                    }
                };
                (outer, inner)
            }
            Mode::Duplex => {
                let session = life.session(state_lt, &env.ident.to_string());
                let outer = quote::quote! {
                    impl<#env, #( #lts: #env, )* #( #params, )*>
                        #ns_core::duplex::Route<#env, #state> for #name
                    where #( #bounds, )* #state: #env, #session: #env
                    {
                        type Session = #session;
                        #[inline(always)]
                        fn open<#inv_req, #inv_d>(
                            #v_params: <Self::Request as #ns_core::route::Project>::Params<#inv_req>,
                            #v_req: &#ns_core::request::View<#inv_req, #inv_d, <Self::Request as #ns_core::route::Schema>::Frame>,
                            #v_heads: <Self::Request as #ns_core::route::Project>::Headers<#inv_req>,
                            #v_body: <Self::Request as #ns_core::route::Project>::ParsedBody<#inv_req, #inv_d>,
                            #v_state: &#env #state,
                        ) -> Self::Session
                        where #inv_d: #inv_req + #env, Self::Session: #ns_core::duplex::Session<#inv_d>
                        {
                            let #v_rqst = <Self::Request as #ns_core::route::Project>::from_parts(
                                #v_params, #v_heads, #v_body, #v_req,
                            );
                            #call((#v_rqst, #v_state))
                        }
                    }
                };
                (outer, TokenStream::new())
            }
        };
        let schema_import = schema_import.into_iter();
        Ok(quote::quote! {
            #fun
            #vis use #call::#name;
            #( #vis use #call::#schema_import; )*
            #spec
            #outer
            mod #call {
                #schema
                #inner_vis struct #name;
                #inner
            }
        })
    }
}
