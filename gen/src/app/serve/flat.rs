use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::{app::serve, runtime::paths::Paths};

pub(super) struct Flat {
    pub(super) definition: TokenStream,
    pub(super) ty: TokenStream,
    pub(super) mode: syn::Ident,
}

pub(super) fn name(spec: &crate::app::spec::Spec, queued: bool) -> syn::Ident {
    format_ident!(
        "__{}{}",
        spec.name,
        if queued { "Queued" } else { "Direct" }
    )
}

pub(super) fn end(task: serve::Task<'_, '_>) -> syn::Ident {
    if task.quota().is_some() {
        task.phase("End")
    } else if task.is_stream() {
        format_ident!("EndAsync")
    } else if matches!(task.route.mode, serve::Mode::Stream { .. }) {
        format_ident!("EndSync")
    } else {
        format_ident!("EndFixed")
    }
}

pub(super) fn staged(
    spec: &crate::app::spec::Spec,
    exec: &serve::Execution<'_, '_>,
    head: bool,
    value: TokenStream,
) -> TokenStream {
    let phase = name(spec, false);
    let mut patterns = Vec::new();
    let mut ends = Vec::new();
    for (task, fiber) in exec.fibers() {
        if head {
            let head = task.phase("Head");
            patterns.push(quote!(#phase::#head(_)));
        }
        if fiber.stream {
            let body = task.phase("Body");
            patterns.push(quote!(#phase::#body(_)));
            let end = end(*task);
            if !ends.contains(&end) {
                patterns.push(quote!(#phase::#end { .. }));
                ends.push(end);
            }
        }
    }
    if patterns.is_empty() {
        quote!(false)
    } else {
        quote!(matches!(#value, #(#patterns)|*))
    }
}

pub(super) fn counts(
    paths: &Paths,
    spec: &crate::app::spec::Spec,
    exec: &serve::Execution<'_, '_>,
    queued: bool,
) -> TokenStream {
    let ns = &paths.fiber;
    let [tasks, quotas, total] = spec.state_generics.locals(["tasks", "quotas", "__total"]);
    let tasks_in_plane = exec
        .tasks()
        .iter()
        .filter(|task| !queued || matches!(task.route.mode, serve::Mode::Fiber { .. }));
    let capacities: Vec<_> = tasks_in_plane
        .clone()
        .filter_map(|task| task.capacity())
        .collect();
    if capacities.is_empty() {
        return TokenStream::new();
    }
    let indexes: Vec<_> = (0..capacities.len()).collect();
    let bounded = tasks_in_plane.count() == capacities.len();
    let limit = bounded.then(|| {
        quote! {
            let #tasks = #tasks.min(#ns::o3::depot::capacity::Count::new(#total));
        }
    });
    quote! {
        let (#quotas, #total) = const {
            let #quotas = [#(
                #ns::o3::depot::capacity::Limit::<{ u32::MAX as usize }>
                    ::fixed::<{ #capacities }>().count().get(),
            )*];
            (#quotas, 0u32 #(.saturating_add(#quotas[#indexes]))*)
        };
        #limit
    }
}

pub(super) fn remove(
    spec: &crate::app::spec::Spec,
    exec: &serve::Execution<'_, '_>,
    queued: bool,
    entry: &syn::Ident,
    quotas: TokenStream,
) -> TokenStream {
    let flat = name(spec, queued);
    let arms: Vec<_> = exec.tasks().iter().filter_map(|task| {
        let index = if queued {
            let serve::Mode::Fiber { fiber, .. } = &task.route.mode else { return None };
            fiber.quota?
        } else { task.quota()? };
        let future = task.phase("Future");
        let head = task.phase("Head");
        let body = task.phase("Body");
        let end = end(*task);
        let phases = match &task.route.mode {
            serve::Mode::Fiber { fiber, .. } if queued && !fiber.stream => quote!(#flat::#future(_)),
            serve::Mode::Fiber { fiber, .. } if fiber.stream => {
                quote!(#flat::#future(_) | #flat::#head(..) | #flat::#body(..) | #flat::#end { .. })
            }
            serve::Mode::Fiber { .. } => quote!(#flat::#future(_) | #flat::#head(_) | #flat::#end { .. }),
            serve::Mode::Stream { .. } => quote!(#flat::#body(_) | #flat::#end { .. }),
            _ => unreachable!("task"),
        };
        Some(quote!(#phases => Some(#index),))
    }).collect();
    if arms.is_empty() {
        return quote!(#entry.remove(););
    }
    let quota = spec.state_generics.local("__quota");
    let total = if queued {
        exec.fibers().len()
    } else {
        exec.tasks().len()
    };
    let other = (arms.len() < total).then(|| quote!(_ => None));
    quote! {
        let #quota = match #entry.get() { #(#arms)* #other };
        #entry.remove();
        if let Some(#quota) = #quota { #quotas[#quota] += 1; }
    }
}

pub(super) fn build(
    paths: &Paths,
    spec: &crate::app::spec::Spec,
    exec: &serve::Execution<'_, '_>,
    queued: bool,
) -> Flat {
    let ns = &paths.fiber;
    let core = &paths.core;
    let http = &paths.http;
    let flat = name(spec, queued);
    let projection = format_ident!("{}Projection", flat);
    let mode = format_ident!("{}Mode", flat);
    let event = format_ident!("{}Event", flat);
    let completed = format_ident!("{}Completed", flat);
    let fixed = format_ident!("__{}Fixed", spec.name);
    let mixed = exec.fibers().iter().any(|(_, fiber)| fiber.stream);
    let mut params = Vec::new();
    let mut impl_params = Vec::new();
    let mut args = Vec::new();
    let mut bounds = Vec::new();
    let mut variants = Vec::new();
    let mut polls = Vec::new();
    let mut held = Vec::new();
    let mut ends = Vec::new();
    let mut fixed_types = Vec::new();
    let mut errors = Vec::new();
    let mut failures = Vec::new();
    for task in exec.tasks().iter().copied() {
        let fiber = match &task.route.mode {
            serve::Mode::Fiber { fiber, .. } => Some(fiber),
            _ if queued => continue,
            _ => None,
        };
        let future = task.phase("Future");
        let head = task.phase("Head");
        let body = task.phase("Body");
        let end = end(task);
        let f = task.phase("F");
        let o = task.phase("O");
        let s = task.phase("S");
        let has_stream = fiber.is_none_or(|fiber| fiber.stream);
        if let Some(fiber) = fiber {
            let actual = &fiber.future;
            let output = task.fiber_output(paths, spec.state.ty());
            params.push(quote!(#f));
            impl_params.push(quote!(#f));
            args.push(quote!(#actual));
            impl_params.push(quote!(#o));
            if !queued || fiber.stream {
                params.push(quote!(#o));
                args.push(output);
            }
            bounds.push(quote!(#f: #ns::dope_fiber::abi::Fiber<'d, Output = #o>,));
            variants.push(quote!(#future(#[pin] #f),));
            let complete = if queued && !fiber.stream {
                let variant = task.variant();
                fixed_types.push(quote!(#o));
                let output = quote!(#fixed::#variant(output));
                let output = if mixed {
                    quote!(#completed::Fixed(#output))
                } else {
                    output
                };
                quote!(::core::ops::ControlFlow::Break(#output))
            } else {
                let flag = queued.then(|| quote!(, true));
                let flag_type = queued.then(|| quote!(, bool));
                variants.push(quote!(#head(#o #flag_type),));
                let output = if queued {
                    quote!(::core::ops::ControlFlow::Continue(#event::Head))
                } else {
                    quote!(Some(#event::Head))
                };
                polls.push(quote!(#projection::#head(..) => ::core::task::Poll::Pending,));
                quote!({ this.set(#flat::#head(output #flag)); #output })
            };
            polls.push(quote! {
                #projection::#future(future) => {
                    // SAFETY: this variant owns the unfinished pinned child and consumes one admission.
                    match unsafe { #ns::dope_fiber::context::raw::Live::new(future) }.admitted(admit) {
                        ::core::task::Poll::Pending => ::core::task::Poll::Pending,
                        ::core::task::Poll::Ready(output) => ::core::task::Poll::Ready(#complete),
                    }
                }
            });
            if queued {
                let error = if fiber.stream {
                    quote!(Active)
                } else {
                    quote!(Absent)
                };
                held.push(quote!(#projection::#future(_) => Err(#ns::dope_fiber::task::storage::fixed::schedule::result::Error::#error),));
            }
        }
        if has_stream {
            let stream = if fiber.is_some() {
                let route = task.route();
                let state = spec.state.ty();
                quote!(#core::manifold::task::response::Stream<'d, #route, #state>)
            } else {
                task.store_stream(paths).0
            };
            params.push(quote!(#s));
            impl_params.push(quote!(#s));
            args.push(stream);
            bounds.push(quote!(#s: #ns::dope_fiber::abi::Stream<'d, Item: ::core::convert::TryInto<#http::payload::response::dispatch::chunk::Chunk<'d>, Error: ::core::fmt::Debug>>,));
            let error = task.phase("Error");
            errors.push(quote!(<<#s as #ns::dope_fiber::abi::Stream<'d>>::Item as ::core::convert::TryInto<#http::payload::response::dispatch::chunk::Chunk<'d>>>::Error));
            failures.push(error.clone());
            let flag_type = queued.then(|| quote!(, bool));
            let flag = queued.then(|| quote!(, held));
            variants.push(quote!(#body(#[pin] #s #flag_type),));
            let check = queued.then(|| {
                quote!(if *held {
                    return ::core::task::Poll::Pending;
                })
            });
            let pause = queued.then(|| quote!(*held = true;));
            let item = if queued {
                quote!(::core::ops::ControlFlow::Continue(item))
            } else {
                quote!(Some(item))
            };
            let finish = if queued {
                quote!(::core::ops::ControlFlow::Break(#completed::#end))
            } else {
                quote!({ this.set(#flat::#end {}); None })
            };
            polls.push(quote! {
                #projection::#body(stream #flag) => {
                    #check
                    // SAFETY: Body owns the pinned live stream; EOF ends its polling authority.
                    match unsafe { #ns::dope_fiber::context::raw::Live::new(stream) }.admitted_next(admit) {
                        ::core::task::Poll::Pending => ::core::task::Poll::Pending,
                        ::core::task::Poll::Ready(Some(item)) => {
                            #pause
                            let item = match ::core::convert::TryInto::try_into(item) {
                                Ok(chunk) => #event::Chunk(chunk),
                                Err(cause) => #event::#error(cause),
                            };
                            ::core::task::Poll::Ready(#item)
                        }
                        ::core::task::Poll::Ready(None) => ::core::task::Poll::Ready(#finish),
                    }
                }
            });
            if queued {
                held.push(
                    quote!(#projection::#head(_, held) | #projection::#body(_, held) => Ok(held),),
                );
            }
        }
        if !queued || has_stream {
            if !ends.contains(&end) {
                let field = queued.then(|| quote!(held: bool));
                variants.push(quote!(#end { #field },));
                let finish = if queued {
                    quote!(::core::ops::ControlFlow::Break(#completed::#end))
                } else {
                    quote!(None)
                };
                let guard = queued.then(|| {
                    quote!(if *held {
                        return ::core::task::Poll::Pending;
                    })
                });
                let pattern = queued.then(|| quote!(held));
                polls.push(quote!(#projection::#end { #pattern } => { #guard ::core::task::Poll::Ready(#finish) },));
                if queued {
                    held.push(quote!(#projection::#end { held } => Ok(held),));
                }
                ends.push(end);
            }
        }
    }
    let ty = quote!(#flat<#(#args),*>);
    if variants.is_empty() {
        return Flat {
            definition: TokenStream::new(),
            ty: quote!(()),
            mode,
        };
    }
    let error_params: Vec<_> = failures
        .iter()
        .enumerate()
        .map(|(i, _)| format_ident!("E{i}"))
        .collect();
    let event_def = (!queued || mixed).then(|| {
        quote! {
            enum #event<'d, #(#error_params),*> {
                Head,
                Chunk(#http::payload::response::dispatch::chunk::Chunk<'d>),
                #(#failures(#error_params),)*
            }
        }
    });
    let item = quote!(#event<'d, #(#errors),*>);
    let enum_def = quote! {
        #[#ns::pin_project::pin_project(project = #projection)]
        enum #flat<#(#params),*> { #(#variants)* }
    };
    let definition = if queued {
        // Fixed outputs are transient: the native scheduler retires their slots first.
        let fixed_arg = (!fixed_types.is_empty()).then(|| quote!(#fixed<#(#fixed_types),*>));
        let fixed_param = fixed_arg.as_ref().map(|_| quote!(C));
        let fixed_variant = fixed_arg.as_ref().map(|_| quote!(Fixed(C),));
        let complete_def =
            mixed.then(|| quote!(enum #completed<#fixed_param> { #fixed_variant #(#ends,)* }));
        let output = if mixed {
            quote!(#completed<#fixed_arg>)
        } else {
            quote!(#fixed_arg)
        };
        let item = if mixed {
            item
        } else {
            quote!(::core::convert::Infallible)
        };
        quote! {
            #enum_def
            #event_def
            #complete_def
            struct #mode;
            // SAFETY: phase transitions consume completed children and held flags gate retained items.
            unsafe impl<'d, #(#impl_params),*> #ns::dope_fiber::task::storage::fixed::schedule::Mode<'d, #flat<#(#params),*>> for #mode
            where #(#bounds)*
            {
                type Output = #output;
                type Item = #item;
                const INITIAL: Self = Self;
                fn poll(&mut self, call: #ns::dope_fiber::context::PollCall<'_, '_, 'd, #flat<#(#params),*>>)
                    -> ::core::task::Poll<::core::ops::ControlFlow<Self::Output, Self::Item>>
                {
                    let (mut this, admit) = call.into_admit();
                    match this.as_mut().project() { #(#polls)* }
                }
                fn held<'a>(&'a mut self, fiber: ::core::pin::Pin<&'a mut #flat<#(#params),*>>)
                    -> Result<&'a mut bool, #ns::dope_fiber::task::storage::fixed::schedule::result::Error>
                {
                    match fiber.project() { #(#held)* }
                }
            }
        }
    } else {
        quote! {
            #enum_def
            #event_def
            impl<'d, #(#params),*> #ns::dope_fiber::abi::Stream<'d> for #flat<#(#params),*>
            where #(#bounds)*
            {
                type Item = #item;
                fn poll_next(call: #ns::dope_fiber::context::PollCall<'_, '_, 'd, Self>) -> ::core::task::Poll<Option<Self::Item>> {
                    let (mut this, admit) = call.into_admit();
                    match this.as_mut().project() { #(#polls)* }
                }
            }
        }
    };
    Flat {
        definition,
        ty,
        mode,
    }
}
