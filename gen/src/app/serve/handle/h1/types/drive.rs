use proc_macro2::TokenStream;
use quote::quote;

use crate::{
    app::serve::{flat, handle},
    runtime::paths::Paths,
};

pub(super) fn methods(
    paths: &Paths,
    ctx: &handle::Context<'_, '_, '_>,
    ty: &TokenStream,
    slot: &TokenStream,
    bound: &TokenStream,
) -> TokenStream {
    if ctx.exec.tasks().is_empty() {
        return TokenStream::new();
    }
    let spec = ctx.plan.spec;
    let symbols = &spec.state_generics;
    let [brand, event_life] = symbols.lives(["brand", "e"]);
    let [id, mode, member] = symbols.idents(["__ID", "__M", "__MEMBER"]);
    let ns = &paths.fiber;
    let core = &paths.core;
    let h1 = &paths.h1;
    let phase = flat::name(spec, false);
    let projection = quote::format_ident!("{}Projection", phase);
    let event = quote::format_ident!("{}Event", phase);
    let live = quote!(#ns::dope_fiber::task::storage::fixed::Id<#brand, 'd, #id>);
    let ended = quote!(#ns::dope_fiber::task::storage::fixed::Id<#brand, 'd, #id, #ns::dope_fiber::task::storage::fixed::local::Ended>);
    let count = ctx
        .exec
        .tasks()
        .iter()
        .filter(|task| task.quota().is_some())
        .count();
    let quota_decl = (count != 0).then(|| quote!(quotas: &mut [u32; #count],));
    let quota_arg = (count != 0).then(|| quote!(quotas,));
    let entry = syn::Ident::new("entry", proc_macro2::Span::call_site());
    let remove = flat::remove(spec, ctx.exec, false, &entry, quote!(quotas));
    let output = spec.state.ty();
    let heads = ctx.exec.fibers().iter().map(|(task, fiber)| {
        let head = task.phase("Head");
        let body = task.phase("Body");
        let end = flat::end(*task);
        let route = task.route();
        let credit = task.quota().map(|index| quote!(quotas[#index] += 1;));
        let emit = quote! {
            let deferred = #h1::pump::Ingress::deferred(state);
            let (outcome, stream) = #h1::route::task::Egress::new(write, &date, deferred, request)
                .finish::<#route, #output>(output);
        };
        let finish = if fiber.stream {
            quote! {
                #emit
                let result = #h1::server::payload::write::try_outcome(outcome, life, exchange, |intent, done| {
                    state.settle(intent);
                    stream.is_none().then_some(done)
                });
                match stream {
                    Some(stream) => value.set(#phase::#body(stream)),
                    None => { member.discard(); #credit }
                }
                result
            }
        } else {
            quote! {
                member.discard();
                #credit
                #emit
                let _ = stream;
                #h1::server::payload::write::try_outcome(outcome, life, exchange, |intent, done| {
                    state.settle(intent);
                    Some(done)
                })
            }
        };
        quote! {
            #projection::#head(output) => {
                // SAFETY: Head's output is unpinned. Install the existing empty phase before any callback or drop.
                let output = unsafe {
                    let output = ::core::ptr::read(output);
                    ::core::ptr::write(value.as_mut().get_unchecked_mut(), #phase::#end {});
                    output
                };
                #finish
            }
        }
    });
    let staged = flat::staged(spec, ctx.exec, false, quote!(value));
    let terminal =
        quote!(if #staged { turn::Readiness::Staged } else { turn::Readiness::NewlyReady });
    let failures = ctx
        .exec
        .tasks()
        .iter()
        .filter(|task| {
            task.is_stream() || matches!(task.route.mode, crate::app::serve::Mode::Stream { .. })
        })
        .map(|task| {
            let error = task.phase("Error");
            quote! {
                #event::#error(cause) => {
                    #core::life::Mode::diagnostic(life, &cause);
                    conn.state_mut().settle(head::Connection::Close);
                    conn.close_after();
                    break turn::Turn::retired(terminal, turn::Completion::Rejected);
                }
            }
        });
    let result = quote!(#h1::server::task::turn::Turn<
        <#mode as #core::life::Mode<#ns::dope::manifold::network::listener::conn::Id<'d, #id>, u64>>::Retention
    >);
    let exchange = quote!(#core::life::exchange::Id<#ns::dope::manifold::network::listener::conn::Id<'d, #id>, u64>);
    let bodies = quote! {
        Running::Body(task) => return match tasks.entry(task) {
            Ok(entry) => Self::__body(ControlFlow::Continue(entry), #quota_arg conn, driver, life, exchange),
            Err(_) => { conn.close_after(); turn::Turn::ConnectionCancelled }
        },
        Running::Closing(task) => return match tasks.entry(task) {
            Ok(entry) => Self::__body(ControlFlow::Break(entry), #quota_arg conn, driver, life, exchange),
            Err(_) => { conn.close_after(); turn::Turn::ConnectionCancelled }
        },
    };
    let task_body = if ctx.exec.fibers().is_empty() {
        quote! {
            use #h1::server::task::{runtime::Running, turn};
            use ::core::ops::ControlFlow;
            match running {
                #bodies
                Running::Future(..) | Running::Head(..) => unreachable!("phase"),
            }
        }
    } else {
        quote! {
                use #ns::dope_fiber::{context, task::storage::fixed::local::Member};
                use #ns::dope::manifold::network::listener::conn::write::{self, body};
                use #h1::server::task::{runtime::Running, turn};
                use ::core::{ops::ControlFlow, task::Poll};
                let (request, task, ready) = match running {
                    #bodies
                    Running::Future(request, task) => (request, task, turn::Readiness::NewlyReady),
                    Running::Head(request, task) => (request, task, turn::Readiness::Staged),
                };
                if <#mode as #core::life::Mode<_, u64>>::ENABLED && conn.work().remaining() == 0 {
                    conn.state_mut().async_state.insert(match ready {
                        turn::Readiness::NewlyReady => Running::Future(request, task),
                        turn::Readiness::Staged => Running::Head(request, task),
                    });
                    conn.wake_target().wake();
                    return turn::Turn::Deferred;
                }
                let entry = match tasks.entry(task) {
                    Ok(entry) => entry,
                    Err(_) => { conn.close_after(); return turn::Turn::ConnectionCancelled; }
                };
                let entry = match ready {
                    turn::Readiness::Staged => entry,
                    turn::Readiness::NewlyReady => match context::Context::with_target(
                        conn.wake_target(), conn.work(), driver.reborrow(),
                        |mut context| entry.poll_next(context.as_mut()),
                    ) {
                        ControlFlow::Continue((entry, Poll::Ready(#event::Head))) => entry,
                        ControlFlow::Continue((entry, Poll::Pending)) => {
                            conn.state_mut().async_state.insert(Running::Future(request, entry.into_id()));
                            return turn::Turn::Pending;
                        }
                        _ => unreachable!("phase"),
                    },
                };
                let (task, mut member) = entry.into_parts();
                let date = date.load();
                let result = conn.write().try_draft(driver, |state, write| {
                    let mut value = member.value();
                    match value.as_mut().project() { #(#heads,)* _ => unreachable!("phase") }
                });
                let failure = match result {
                    Ok(write::Outcome::Committed(None)) => {
                        conn.state_mut().async_state.insert(Running::Body(task));
                        conn.wake_target().wake();
                        return turn::Turn::Streaming { ready };
                    }
                    Ok(write::Outcome::Committed(Some(done))) => return turn::Turn::retired(ready, done),
                    Ok(write::Outcome::Deferred(_)) => {
                        conn.state_mut().async_state.insert(Running::Head(request, task));
                        return match ready {
                            turn::Readiness::NewlyReady => turn::Turn::ReadyDeferred,
                            turn::Readiness::Staged => turn::Turn::StagedDeferred,
                        };
                    }
                    Ok(write::Outcome::Closed) => None,
                    Err(cause) => match #h1::server::payload::write::reject(cause) {
                        #h1::server::payload::write::Failure::Capacity(cause) => Some(body::Failure::Capacity(cause)),
                        #h1::server::payload::write::Failure::Rejected { cause, receipt: Some(_) } => {
                            conn.write().reject(cause);
                            return turn::Turn::retired(ready, turn::Completion::Rejected);
                        }
                        #h1::server::payload::write::Failure::Rejected { cause, receipt: None } => Some(cause),
                    }
                };
                let removed = match tasks.entry(task) {
                    Ok(mut entry) => { #remove true }
                    Err(_) => false,
                };
                if let Some(cause) = failure { conn.write().reject(cause); }
                else { Self::__release(conn); }
                if removed { turn::Turn::retired(ready, turn::Completion::Rejected) }
                else { conn.close_after(); turn::Turn::ConnectionCancelled }
        }
    };
    quote! {
        fn __task<#event_life>(
            tasks: &mut #ns::dope_fiber::task::storage::scope::Scope<#brand, ::core::pin::Pin<&mut
                #ns::dope_fiber::task::storage::fixed::Slab<'d, #ty, { u32::MAX as usize }, #id, #ns::o3::depot::arena::slab::storage::Heap>
            >>,
            #quota_decl
            running: #h1::server::task::runtime::Running<#live, #ended>,
            date: &#h1::server::date::Clock,
            conn: &mut #slot,
            driver: &mut #ns::dope::core::driver::Context<'_, 'd>,
            life: &mut #mode,
            exchange: #exchange,
        ) -> #result {
            #task_body
        }

        fn __body<#event_life, #member: #ns::dope_fiber::task::storage::fixed::local::Member<Fiber = #ty>>(
            mut entry: ::core::ops::ControlFlow<
                #ns::dope_fiber::task::storage::fixed::local::Entry<#member, #ended>,
                #ns::dope_fiber::task::storage::fixed::local::Entry<#member, #live>,
            >,
            #quota_decl
            conn: &mut #slot,
            driver: &mut #ns::dope::core::driver::Context<'_, 'd>,
            life: &mut #mode,
            exchange: #exchange,
        ) -> #result {
            use #ns::dope_fiber::context;
            use #ns::dope::{manifold::network::listener::conn::write::body, net::link::egress::{self, data}};
            use #h1::{server::{payload, task::{runtime, turn}}, wire::head};
            use #core::life::sequence;
            use ::core::{ops::ControlFlow, task::Poll};
            let value = match &mut entry { ControlFlow::Break(entry) => entry.get(), ControlFlow::Continue(entry) => entry.get() };
            let terminal = #terminal;
            let mut ready = turn::Readiness::Staged;
            let mut buffered = conn.state_mut().async_state.take_stream();
            let done = loop {
                let (body, end) = if let Some(stashed) = buffered.take() {
                    let (body, end) = stashed.into_parts();
                    (Ok(body), end)
                } else {
                    let frame = match entry {
                        ControlFlow::Break(ended) => { entry = ControlFlow::Break(ended); payload::Frame::terminator() }
                        ControlFlow::Continue(live) => {
                            if <#mode as #core::life::Mode<_, u64>>::ENABLED && conn.work().remaining() == 0 {
                                conn.state_mut().async_state.insert(runtime::Running::Body(live.into_id()));
                                conn.wake_target().wake();
                                return turn::Turn::Deferred;
                            }
                            match context::Context::with_target(conn.wake_target(), conn.work(), driver.reborrow(), |mut context| live.poll_next(context.as_mut())) {
                                ControlFlow::Break(ended) => { entry = ControlFlow::Break(ended); ready = terminal; continue; }
                                ControlFlow::Continue((live, Poll::Pending)) => {
                                    conn.state_mut().async_state.insert(runtime::Running::Body(live.into_id()));
                                    return turn::Turn::Pending;
                                }
                                ControlFlow::Continue((live, Poll::Ready(item))) => {
                                    entry = ControlFlow::Continue(live);
                                    match item {
                                        #event::Chunk(chunk) => payload::Frame::chunk(chunk),
                                        #event::Head => unreachable!("phase"),
                                        #(#failures)*
                                    }
                                }
                            }
                        }
                    };
                    let charge = frame.retention();
                    let tag = sequence::Mode::tag(life, exchange, charge);
                    sequence::Mode::stage(life, conn.state_mut().async_state.session_mut(), exchange, charge);
                    let body = payload::Stream::new(frame, tag);
                    let end = body.end();
                    let body = match egress::Load::new(body).try_nonempty() {
                        Ok(body) => match body.try_bound::<#bound>() {
                            Ok(body) => Ok(body),
                            Err(rejected) => {
                                let (body, maximum) = rejected.into_parts();
                                conn.write().reject(body::Failure::Bound { charge: body.charge(), maximum: maximum.get() });
                                Err(body.into_inner())
                            }
                        },
                        Err(rejected) => {
                            let (body, cause) = rejected.into_parts();
                            conn.write().reject(body::Failure::Payload(cause));
                            Err(body.into_inner())
                        }
                    };
                    (body, end)
                };
                let result = match body {
                    Err(body) => Err(body),
                    Ok(body) => {
                        let charge = data::raw::Payload::charge(body.as_inner());
                        match conn.write().admit_right(body, driver) {
                            body::Admit::Committed => {
                                sequence::Mode::accept(life, conn.state_mut().async_state.session_mut(), exchange, charge);
                                Ok(None)
                            }
                            body::Admit::Deferred { load, .. } => Ok(Some(load)),
                            body::Admit::Closed(load) => Err(load.into_unbound().into_inner()),
                            body::Admit::Rejected { cause, load } => {
                                conn.write().reject(body::Failure::Capacity(cause));
                                Err(load.into_unbound().into_inner())
                            }
                            body::Admit::Unavailable(rejected) => {
                                let (load, cause) = rejected.into_parts();
                                conn.write().reject(body::Failure::Reserve(cause));
                                Err(load.into_unbound().into_inner())
                            }
                        }
                    }
                };
                let retry = match result {
                    Ok(retry) => retry,
                    Err(load) => {
                        conn.state_mut().settle(head::Connection::Close);
                        conn.close_after();
                        let tag = load.release();
                        if let Some((owner, charge)) = sequence::Mode::transport(life, conn.id(), tag) {
                            sequence::Mode::unstage(life, conn.state_mut().async_state.session_mut(), owner, charge);
                        }
                        break match &entry {
                            ControlFlow::Break(_) => turn::Turn::retired(ready, turn::Completion::Rejected),
                            ControlFlow::Continue(_) => match terminal {
                                turn::Readiness::NewlyReady => turn::Turn::TransportCancelled,
                                turn::Readiness::Staged => turn::Turn::retired(terminal, turn::Completion::Rejected),
                            }
                        };
                    }
                };
                if let Some(load) = retry {
                    let staged = runtime::Stream::new(load);
                    match conn.state_mut().async_state.try_stage(staged) {
                        Ok(()) => {
                            let closing = matches!(entry, ControlFlow::Break(_));
                            conn.state_mut().async_state.insert(match entry {
                                ControlFlow::Break(entry) => runtime::Running::Closing(entry.into_id()),
                                ControlFlow::Continue(entry) => runtime::Running::Body(entry.into_id()),
                            });
                            return match ready {
                                turn::Readiness::NewlyReady => turn::Turn::ReadyDeferred,
                                turn::Readiness::Staged if closing => turn::Turn::StagedDeferred,
                                turn::Readiness::Staged => turn::Turn::Deferred,
                            };
                        }
                        Err(staged) => {
                            let tag = staged.release();
                            if let Some((owner, charge)) = sequence::Mode::transport(life, conn.id(), tag) {
                                sequence::Mode::unstage(life, conn.state_mut().async_state.session_mut(), owner, charge);
                            }
                            conn.close_after();
                            break turn::Turn::ConnectionCancelled;
                        }
                    }
                }
                match end {
                    payload::End::More => {}
                    payload::End::Last => break turn::Turn::retired(ready, turn::Completion::Completed),
                }
            };
            match entry { ControlFlow::Break(mut entry) => { #remove } ControlFlow::Continue(mut entry) => { #remove } }
            Self::__release(conn);
            done
        }

        fn __release<#event_life>(conn: &mut #slot) {
            if matches!(#h1::pump::Ingress::deferred(conn.state_mut()), #h1::wire::head::Connection::Close) { conn.close_after(); }
        }
    }
}
