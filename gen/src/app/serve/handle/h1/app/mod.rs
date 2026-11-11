use crate::{macros::tokens::local, runtime::paths::Paths};

mod chunk;
mod report;
use crate::app::serve::{
    duplex,
    handle::h1::{routes, types},
};

pub(super) fn build(
    paths: &Paths,
    types: &types::Types<'_>,
    routes: &routes::Routes,
) -> proc_macro2::TokenStream {
    let [f_core, f_life, f_state] = ["core", "lifecycle", "state"].map(local);
    let symbols = &types.names.generics;
    let [n_link] = symbols.locals(["__connection"]);
    let [n_dprog, n_due] = symbols.locals(["__deadline_progress", "__due"]);
    let [n_exch, n_expiry] = symbols.locals(["__exchange", "__expiry"]);
    let [n_next, n_now, n_out] = symbols.locals(["__next", "__now", "__out"]);
    let [n_phase, n_root, v_app] = symbols.locals(["__phase", "__root", "app"]);
    let [v_cause, v_link] = symbols.locals(["cause", "connection"]);
    let [v_linkid, v_core, v_date] = symbols.locals(["connection_id", "core", "date"]);
    let [v_driver, v_error, v_exch] = symbols.locals(["driver", "error", "exchange"]);
    let [v_result, v_wait, v_ready] = symbols.locals(["outcome", "pending", "ready"]);
    let [v_this] = symbols.locals(["this"]);
    let [l_a, l_w, g_event] = types.names.generics.lives(["__a", "__wire", "e"]);
    let [g_id, g_mode, g_wire] = types.names.generics.idents(["__ID", "__M", "__W"]);
    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    use quote::quote;

    let names = &types.names;
    let name = names.name;
    let state = names.state_ty;
    let retire = quote::format_ident!("__{name}Retirement");
    let core = names.core_ident;
    let bounds = &types.generics.bounds;
    let def = &types.generics.application_params;
    let args = &types.generics.application_generic_use;
    let core_def = &types.generics.core_params;
    let core_args = &types.generics.core_generic_use;
    let proto = &types.protocol;
    let slot = &proto.h1_slot;
    let conn = &proto.connections.http_connection;
    let pending = &proto.connections.pending;
    let egress = &proto.egress_type;
    let source = &proto.input_type;
    let edit = &types.http_mut;
    let audit = &types.audit;
    let tasks = &types.tasks;
    let body = &tasks.send_body;
    let wake = &tasks.wake_body;
    let close = &tasks.close_body;
    let task_methods = &types.task_methods;
    let cancel = &routes.cancel_arms;
    let retire_pending =
        (!cancel.is_empty()).then(|| quote::quote!(#v_exch.retire(#v_this.#f_life, #v_result);));
    let cancelrd = &routes.ready_cancel;
    let ed = match proto.duplex {
        duplex::State::Enabled => quote!(#v_driver),
        duplex::State::Disabled => quote!(_),
    };
    let begin = quote! {
        let #n_link = #v_link.id();
        let #n_now = {
            let #v_this = self.as_mut().project().#f_core.project();
            #ns_h1::server::deadline::Mode::begin(
                #v_this.#f_life,
                #v_driver.driver_ref().clock().now(),
            )
        };
        let mut #n_dprog =
            #ns_h1::server::deadline::Progress::default();
    };
    let finish = quote! {
        let #n_root = {
            let #v_this = self.as_mut().project().#f_core.project();
            #ns_h1::server::deadline::Mode::finish(
                #v_this.#f_life,
                #n_link,
                #n_now,
                #n_dprog.head(),
                #n_dprog.body(),
            )
        };
        if let ::core::result::Result::Err(#v_cause) = #n_root.try_apply(&mut #v_link, #n_now) {
            #v_link.deadline().reject(#v_cause);
        }
    };
    let core = quote! {
        impl<#core_def> #core #core_args
        where
            #bounds
            #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
        {
            #task_methods

            fn send<#g_event>(
                mut self: ::core::pin::Pin<&mut Self>,
                #v_date: &#ns_h1::server::date::Clock,
                #v_link: &mut #slot,
                #v_driver: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
            ) -> #ns_h1::server::deadline::Progress {
                #body
            }

            fn activate<#g_event>(
                mut self: ::core::pin::Pin<&mut Self>,
                #v_date: &#ns_h1::server::date::Clock,
                #v_link: &mut #slot,
                #v_driver: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
            ) {
                #wake
            }

            fn __cancel_pending(self: ::core::pin::Pin<&mut Self>, #v_wait: #pending) {
                let mut #v_this = self.project();
                let #v_exch = #v_wait.exchange;
                let #v_result = match #v_wait.route {
                    #( #cancel, )*
                };
                #retire_pending
            }

            fn close<#g_event>(
                mut self: ::core::pin::Pin<&mut Self>,
                #v_link: &mut #slot,
            ) {
                if let ::core::option::Option::Some(#v_wait) =
                    #edit.ingress.take_pending()
                {
                    self.as_mut().__cancel_pending(#v_wait);
                }
                if let ::core::option::Option::Some(#v_ready) =
                    #edit.ingress.take_ready()
                {
                    let mut #v_this = self.as_mut().project();
                    #cancelrd
                }
                #close
                #edit.retire();
            }
        }

    };
    let application = |mixed: bool, ns_h2: &proc_macro2::TokenStream| {
        let [g_h2a, g_h2f] = symbols.idents(["__H2A", "__H2F"]);
        let f_stamp = local("stamp");
        let mixed_conn = quote::format_ident!("__{name}Cleartext");
        let (def, args) = if mixed {
            (
                &types.generics.mixed_application_params,
                &types.generics.mixed_application_generic_use,
            )
        } else {
            (def, args)
        };
        let source = if mixed && matches!(proto.duplex, duplex::State::Disabled) {
            quote!(#ns_fiber::dope::manifold::app::receive::Scoped<true>)
        } else {
            source.clone()
        };
        let payload = super::super::h1_payload(paths, quote!(()));
        let http2 = quote! {
            #ns_h2::server::runtime::app::App<'d, #g_h2a, #g_wire, #g_h2f, #payload>
        };
        let extra = mixed.then(|| {
            quote! {
                #g_mode: #ns_core::life::sequence::Mode<
                    #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                    TransportTag = (),
                >,
                #http2: #ns_fiber::dope::manifold::network::listener::handler::Application<
                    'd, #g_id, Wire = #g_wire, Egress = #egress,
                > + #ns_fiber::dope::manifold::network::listener::handler::send::Drive<'d, #g_id>,
            }
        });
        let bounds = quote!(#bounds #extra);
        let outer_conn = if mixed {
            quote! {
                #mixed_conn<#conn,
                    <#http2 as #ns_fiber::dope::manifold::network::listener::handler::Application<'d, #g_id>>::Conn>
            }
        } else {
            quote!(#conn)
        };
        let slot = quote! {
            #ns_fiber::dope::manifold::network::listener::conn::Ctx<
                #g_event, '_, '_, '_, '_, 'd, #g_id, #g_wire, #outer_conn, #egress,
            >
        };
        let conn_ref = quote! {
            #ns_fiber::dope::manifold::network::listener::conn::Ref<
                '_, '_, '_, '_, 'd, #g_id, #g_wire, #outer_conn, #egress,
            >
        };
        let mixed_mut = mixed.then(|| quote!(mut));
        let mutable = (mixed || proto.upgrade).then(|| quote!(mut));
        let h2 = quote! {
            ::core::pin::Pin::new(&mut self.as_mut().project().#f_stamp.get_mut().1)
        };
        let h2_ref = quote! {
            ::core::pin::Pin::new(&self.project_ref().#f_stamp.get_ref().1)
        };
        let h1_mut = quote! {
            let mut #v_link = #v_link.map_state(#mixed_conn::h1_mut);
        };
        let h1_ref = quote! {
            let #v_link = #v_link.map_state(#mixed_conn::h1);
        };
        let enter = |native: Option<proc_macro2::TokenStream>,
                     otherwise: proc_macro2::TokenStream,
                     projection: &proc_macro2::TokenStream| {
            mixed.then(|| {
                let native = native.map(|call| {
                    quote! {
                        #mixed_conn::Http2(_) => return #call,
                    }
                });
                quote! {
                    match #v_link.state() {
                        #mixed_conn::Http1(_) => {}
                        #native
                        _ => return #otherwise,
                    }
                    #projection
                }
            })
        };
        let http_only = enter(None, quote!(()), &h1_mut);
        let query_enter = enter(None, quote!(false), &h1_ref);
        let timeout_enter = enter(
            Some(quote! {
                #ns_fiber::dope::manifold::network::listener::handler::Application::timeout(
                    #h2, #v_link.map_state(#mixed_conn::h2_mut), #v_driver,
                )
            }),
            quote!(#ns_fiber::dope::manifold::network::listener::handler::Timeout::Discard),
            &quote!(let #v_link = #v_link.map_state(#mixed_conn::h1_mut);),
        );
        let drain_enter = enter(
            Some(quote! {
                #ns_fiber::dope::manifold::network::listener::handler::Application::drain(
                    #h2, #v_link.map_state(#mixed_conn::h2_mut), #v_driver,
                )
            }),
            quote!(#ns_fiber::dope::manifold::network::listener::handler::Drain::Discard),
            &quote!(let #v_link = #v_link.map_state(#mixed_conn::h1_mut);),
        );
        let gate_enter = enter(
            Some(quote! {
                #ns_fiber::dope::manifold::network::listener::handler::Application::close_gate(
                    #h2_ref, #v_link.map_state(#mixed_conn::h2),
                )
            }),
            quote!(#ns_fiber::dope::net::link::slot::types::CloseGate::Immediate),
            &h1_ref,
        );
        let send_enter = enter(
            Some(quote! {
                #ns_fiber::dope::manifold::network::listener::handler::send::Drive::send(
                    #h2, #v_link.map_state(#mixed_conn::h2_mut), #v_driver,
                )
            }),
            quote!(()),
            &h1_mut,
        );
        let activation_enter = enter(
            Some(quote! {
                #ns_fiber::dope::manifold::network::listener::handler::Application::activate(
                    #h2, #v_link.map_state(#mixed_conn::h2_mut), #v_driver,
                )
            }),
            quote!(()),
            &h1_mut,
        );
        let closing_enter = enter(
            Some(quote! {
                #ns_fiber::dope::manifold::network::listener::handler::Application::close(
                    #h2, #v_link.map_state(#mixed_conn::h2_mut),
                )
            }),
            quote!(()),
            &h1_mut,
        );
        let timing = mixed.then(|| {
            quote! {
                fn timing(&self) -> #ns_fiber::dope::manifold::network::listener::handler::Timing {
                    #ns_fiber::dope::manifold::network::listener::handler::Timing::managed()
                }
            }
        });
        let is_gated = mixed.then(|| {
            let enabled = matches!(proto.duplex, duplex::State::Enabled);
            quote! {
                fn is_gated(#v_link: &Self::Conn) -> ::core::primitive::bool {
                    match #v_link {
                        #mixed_conn::Http1(_) => #enabled,
                        #mixed_conn::Http2(#v_link) => <#http2 as
                            #ns_fiber::dope::manifold::network::listener::handler::Application<'d, #g_id>
                        >::is_gated(#v_link),
                        #mixed_conn::Pending(_) => false,
                    }
                }
            }
        });
        let connection = if mixed {
            quote!(::core::result::Result::Ok(#mixed_conn::Pending(::core::default::Default::default())))
        } else {
            quote!(::core::result::Result::Ok(
                ::core::default::Default::default()
            ))
        };
        let retirement = if mixed {
            quote!(#ns_fiber::dope::manifold::network::listener::handler::retire::Close)
        } else {
            quote!(#retire)
        };
        let send_policy = if mixed {
            quote!(#ns_fiber::dope::manifold::network::listener::handler::send::Run)
        } else {
            quote!(#ns_fiber::dope::manifold::network::listener::handler::send::Reported<
                #ns_fiber::dope::manifold::network::listener::handler::send::Run,
            >)
        };
        let (send, start, eof, gate, recv, stop, input, gated, scratch) = {
            let dispatch = &types.names.dispatch_ident;
            let v_routes = symbols.local("routes");
            let [v_reason, v_slot] = symbols.locals(["reason", "slot"]);
            let [n_drive] = symbols.locals(["__drive"]);
            let [n_prog, v_action] = symbols.locals(["__progress", "action"]);
            let [v_budget, v_bytes, v_chunk] = symbols.locals(["budget", "bytes", "chunk"]);
            let v_prefix = symbols.local("prefix");
            let [v_raw, v_tail] = symbols.locals(["raw", "tail"]);
            let l_wire = types.names.generics.lifetime("__w");
            let g_r = types.names.generics.ident("__R");
            let ns_http = &paths.http;

            let ns_h2 = types.h2;
            let [v_pending, v_count, v_complete] = symbols.locals(["pending", "count", "complete"]);
            let [v_state, v_h2] = symbols.locals(["state", "h2"]);
            let chunk_mut = mixed.then(|| quote!(mut));
            let clock = if mixed {
                quote!(#v_date.as_ref().get_ref().0)
            } else {
                proto.duplex.clock(types.names.generics)
            };
            let stamp = mixed.then(|| quote!(.0));
            let scratch = if proto.upgrade {
                quote!(&mut self.as_mut().project().#f_stamp.get_mut()#stamp.2)
            } else {
                quote!(&mut ::std::vec::Vec::new())
            };
            let duplex = proto.duplex;
            let slot = &proto.h1_slot;
            let scoped_bounds = mixed.then(|| {
                quote! {
                    #g_h2a: #ns_core::manifold::server::app::Application<
                        'd, #ns_h2::server::runtime::Connection<'d, #g_h2a>,
                        #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                    >,
                    #http2: #ns_fiber::dope::manifold::network::listener::handler::Scoped<
                        #l_wire, 'd, #g_id, Wire = #g_wire, Egress = #egress,
                        Conn = #ns_h2::server::runtime::app::State<
                            'd, #g_h2a,
                            #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                        >,
                        Build = #ns_h2::server::runtime::Build<#g_h2a::Build>,
                    >,
                }
            });
            let blocked_enter = enter(
                Some(quote! {
                    #ns_fiber::dope::manifold::network::listener::handler::Application::is_blocked(
                        #h2_ref, #v_link.map_state(#mixed_conn::h2),
                    )
                }),
                quote!(false),
                &h1_ref,
            );
            let http_drained = enter(
                None,
                quote!(#ns_fiber::dope::manifold::network::listener::handler::PostDrain::Idle),
                &h1_mut,
            );
            let h1_stamp = mixed.then(|| {
                quote! {
                    let #v_date = ::core::pin::Pin::new(&mut #v_date.get_mut().0);
                }
            });
            let retention = if mixed {
                quote!(self.#f_stamp.0.1.max(
                    <#http2 as #ns_fiber::dope::manifold::network::listener::handler::Scoped<#l_wire, 'd, #g_id>>::retention(&self.#f_stamp.1),
                ))
            } else {
                quote!(self.#f_stamp.1)
            };
            let tag = quote! {
                <#g_mode as #ns_core::life::sequence::Mode<
                    #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                >>::TransportTag
            };
            let send = quote! {
                let #v_date = self.as_mut().project();
                let (mut #v_core, #v_date) = (#v_date.#f_core, #v_date.#f_stamp);
                #h1_stamp
                #n_dprog.merge(#v_core.as_mut().send(
                    #clock,
                    &mut #v_link,
                    #v_driver,
                ));
            };
            let send = match duplex {
                duplex::State::Enabled => quote! {
                    match #v_link.state().mode() {
                        #ns_h1::switch::Mode::Http
                        | #ns_h1::switch::Mode::Switching(_) => {
                            #send
                        }
                        #ns_h1::switch::Mode::Raw(_) => {}
                    }
                },
                duplex::State::Disabled => send,
            };
            let start = match duplex {
                duplex::State::Enabled => quote! {
                    match #v_link.state().mode() {
                        #ns_h1::switch::Mode::Raw(_) => {
                            #ns_h1::server::duplex::Runtime::<_, #tag>::new(&mut #v_link, #scratch)
                                .activate(#v_driver);
                        }
                        #ns_h1::switch::Mode::Http
                        | #ns_h1::switch::Mode::Switching(_) => {
                        let #v_action = #v_link.state().action();
                        let #v_date = self.as_mut().project();
                        let (mut #v_core, #v_date) = (#v_date.#f_core, #v_date.#f_stamp);
                    #h1_stamp
                        match #v_action {
                            ::core::option::Option::Some(_) => #n_dprog.merge(#v_core.as_mut().send(
                                    #clock,
                                    &mut #v_link,
                                    #v_driver,
                                )),
                            ::core::option::Option::None => {
                                let #n_drive = {
                                    let mut #v_routes = #dispatch {
                                        app: #v_core.as_mut(),
                                        id: #v_link.id(),
                                        date: #clock,
                                    };
                                    #ns_h1::server::conn::driver::resume(
                                        ::core::pin::Pin::new(&mut #v_routes),
                                        &mut #v_link,
                                        #v_driver,
                                    )
                                };
                                let (_, #n_prog) = #n_drive.into_parts();
                                #n_dprog.merge(#n_prog);
                            }
                        }
                        #v_core.as_mut().activate(
                            #clock,
                            &mut #v_link,
                            #v_driver,
                        );
                        }
                    }
                },
                duplex::State::Disabled => quote! {
                    let #v_date = self.as_mut().project();
                    let (mut #v_core, #v_date) = (#v_date.#f_core, #v_date.#f_stamp);
                    #h1_stamp
                    if #v_link.state().action().is_some()
                        || #v_link.state().can_resume()
                    {
                        #n_dprog.merge(#v_core.as_mut().send(
                            #clock,
                            &mut #v_link,
                            #v_driver,
                        ));
                    } else {
                        #v_core.as_mut().activate(
                            #clock,
                            &mut #v_link,
                            #v_driver,
                        );
                    }
                },
            };
            let pending = quote! {
                #v_link.state().can_resume()
                    || #v_link.state().action().is_some()
                    || ::core::matches!(
                        #ns_h1::pump::Tasks::activity(&#v_link.state().async_state),
                        #ns_h1::pump::Activity::Task | #ns_h1::pump::Activity::Stream
                    )
            };
            let eof = match duplex {
                duplex::State::Enabled => quote! {
                    if !(#pending) && ::core::matches!(
                        #v_link.state().mode(),
                        #ns_h1::switch::Mode::Raw(_)
                    ) {
                        #ns_h1::server::duplex::Runtime::<_, #tag>::new(&mut #v_link, #scratch)
                            .finish_input(#v_driver);
                    }
                },
                duplex::State::Disabled => proc_macro2::TokenStream::new(),
            };
            let eof = quote! {
                #v_link.state_mut().ingress.end_discard();
                #eof
            };
            let gate = match duplex {
                duplex::State::Enabled => quote! {
                    match #v_link.state().raw() {
                        ::core::result::Result::Ok(#v_raw) => match #v_raw.progress() {
                            #ns_core::duplex::machine::Progress::Finished => {
                                #ns_fiber::dope::net::link::slot::types::CloseGate::Immediate
                            }
                            #ns_core::duplex::machine::Progress::Idle
                            | #ns_core::duplex::machine::Progress::Turn => {
                                #ns_fiber::dope::net::link::slot::types::CloseGate::Deferred
                            }
                        },
                        ::core::result::Result::Err(#ns_h1::switch::Mode::Http) => {
                            #ns_fiber::dope::net::link::slot::types::CloseGate::Immediate
                        }
                        ::core::result::Result::Err(#ns_h1::switch::Mode::Switching(_)) => {
                            #ns_fiber::dope::net::link::slot::types::CloseGate::Deferred
                        }
                    }
                },
                duplex::State::Disabled => {
                    quote!(#ns_fiber::dope::net::link::slot::types::CloseGate::Immediate)
                }
            };
            let gate = quote! {
                if #v_link.state().ingress.is_discard() || #pending {
                    #ns_fiber::dope::net::link::slot::types::CloseGate::Deferred
                } else {
                    #gate
                }
            };
            let recv = match duplex {
                duplex::State::Enabled => quote! {
                        match #v_link.state().raw() {
                            ::core::result::Result::Ok(#v_raw) => {
                                ::core::matches!(#v_raw.hold(), #ns_core::duplex::machine::Hold::Held)
                                    || ::core::matches!(#v_raw.access(), #ns_core::duplex::machine::Access::Blocked)
                            }
                            ::core::result::Result::Err(#ns_h1::switch::Mode::Http) => false,
                            ::core::result::Result::Err(#ns_h1::switch::Mode::Switching(_)) => true,
                        }
                },
                duplex::State::Disabled => quote!(false),
            };
            let recv = (mixed || matches!(duplex, duplex::State::Enabled)).then(|| {
                quote! {
                    fn is_blocked(
                        self: ::core::pin::Pin<&Self>,
                        #v_link: #conn_ref,
                    ) -> ::core::primitive::bool {
                        #blocked_enter
                        #recv
                    }
                }
            });
            let close = match duplex {
                duplex::State::Enabled => quote! {
                    match #v_link.state_mut().raw_mut() {
                        ::core::result::Result::Ok(#v_raw) => #v_raw.retire(),
                        ::core::result::Result::Err(#ns_h1::switch::Mode::Http
                        | #ns_h1::switch::Mode::Switching(_)) => {
                            #v_core.as_mut().close(&mut #v_link)
                        }
                    }
                },
                duplex::State::Disabled => quote!(#v_core.as_mut().close(&mut #v_link);),
            };
            let v_input = symbols.local("input");
            let merged_scratch = if proto.upgrade {
                quote!(&mut #v_app.as_mut().project().#f_stamp.get_mut()#stamp.2)
            } else {
                quote!(&mut self.scratch)
            };
            let drive = chunk::render(paths, types, &merged_scratch, stamp.as_ref());
            let selection = mixed.then(|| {
                quote! {
                    enum #mixed_conn<H, C> {
                        Pending(#ns_http::preface::Prefix),
                        Http1(H),
                        Http2(C),
                    }

                    impl<H, C> #mixed_conn<H, C> {
                        fn h1(&self) -> &H {
                            match self {
                                Self::Http1(state) => state,
                                _ => ::core::unreachable!("protocol"),
                            }
                        }

                        fn h1_mut(&mut self) -> &mut H {
                            match self {
                                Self::Http1(state) => state,
                                _ => ::core::unreachable!("protocol"),
                            }
                        }

                        fn h2(&self) -> &C {
                            match self {
                                Self::Http2(state) => state,
                                _ => ::core::unreachable!("protocol"),
                            }
                        }

                        fn h2_mut(&mut self) -> &mut C {
                            match self {
                                Self::Http2(state) => state,
                                _ => ::core::unreachable!("protocol"),
                            }
                        }
                    }
                }
            });
            let select = mixed.then(|| quote! {
                use #ns_http::o3::buffer::write::consume::Consume;
                use #ns_fiber::dope::manifold::network::listener::handler::Scoped as _;
                use #ns_fiber::dope::net::wire::receive::scoped::Input;

                match #v_link.state() {
                    #mixed_conn::Http1(_) => {}
                    #mixed_conn::Http2(_) => {
                        let #v_h2 = &mut self.as_mut().project().#f_stamp.get_mut().1;
                        return ::core::pin::Pin::new(#v_h2).scoped_chunk(
                            #v_link.map_state(#mixed_conn::h2_mut), Input::new(#v_chunk, #v_budget), #v_driver,
                        );
                    }
                    #mixed_conn::Pending(_) => {
                        if #ns_http::o3::buffer::Source::remaining(#v_chunk) == 0 {
                            return Outcome::Continue;
                        }
                        let #v_slot = #v_link.state_mut();
                        let #mixed_conn::Pending(#v_prefix) = #v_slot else {
                            ::core::unreachable!("protocol")
                        };
                        let #v_pending = !#v_prefix.as_ref().is_empty();
                        match ::core::mem::take(#v_prefix).read(#v_chunk.as_ref()) {
                            ::core::result::Result::Ok((
                                #v_count, #ns_http::preface::Read::Pending(#v_state),
                            )) => {
                                #v_chunk.consume_max(#v_count);
                                *#v_prefix = #v_state;
                                if !#v_pending
                                    && let ::core::result::Result::Err(#v_cause) = #v_link
                                        .deadline()
                                        .try_ensure(#v_driver.driver_ref().clock().now())
                                {
                                    #v_link.deadline().reject(#v_cause);
                                    return Outcome::CloseAfter;
                                }
                                return Outcome::Continue;
                            }
                            ::core::result::Result::Err(#v_cause) => {
                                *#v_slot = #mixed_conn::Http1(
                                    #ns_h1::session::Conn::prefixed(#v_cause.into_parts().0),
                                );
                            }
                            ::core::result::Result::Ok((
                                #v_count, #ns_http::preface::Read::Complete(#v_complete),
                            )) => {
                                #v_chunk.consume_max(#v_count);
                                let #v_h2 = &mut self.as_mut().project().#f_stamp.get_mut().1;
                                match #v_h2.connection_after_preface::<#g_id>(
                                    #v_driver.driver_ref().random_seed(), #v_complete,
                                ) {
                                    ::core::result::Result::Ok(#v_state) => {
                                        *#v_slot = #mixed_conn::Http2(#v_state);
                                    }
                                    ::core::result::Result::Err(#v_cause) => {
                                        <#http2 as #ns_fiber::dope::manifold::network::listener::handler::Application<'d, #g_id>>::build_failed(
                                            ::core::pin::Pin::new(#v_h2), #v_cause,
                                        );
                                        return Outcome::CloseAfter;
                                    }
                                }
                                if #v_pending {
                                    #v_link.deadline().cancel();
                                }
                                return ::core::pin::Pin::new(#v_h2).scoped_chunk(
                                    #v_link.map_state(#mixed_conn::h2_mut), Input::new(#v_chunk, #v_budget), #v_driver,
                                );
                            }
                        }
                    }
                }
                let mut #v_link = #v_link.map_state(#mixed_conn::h1_mut);
            });
            let input = match (duplex, mixed) {
                (duplex::State::Enabled, _) | (duplex::State::Disabled, true) => {
                    let scratch = matches!(duplex, duplex::State::Enabled) && !proto.upgrade;
                    let scratch_field =
                        scratch.then(|| quote!(scratch: ::std::vec::Vec<::core::primitive::u8>,));
                    let scratch_init = scratch.then(|| quote!(scratch: ::std::vec::Vec::new(),));
                    let read = quote::format_ident!("{}Read", name);
                    let [r_life, turn_life] = symbols.lives(["__read", "__driver"]);
                    let [n_head, n_count, n_whole, n_end] =
                        symbols.locals(["__head", "__count", "__whole", "__end"]);
                    let [n_out, n_cause] = symbols.locals(["__outcome", "__cause"]);
                    let [n_raw, n_prefix, n_failed] =
                        symbols.locals(["__raw", "__prefix", "__failed"]);
                    let read_type = (!mixed || matches!(duplex, duplex::State::Disabled)).then(|| quote! {
                        struct #read<#r_life, #turn_life, 'd, A, C> {
                            app: ::core::pin::Pin<&#r_life mut A>,
                            conn: &#r_life mut C,
                            driver: &#r_life mut #ns_fiber::dope::core::driver::Context<#turn_life, 'd>,
                            budget: &#r_life #ns_http::o3::buffer::resident::Budget<'d>,
                            #scratch_field
                        }
                    });
                    let (raw_import, whole, dispatch, progress) = match duplex {
                        duplex::State::Enabled => (
                            quote!(use #ns_h1::{server::duplex::Runtime, switch::Mode};),
                            quote!(#n_whole),
                            quote! {
                                match #v_link.state().mode() {
                                    Mode::Raw(_) => Runtime::new(
                                        #v_link, #merged_scratch,
                                    ).chunk(#n_whole, #v_driver),
                                    Mode::Switching(_) => {
                                        return (::core::option::Option::None, ControlFlow::Break(Outcome::Continue));
                                    }
                                    Mode::Http => {
                                        let #v_bytes = #v_input.as_slice();
                                        #drive
                                    }
                                }
                            },
                            quote! {
                                match self.conn.state().raw() {
                                    ::core::result::Result::Ok(#n_raw) => match #n_raw.access() {
                                        #ns_core::duplex::machine::Access::Vacant(()) => {
                                            ControlFlow::Continue(#n_out)
                                        }
                                        #ns_core::duplex::machine::Access::Blocked => ControlFlow::Break(#n_out),
                                    },
                                    ::core::result::Result::Err(Mode::Http) => ControlFlow::Continue(#n_out),
                                    ::core::result::Result::Err(Mode::Switching(_)) => ControlFlow::Break(#n_out),
                                }
                            },
                        ),
                        duplex::State::Disabled => (
                            quote!(),
                            quote!(_),
                            quote!({ let #v_bytes = #v_input.as_slice(); #drive }),
                            quote!(ControlFlow::Continue(#n_out)),
                        ),
                    };
                    let local_read = quote! {
                        #read_type
                        impl<#g_event, #def>
                            #ns_fiber::dope::net::wire::receive::mode::access::Use<
                                'd, #ns_fiber::dope::manifold::app::receive::Outcome,
                            > for #read<'_, '_, 'd, #name #args, #slot>
                        where
                            #bounds
                            #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
                        {
                            fn apply<#g_r>(&mut self, #n_head: &mut #g_r) -> ::core::ops::ControlFlow<
                                #ns_fiber::dope::manifold::app::receive::Outcome,
                                #ns_fiber::dope::manifold::app::receive::Outcome,
                            >
                            where
                                #g_r: #ns_http::o3::buffer::Complete
                                    + #ns_http::o3::buffer::hold::raw::Retain<'d>
                                    + #ns_http::o3::buffer::write::consume::Consume
                                    + #ns_http::o3::buffer::raw::Consume,
                            {
                                use ::core::ops::ControlFlow;
                                use #ns_fiber::dope::manifold::app::receive::{Failure, Outcome};
                                use #ns_fiber::dope::net::link::egress;
                                #raw_import
                                use #ns_http::o3::buffer::hold::scope::Input;

                                let #v_app = &mut self.app;
                                let #v_link = &mut *self.conn;
                                let #v_driver = &mut *self.driver;
                                let #n_out = Input::consume(#n_head, self.budget, |#v_input| {
                                    let ::core::option::Option::Some(#n_count) =
                                        ::core::num::NonZero::new(#v_input.as_slice().len())
                                    else {
                                        return (::core::option::Option::None, ControlFlow::Break(Outcome::Continue));
                                    };
                                    let (#whole, #n_end) = match #v_input.try_prefix::<{
                                        #ns_fiber::dope::core::io::transfer::MAX_BYTES as u32
                                    }>(0, #n_count) {
                                        ::core::result::Result::Ok(#n_prefix) => #n_prefix,
                                        ::core::result::Result::Err(#n_cause) => {
                                            return (
                                                ::core::option::Option::None,
                                                ControlFlow::Break(Outcome::Failed(Failure::Input(#n_cause))),
                                            );
                                        }
                                    };
                                    let #n_out = #dispatch;
                                    match #n_out {
                                        Outcome::Continue => (
                                            ::core::option::Option::Some(#n_end),
                                            ControlFlow::Continue(Outcome::Continue),
                                        ),
                                        #n_failed @ (Outcome::CloseAfter | Outcome::Failed(_)) => {
                                            (::core::option::Option::None, ControlFlow::Break(#n_failed))
                                        }
                                    }
                                });
                                let ControlFlow::Continue(#n_out) = #n_out else {
                                    return #n_out;
                                };
                                if self.conn.write().pending() == egress::Pending::Present {
                                    return ControlFlow::Break(#n_out);
                                }
                                #progress
                            }
                        }
                    };
                    quote! {
                        #selection
                        #local_read
                    impl<#l_wire, #def>
                        #ns_fiber::dope::manifold::network::listener::handler::Scoped<#l_wire, 'd, #g_id>
                        for #name #args
                    where
                        #bounds
                        'd: #l_wire,
                        #scoped_bounds
                        #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
                    {
                        fn retention(&self) -> #ns_fiber::dope::manifold::app::receive::limits::Budget {
                            #retention
                        }

                        fn scoped_chunk<#g_event>(
                            #chunk_mut self: ::core::pin::Pin<&mut Self>,
                            mut #v_link: #ns_fiber::dope::manifold::network::listener::conn::Ctx<
                                #g_event,
                                '_,
                                '_,
                                '_,
                                #l_wire,
                                'd,
                                 #g_id,
                                #g_wire,
                                #outer_conn,
                                #egress,
                            >,
                            #v_chunk: #ns_fiber::dope::net::wire::receive::scoped::Input<'_, #l_wire, 'd, #g_wire>,
                            #v_driver: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
                        ) -> #ns_fiber::dope::manifold::app::receive::Outcome {
                            use #ns_fiber::dope::manifold::app::receive::Outcome;
                            let (#v_chunk, #v_budget) = #v_chunk.into_parts();
                            #select
                            #v_chunk.drive(Outcome::Continue, &mut #read {
                                app: self,
                                conn: &mut #v_link,
                                driver: #v_driver,
                                budget: #v_budget,
                                #scratch_init
                            })
                        }
                    }


                    }
                }
                (duplex::State::Disabled, false) => quote! {
                    impl<#def>
                        #ns_fiber::dope::manifold::network::listener::handler::Borrowed<'d, #g_id>
                        for #name #args
                    where
                        #bounds
                        #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
                    {
                        /// Handles one borrowed input chunk: the borrowing callback
                        /// cannot retain a raw suffix, so the host's handoff is
                        /// unrepresentable here.
                        fn chunk<#g_event, #g_r: #ns_http::o3::buffer::bytes::TryRetain>(
                            self: ::core::pin::Pin<&mut Self>,
                            mut #v_link: #slot,
                            #v_chunk: #g_r,
                            #v_driver: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
                        ) -> #ns_fiber::dope::manifold::app::receive::Outcome {
                        use #ns_fiber::dope::manifold::app::receive::Outcome;
                        let mut #v_app = self;
                        let #v_link = &mut #v_link;
                        let #v_bytes = #v_chunk.as_ref();
                        #drive
                    }
                    }
                },
            };
            let progress = quote! {
                match #v_raw.progress() {
                    #ns_core::duplex::machine::Progress::Turn => {
                        #ns_fiber::dope::manifold::network::listener::handler::PostDrain::Activate
                    }
                    #ns_core::duplex::machine::Progress::Idle
                    | #ns_core::duplex::machine::Progress::Finished => {
                        #ns_fiber::dope::manifold::network::listener::handler::PostDrain::Idle
                    }
                }
            };
            let drains = mixed.then(|| {
                quote! {
                    fn drains(#v_link: &Self::Conn) -> ::core::primitive::bool {
                        ::core::matches!(#v_link, #mixed_conn::Http1(_))
                    }
                }
            });
            let gated = match duplex {
                duplex::State::Enabled => quote! {
                    impl<#def>
                        #ns_fiber::dope::manifold::network::listener::handler::Gated<'d, #g_id>
                        for #name #args
                    where
                        #bounds
                        #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
                    {
                        #drains

                        fn send_drained<#g_event>(
                            self: ::core::pin::Pin<&mut Self>,
                            mut #v_link: #ns_fiber::dope::manifold::network::listener::conn::Drained<
                                #g_event,
                                '_,
                                '_,
                                '_,
                                '_,
                                'd,
                                 #g_id,
                                #g_wire,
                                #outer_conn,
                                #egress,
                            >,
                        ) -> #ns_fiber::dope::manifold::network::listener::handler::PostDrain {
                            #http_drained
                            match #ns_h1::pump::Drain::complete(#v_link.state_mut()) {
                                    #ns_h1::pump::Completion::None => {
                                        match #v_link.state().raw() {
                                            ::core::result::Result::Ok(#v_raw) => { #progress }
                                            ::core::result::Result::Err(#ns_h1::switch::Mode::Http
                                            | #ns_h1::switch::Mode::Switching(_)) => {
                                                #ns_fiber::dope::manifold::network::listener::handler::PostDrain::Idle
                                            }
                                        }
                                    }
                                    #ns_h1::pump::Completion::Resume => {
                                        #ns_fiber::dope::manifold::network::listener::handler::PostDrain::Activate
                                    }
                                    #ns_h1::pump::Completion::Close(#v_reason) => {
                                        #v_link.state_mut().defer(#v_reason);
                                        #ns_fiber::dope::manifold::network::listener::handler::PostDrain::Activate
                                    }
                                    #ns_h1::pump::Completion::Switch => {
                                        let #v_tail = #v_link.state_mut().take_input();
                                        match #v_link.state_mut().raw_mut() {
                                            ::core::result::Result::Ok(#v_raw) => {
                                                match #v_tail {
                                                    ::core::option::Option::Some(#v_tail) => match #v_raw.input() {
                                                        #ns_core::duplex::machine::Access::Vacant(#v_slot) => {
                                                            #v_slot.commit(#ns_http::payload::owned::Segments::from(#v_tail));
                                                            { #progress }
                                                        }
                                                        #ns_core::duplex::machine::Access::Blocked => {
                                                            #v_link.close_after();
                                                            #ns_fiber::dope::manifold::network::listener::handler::PostDrain::Idle
                                                        }
                                                    },
                                                    ::core::option::Option::None => { #progress },
                                                }
                                            }
                                            ::core::result::Result::Err(#ns_h1::switch::Mode::Http
                                            | #ns_h1::switch::Mode::Switching(_)) => {
                                                #v_link.close_after();
                                                #ns_fiber::dope::manifold::network::listener::handler::PostDrain::Idle
                                            }
                                        }
                                    }
                            }
                        }
                    }
                },
                duplex::State::Disabled => proc_macro2::TokenStream::new(),
            };
            (send, start, eof, gate, recv, close, input, gated, scratch)
        };
        let close_body = quote! {
                let #v_linkid = #v_link.id();
                let mut #v_core = self.project().#f_core;
                {
                    let #v_this = #v_core.as_mut().project();
                    #ns_core::life::Mode::observe(
                        #v_this.#f_life,
                        #ns_core::life::Event::SessionClosing(#v_linkid),
                    );
                }
                #stop
        };
        let accept = (!mixed).then(|| {
            quote! {
                fn accept<#g_event>(
                    self: ::core::pin::Pin<&mut Self>,
                    #v_link: #slot,
                ) -> #ns_fiber::dope::manifold::app::receive::Outcome {
                    let #v_this = self.project().#f_core.project();
                    let #v_link = #v_link.id();
                    #ns_core::life::Mode::observe(
                        #v_this.#f_life,
                        #ns_core::life::Event::SessionOpened(#v_link),
                    );
                    #ns_core::life::Mode::observe(
                        #v_this.#f_life,
                        #ns_core::life::Event::SessionEstablished(#v_link),
                    );
                    #ns_fiber::dope::manifold::app::receive::Outcome::Continue
                }

            }
        });
        let retirement_impl = (!mixed).then(|| {
            quote! {
            struct #retire;

            impl<#def>
                #ns_fiber::dope::manifold::network::listener::handler::retire::Policy<
                    'd,
                    #name #args,
                     #g_id,
                > for #retire
            where
                #bounds
                #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
            {
                fn select() -> ::core::option::Option<Self> {
                    ::core::option::Option::Some(Self)
                }

                fn retire(
                    self,
                    #v_app: ::core::pin::Pin<&mut #name #args>,
                    mut #v_link: #ns_fiber::dope::manifold::network::listener::conn::Retired<
                        '_,
                        '_,
                        'd,
                         #g_id,
                        #g_wire,
                        #conn,
                        #egress,
                    >,
                ) {
                    let #v_linkid = #v_link.id();
                    let #v_this = #v_app.project().#f_core.project();
                    #ns_core::life::sequence::Mode::close(
                        #v_this.#f_life,
                        #v_link.state_mut().async_state.session_mut(),
                        #v_linkid,
                    );
                    #ns_core::life::Mode::close(
                        #v_this.#f_life,
                        #v_linkid,
                        || #audit,
                    );
                }
            }

            }
        });
        let failure_impl = (!mixed).then(|| quote! {
        impl<#l_w, #def>
            #ns_fiber::dope::manifold::network::listener::handler::failures::Policy<
                #l_w,
                'd,
                Self,
                 #g_id,
            > for #name #args
        where
            #bounds
            'd: #l_w,
            #state: #ns_h1::server::listener::failure::Policy<#g_wire>,
            #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
        {
            fn open_failed(
                #v_app: ::core::pin::Pin<&mut Self>,
                #v_cause: #ns_fiber::dope::net::wire::open::Error<#l_w, #g_wire>,
            ) {
                let #v_this = #v_app.project().#f_core.project();
                #ns_h1::server::listener::failure::Policy::open_failed(*#v_this.#f_state, #v_cause);
            }

            /// A malformed peer address is a platform fault with no wire failure
            /// class to carry it, so the accepted socket is closed without any
            /// application-visible cause.
            fn peer() -> ::core::option::Option<
                impl for<#l_a> #ns_fiber::o3::base::Call<(
                    ::core::pin::Pin<&#l_a mut Self>,
                    #ns_fiber::dope::core::io::socket::Error,
                )>,
            > {
                ::core::option::Option::None::<::core::convert::Infallible>
            }

            fn wire_failed<#g_event>(
                #v_app: ::core::pin::Pin<&mut Self>,
                #v_link: #ns_fiber::dope::manifold::network::listener::conn::Ctx<
                    #g_event,
                    '_,
                    '_,
                    '_,
                    #l_w,
                    'd,
                     #g_id,
                    #g_wire,
                    #conn,
                    #egress,
                >,
                #v_cause: #ns_fiber::dope::net::wire::terminal::Error<#l_w, #g_wire>,
            ) {
                let #v_this = #v_app.project().#f_core.project();
                #ns_h1::server::listener::failure::Policy::wire_failed(
                    *#v_this.#f_state,
                    #v_link.id(),
                    #v_cause,
                );
            }
        }

        });
        let wire_failure = if mixed {
            quote!(#ns_fiber::dope::manifold::network::listener::handler::failures::Close)
        } else {
            quote!(Self)
        };
        let report = (!mixed).then(|| report::render(paths, types));
        quote! {
        impl<#def> #ns_fiber::dope::manifold::network::listener::handler::Application<'d, #g_id>
            for #name #args
        where
            #bounds
            #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
        {
            type Conn = #outer_conn;
            type Build = ::core::convert::Infallible;
            type Wire = #g_wire;
            type Retirement = #retirement;
            type WireFailure = #wire_failure;
            type Egress = #egress;
            type Input = #source;
            type Send = #send_policy;

            fn connection(
                self: ::core::pin::Pin<&Self>,
                _: &#ns_fiber::dope::core::random::Seed,
            ) -> ::core::result::Result<Self::Conn, Self::Build> {
                #connection
            }

            fn build_failed(self: ::core::pin::Pin<&mut Self>, #v_error: Self::Build) {
                match #v_error {}
            }

            #recv
            #timing
            #is_gated

            fn deadline(
                self: ::core::pin::Pin<&Self>,
                _: #ns_fiber::dope::core::driver::schedule::clock::Clock<'_, 'd>,
            ) -> ::core::option::Option<
                #ns_fiber::dope::core::driver::schedule::clock::Deadline<'d>
            > {
                ::core::option::Option::None
            }

            fn discarded<#g_event>(
                mut self: ::core::pin::Pin<&mut Self>,
                mut #v_link: #slot,
                #v_driver: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
            ) {
                #http_only
                #begin
                #n_dprog.merge(#ns_h1::server::deadline::Progress::new(
                    #ns_h1::server::deadline::Head::Ready,
                    #ns_h1::server::deadline::Body::None,
                ));
                #finish
            }

            fn timeout<#g_event>(
                mut self: ::core::pin::Pin<&mut Self>,
                #mixed_mut #v_link: #slot,
                #v_driver: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
            ) -> #ns_fiber::dope::manifold::network::listener::handler::Timeout<'d> {
                #timeout_enter
                let #n_link = #v_link.id();
                let #n_now = #v_driver.driver_ref().clock().now();
                let #n_due = {
                    let #v_this = self.as_mut().project().#f_core.project();
                    #ns_h1::server::deadline::Mode::timeout(
                        #v_this.#f_life,
                        #n_link,
                        #n_now,
                    )
                };
                let ::core::option::Option::Some(#n_expiry) = #n_due.expiry else {
                    let ::core::option::Option::Some(#n_next) =
                        #n_due.root.resolve(#n_now)
                    else {
                        return #ns_fiber::dope::manifold::network::listener::handler::Timeout::Discard;
                    };
                    return #ns_fiber::dope::manifold::network::listener::handler::Timeout::Continue(
                        #n_next,
                    );
                };
                match #n_expiry {
                    #ns_h1::server::deadline::Expiry::Connection => {}
                    #ns_h1::server::deadline::Expiry::Exchange {
                        exchange: #n_exch,
                        phase: #n_phase,
                    } => {
                        let #v_this = self.as_mut().project().#f_core.project();
                        if let ::core::option::Option::Some(#n_out) =
                            #ns_core::life::Mode::observer(#v_this.#f_life)
                        {
                            #ns_core::life::Observer::observe(
                                #n_out,
                                #ns_core::life::Event::ExchangeDeadlineExpired {
                                    exchange: #n_exch,
                                    phase: #n_phase,
                                },
                            );
                        }
                    }
                }
                #ns_fiber::dope::manifold::network::listener::handler::Timeout::Discard
            }

            fn drain<#g_event>(
                #mutable self: ::core::pin::Pin<&mut Self>,
                #mixed_mut #v_link: #slot,
                #v_driver: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
            ) -> #ns_fiber::dope::manifold::network::listener::handler::Drain {
                #drain_enter
                <#conn as
                    #ns_h1::server::conn::lifetime::Drain<
                        'd,
                        #g_id,
                        #egress,
                        <#g_mode as #ns_core::life::sequence::Mode<
                            #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                        >>::TransportTag,
                    >
                >::begin(#v_link, #v_driver, #scratch)
            }

            #accept

            fn activate<#g_event>(
                mut self: ::core::pin::Pin<&mut Self>,
                mut #v_link: #slot,
                #v_driver: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
            ) {
                #activation_enter
                #begin
                #start
                #finish
            }

            fn eof<#g_event>(
                #mutable self: ::core::pin::Pin<&mut Self>,
                mut #v_link: #slot,
                _: #ns_fiber::dope::manifold::app::receive::End,
                #ed: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
            ) {
                #http_only
                #eof
            }

            fn close_gate(
                self: ::core::pin::Pin<&Self>,
                #v_link: #conn_ref,
            ) -> #ns_fiber::dope::net::link::slot::types::CloseGate {
                #gate_enter
                #gate
            }

            fn can_receive(
                self: ::core::pin::Pin<&Self>,
                #v_link: #conn_ref,
            ) -> ::core::primitive::bool {
                #query_enter
                #v_link.state().is_draining()
            }

            fn close<#g_event>(
                #mixed_mut self: ::core::pin::Pin<&mut Self>,
                mut #v_link: #slot,
            ) {
                #closing_enter
                #close_body
            }

        }

        #failure_impl

        #retirement_impl

        #report

        impl<#def>
            #ns_fiber::dope::manifold::network::listener::handler::send::Drive<'d, #g_id>
            for #name #args
        where
            #bounds
            #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
        {
            fn send<#g_event>(
                mut self: ::core::pin::Pin<&mut Self>,
                mut #v_link: #slot,
                #v_driver: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
            ) {
                #send_enter
                #begin
                #send
                #finish
            }
        }

        #input

        #gated
        }
    };
    let plain = application(false, &proc_macro2::TokenStream::new());
    let mixed = types.h2.map(|ns_h2| application(true, ns_h2));
    quote! { #core #plain #mixed }
}
