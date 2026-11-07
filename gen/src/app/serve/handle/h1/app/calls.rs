use crate::{
    app::serve::{duplex, handle::h1::types},
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(super) struct Callbacks {
    pub(super) send: proc_macro2::TokenStream,
    pub(super) activate: proc_macro2::TokenStream,
    pub(super) eof: proc_macro2::TokenStream,
    pub(super) gate: proc_macro2::TokenStream,
    pub(super) blocked: proc_macro2::TokenStream,
    pub(super) close: proc_macro2::TokenStream,
    pub(super) input: proc_macro2::TokenStream,
    pub(super) gated: proc_macro2::TokenStream,
    pub(super) scratch: proc_macro2::TokenStream,
}

impl Callbacks {
    pub(super) fn new(
        paths: &Paths,
        types: &types::Types<'_>,
        mixed: bool,
        http2: &proc_macro2::TokenStream,
        def: &proc_macro2::TokenStream,
        args: &proc_macro2::TokenStream,
        bounds: &proc_macro2::TokenStream,
        conn: &proc_macro2::TokenStream,
    ) -> Self {
        let [f_core, f_stamp] = ["core", "stamp"].map(local);
        let symbols = &types.names.generics;
        let dispatch = &types.names.dispatch_ident;
        let v_routes = symbols.local("routes");
        let [v_reason, v_slot] = symbols.locals(["reason", "slot"]);
        let [n_dprog] = symbols.locals(["__deadline_progress"]);
        let [n_drive] = symbols.locals(["__drive"]);
        let [n_prog, v_action] = symbols.locals(["__progress", "action"]);
        let [v_budget, v_bytes, v_chunk] = symbols.locals(["budget", "bytes", "chunk"]);
        let v_prefix = symbols.local("prefix");
        let [v_link, v_core, v_date] = symbols.locals(["connection", "core", "date"]);
        let [v_driver, v_raw, v_tail] = symbols.locals(["driver", "raw", "tail"]);
        let l_wire = types.names.generics.lifetime("__w");
        let g_r = types.names.generics.ident("__R");
        let g_event = types.names.generics.lifetime("e");
        let [g_id, g_mode, g_wire] = types.names.generics.idents(["__ID", "__M", "__W"]);
        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let ns_h1 = &paths.h1;
        let ns_http = &paths.http;
        use quote::quote;

        let names = &types.names;
        let name = names.name;
        let ns_h2 = types.h2;
        let g_h2a = symbols.ident("__H2A");
        let [v_pending, v_count, v_complete] = symbols.locals(["pending", "count", "complete"]);
        let [v_state, v_h2, v_cause] = symbols.locals(["state", "h2", "cause"]);
        let chunk_mut = mixed.then(|| quote!(mut));
        let proto = &types.protocol;
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
        let egress = &proto.egress_type;
        let mixed_conn = quote::format_ident!("__{name}Cleartext");
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
        let http_query = mixed.then(|| {
            quote! {
                if !::core::matches!(#v_link.state(), #mixed_conn::Http1(_)) {
                    return false;
                }
                let #v_link = #v_link.map_state(#mixed_conn::h1);
            }
        });
        let http_drained = mixed.then(|| {
            quote! {
                if !::core::matches!(#v_link.state(), #mixed_conn::Http1(_)) {
                    return #ns_fiber::dope::manifold::network::listener::handler::PostDrain::Idle;
                }
                let mut #v_link = #v_link.map_state(#mixed_conn::h1_mut);
            }
        });
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
                fn is_blocked(
                    self: ::core::pin::Pin<&Self>,
                    #v_link: #ns_fiber::dope::manifold::network::listener::conn::Ref<
                        '_,
                        '_,
                        '_,
                        '_,
                        'd,
                         #g_id,
                        #g_wire,
                        #conn,
                        #egress,
                    >,
                ) -> ::core::primitive::bool {
                    #http_query
                    match #v_link.state().raw() {
                        ::core::result::Result::Ok(#v_raw) => match (#v_raw.hold(), #v_raw.access()) {
                            (
                                #ns_core::duplex::machine::Hold::Released,
                                #ns_core::duplex::machine::Access::Vacant(()),
                            ) => false,
                            (
                                #ns_core::duplex::machine::Hold::Held,
                                #ns_core::duplex::machine::Access::Vacant(()),
                            )
                            | (_, #ns_core::duplex::machine::Access::Blocked) => true,
                        },
                        ::core::result::Result::Err(#ns_h1::switch::Mode::Http) => false,
                        ::core::result::Result::Err(#ns_h1::switch::Mode::Switching(_)) => true,
                    }
                }
            },
            duplex::State::Disabled => proc_macro2::TokenStream::new(),
        };
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
        let v_app = symbols.local("app");
        let v_input = symbols.local("input");
        let merged_scratch = if proto.upgrade {
            quote!(&mut #v_app.as_mut().project().#f_stamp.get_mut()#stamp.2)
        } else {
            quote!(&mut self.scratch)
        };
        let drive = super::chunk::render(paths, types, &merged_scratch, stamp.as_ref());
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
                let [n_raw, n_prefix, n_failed] = symbols.locals(["__raw", "__prefix", "__failed"]);
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
                            #conn,
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
        let gated = match duplex {
            duplex::State::Enabled => quote! {
                impl<#def>
                    #ns_fiber::dope::manifold::network::listener::handler::Gated<'d, #g_id>
                    for #name #args
                where
                    #bounds
                    #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
                {
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
                            #conn,
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
        Self {
            send,
            activate: start,
            eof,
            gate,
            blocked: recv,
            close,
            input,
            gated,
            scratch,
        }
    }
}
