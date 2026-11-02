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
}

impl Callbacks {
    pub(super) fn new(paths: &Paths, types: &types::Types<'_>) -> Self {
        let f_stamp = local("stamp");
        let symbols = &types.names.generics;
        let [v_reason, v_slot, v_staged] = symbols.locals(["reason", "slot", "staged"]);
        let [n_dprog] = symbols.locals(["__deadline_progress"]);
        let [n_drain, n_drive] = symbols.locals(["__drain", "__drive"]);
        let [n_prog, v_action] = symbols.locals(["__progress", "action"]);
        let [v_budget, v_bytes, v_chunk] = symbols.locals(["budget", "bytes", "chunk"]);
        let [v_link, v_core, v_date] = symbols.locals(["connection", "core", "date"]);
        let [v_driver, v_raw, v_read, v_tail] = symbols.locals(["driver", "raw", "read", "tail"]);
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
        let params = &types.generics;
        let def = &params.application_params;
        let args = &params.application_generic_use;
        let bounds = &params.bounds;
        let proto = &types.protocol;
        let clock = proto.duplex.clock(types.names.generics);
        let duplex = proto.duplex;
        let slot = &proto.h1_slot;
        let conn = &proto.connections.http_connection;
        let egress = &proto.egress_type;
        let tag = quote! {
            <#g_mode as #ns_core::life::sequence::Mode<
                #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
            >>::TransportTag
        };
        let send = match duplex {
            duplex::State::Enabled => quote! {
                match #v_link.state().mode() {
                    #ns_h1::switch::Mode::Http
                    | #ns_h1::switch::Mode::Switching(_) => {
                        let (mut #v_core, #v_date) = self.as_mut().__project();
                        #n_dprog.merge(#v_core.as_mut().send(
                            #clock,
                            &mut #v_link,
                            #v_driver,
                        ));
                    }
                    #ns_h1::switch::Mode::Raw(_) => {}
                }
            },
            duplex::State::Disabled => quote! {
                let (mut #v_core, #v_date) = self.as_mut().__project();
                #n_dprog.merge(#v_core.as_mut().send(
                    #clock,
                    &mut #v_link,
                    #v_driver,
                ));
            },
        };
        let start = match duplex {
            duplex::State::Enabled => quote! {
                match #v_link.state().mode() {
                    #ns_h1::switch::Mode::Raw(_) => {
                        #ns_h1::server::duplex::Runtime::<_, #tag>::new(&mut #v_link)
                            .activate(#v_driver);
                    }
                    #ns_h1::switch::Mode::Http
                    | #ns_h1::switch::Mode::Switching(_) => {
                    let #v_action = #v_link.state().action();
                    let (mut #v_core, #v_date) = self.as_mut().__project();
                    match #v_action {
                        ::core::option::Option::Some(_) => #n_dprog.merge(#v_core.as_mut().send(
                                #clock,
                                &mut #v_link,
                                #v_driver,
                            )),
                        ::core::option::Option::None => {
                            let #n_drive = #ns_h1::server::conn::driver::Driver::new(
                                #v_core.as_mut(), #clock,
                            ).resume(&mut #v_link, #v_driver);
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
                let (mut #v_core, #v_date) = self.as_mut().__project();
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
        let eof = match duplex {
            duplex::State::Enabled => quote! {
                let #n_drain = match #v_link.state().mode() {
                    #ns_h1::switch::Mode::Raw(_) => {
                        #ns_h1::server::duplex::Runtime::<_, #tag>::new(&mut #v_link)
                        .finish_input(#v_driver)
                    }
                    #ns_h1::switch::Mode::Http => {
                        #ns_fiber::dope::manifold::network::listener::handler::Drain::Complete
                    }
                    #ns_h1::switch::Mode::Switching(_) => {
                        #ns_fiber::dope::manifold::network::listener::handler::Drain::Pending
                    }
                };
                match #n_drain {
                    #ns_fiber::dope::manifold::network::listener::handler::Drain::Pending => {
                        #ns_fiber::dope::manifold::network::listener::handler::Eof::Hold
                    }
                    #ns_fiber::dope::manifold::network::listener::handler::Drain::Discard
                    | #ns_fiber::dope::manifold::network::listener::handler::Drain::Complete => {
                        #ns_fiber::dope::manifold::network::listener::handler::Eof::Close
                    }
                }
            },
            duplex::State::Disabled => {
                quote!(#ns_fiber::dope::manifold::network::listener::handler::Eof::Close)
            }
        };
        let pending = quote! {
            #v_link.state().can_resume()
                || #v_link.state().action().is_some()
                || ::core::matches!(
                    #ns_h1::pump::Tasks::activity(&#v_link.state().async_state),
                    #ns_h1::pump::Activity::Task | #ns_h1::pump::Activity::Stream
                )
        };
        let eof = quote! {
            #v_link.state_mut().ingress.end_discard();
            if #pending {
                #ns_fiber::dope::manifold::network::listener::handler::Eof::Hold
            } else {
                #eof
            }
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
        let input = match duplex {
            duplex::State::Enabled => quote! {
                impl<#l_wire, #def>
                    #ns_fiber::dope::manifold::network::listener::handler::Scoped<#l_wire, 'd, #g_id>
                    for #name #args
                where
                    #bounds
                    'd: #l_wire,
                    #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
                {
                    fn retention(&self) -> #ns_fiber::dope::manifold::app::receive::limits::Budget {
                        self.#f_stamp.1
                    }

                    fn scoped_chunk<#g_event>(
                        mut self: ::core::pin::Pin<&mut Self>,
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
                        #v_driver: &mut #ns_fiber::dope::core::driver::retained::Context<'_, '_, 'd>,
                    ) -> #ns_fiber::dope::manifold::app::receive::Outcome {
                        use #ns_fiber::dope::manifold::app::receive::Outcome;
                        let (#v_chunk, #v_budget) = #v_chunk.into_parts();
                        let mut #v_read = #ns_h1::server::duplex::Read::new(
                            &mut #v_link,
                            #v_driver,
                            #v_budget,
                            |#v_link, #v_bytes, #v_driver| {
                                let (mut #v_core, #v_date) = self.as_mut().__project();
                                #v_core.as_mut().chunk(#clock, #v_link, #v_bytes, #v_driver)
                            },
                        );
                        #v_chunk.drive(Outcome::Continue, &mut #v_read)
                    }
                }
            },
            duplex::State::Disabled => quote! {
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
                        #v_driver: &mut #ns_fiber::dope::core::driver::retained::Context<'_, '_, 'd>,
                    ) -> #ns_fiber::dope::manifold::app::receive::Outcome {
                    let (mut #v_core, #v_date) = self.__project();
                    use #ns_h1::server::conn::driver::Ingress;
                    match #v_core.as_mut().chunk(
                            #clock,
                            &mut #v_link,
                            #v_chunk.as_ref(),
                            #v_driver,
                        ) {
                        Ingress::Continue => #ns_fiber::dope::manifold::app::receive::Outcome::Continue,
                        Ingress::Handoff { staged: #v_staged, .. } => match #v_staged {},
                        Ingress::CloseAfter => #ns_fiber::dope::manifold::app::receive::Outcome::CloseAfter,
                    }
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
        }
    }
}
