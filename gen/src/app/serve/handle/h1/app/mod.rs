use crate::{macros::tokens::local, runtime::paths::Paths};

mod calls;
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
    let [l_a, l_p, l_s, l_wire, l_w, g_event] = types
        .names
        .generics
        .lives(["__a", "__p", "__s", "__w", "__wire", "e"]);
    let [g_id, g_mode, g_wire] = types.names.generics.idents(["__ID", "__M", "__W"]);
    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    use quote::quote;

    let calls = calls::Callbacks::new(paths, types);
    let report = report::render(paths, types);
    let names = &types.names;
    let name = names.name;
    let state = names.state_ty;
    let retire = quote::format_ident!("__{name}Retirement");
    let core = names.core_ident;
    let bounds = &types.generics.bounds;
    let def = &types.generics.application_params;
    let args = &types.generics.application_generic_use;
    let proto = &types.protocol;
    let slot = &proto.h1_slot;
    let conn = &proto.connections.http_connection;
    let pending = &proto.connections.pending;
    let egress = &proto.egress_type;
    let source = &proto.input_type;
    let chunk = chunk::render(paths, types);
    let edit = &types.http_mut;
    let audit = &types.audit;
    let tasks = &types.tasks;
    let body = &tasks.send_body;
    let wake = &tasks.wake_body;
    let close = &tasks.close_body;
    let send = &calls.send;
    let start = &calls.activate;
    let eof = &calls.eof;
    let gate = &calls.gate;
    let recv = &calls.blocked;
    let stop = &calls.close;
    let input = &calls.input;
    let gated = &calls.gated;
    let scratch = &calls.scratch;
    let mutable = proto.upgrade.then(|| quote!(mut));
    let cancel = &routes.cancel_arms;
    let retire_pending =
        (!cancel.is_empty()).then(|| quote::quote!(#v_exch.retire(#v_this.#f_life, #v_result);));
    let cancelrd = &routes.ready_cancel;
    let conn_ref = quote! {
        #ns_fiber::dope::manifold::network::listener::conn::Ref<
            '_, '_, '_, '_, 'd, #g_id, #g_wire, #conn, Self::Egress,
        >
    };
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
        if #n_root.try_apply(&mut #v_link, #n_now).is_err() {
            #v_link.close_after();
        }
    };
    quote! {
        impl<#def> #core #args
        where
            #bounds
            #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
        {
            #chunk

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

        impl<#def> #ns_fiber::dope::manifold::network::listener::handler::Application<'d, #g_id>
            for #name #args
        where
            #bounds
            #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
        {
            type Conn = #conn;
            type Build = ::core::convert::Infallible;
            type Wire = #g_wire;
            type Retirement = #retire;
            type WireFailure = Self;
            type Egress = #egress;
            type Input = #source;
            type Send = #ns_fiber::dope::manifold::network::listener::handler::send::Reported<
                #ns_fiber::dope::manifold::network::listener::handler::send::Run,
            >;

            fn connection(
                self: ::core::pin::Pin<&Self>,
                _: &#ns_fiber::dope::core::random::Seed,
            ) -> ::core::result::Result<Self::Conn, Self::Build> {
                ::core::result::Result::Ok(::core::default::Default::default())
            }

            fn build_failed(self: ::core::pin::Pin<&mut Self>, #v_error: Self::Build) {
                match #v_error {}
            }

            #recv

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
                #begin
                #n_dprog.merge(#ns_h1::server::deadline::Progress::new(
                    #ns_h1::server::deadline::Head::Ready,
                    #ns_h1::server::deadline::Body::None,
                ));
                #finish
            }

            fn timeout<#g_event>(
                mut self: ::core::pin::Pin<&mut Self>,
                #v_link: #slot,
                #v_driver: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
            ) -> #ns_fiber::dope::manifold::network::listener::handler::Timeout<'d> {
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
                #v_link: #slot,
                #v_driver: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
            ) -> #ns_fiber::dope::manifold::network::listener::handler::Drain {
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

            fn activate<#g_event, #l_a, #l_p, #l_s, #l_wire>(
                mut self: ::core::pin::Pin<&mut Self>,
                mut #v_link: #ns_fiber::dope::manifold::network::listener::conn::Ctx<#g_event, #l_a, #l_p, #l_s, #l_wire, 'd, #g_id, #g_wire, #conn, Self::Egress>,
                #v_driver: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
            ) -> #ns_fiber::dope::manifold::network::listener::conn::Ctx<#g_event, #l_a, #l_p, #l_s, #l_wire, 'd, #g_id, #g_wire, #conn, Self::Egress>
            where
                'd: #l_wire,
            {
                #begin
                #start
                #finish
                #v_link
            }

            fn eof<#g_event>(
                #mutable self: ::core::pin::Pin<&mut Self>,
                mut #v_link: #slot,
                _: #ns_fiber::dope::manifold::app::receive::End,
                #ed: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
            ) {
                #eof
            }

            fn close_gate(
                self: ::core::pin::Pin<&Self>,
                #v_link: #conn_ref,
            ) -> #ns_fiber::dope::net::link::slot::types::CloseGate {
                #gate
            }

            fn can_receive(
                self: ::core::pin::Pin<&Self>,
                #v_link: #conn_ref,
            ) -> ::core::primitive::bool {
                #v_link.state().is_draining()
            }

            fn close<#g_event, #l_a, #l_p, #l_s, #l_wire>(
                self: ::core::pin::Pin<&mut Self>,
                mut #v_link: #ns_fiber::dope::manifold::network::listener::conn::Ctx<#g_event, #l_a, #l_p, #l_s, #l_wire, 'd, #g_id, #g_wire, #conn, Self::Egress>,
            ) -> #ns_fiber::dope::manifold::network::listener::conn::Ctx<#g_event, #l_a, #l_p, #l_s, #l_wire, 'd, #g_id, #g_wire, #conn, Self::Egress>
            where
                'd: #l_wire,
            {
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
                #v_link
            }

        }

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
                #begin
                #send
                #finish
            }
        }

        #input

        #gated
    }
}
