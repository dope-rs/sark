use crate::{
    app::serve::handle::h1::{routes, terminal, types},
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(super) fn build<'view, 'source>(
    paths: &Paths,
    types: &'view types::Types<'source>,
    routes: &'view routes::Routes,
    methods: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let [f_life, f_state] = ["lifecycle", "state"].map(local);
    let symbols = &types.names.generics;
    let [v_route, v_tag, v_trans] = symbols.locals(["route", "tag", "transport"]);
    let [v_body, v_charge, v_conn] = symbols.locals(["body", "charge", "conn"]);
    let [v_link, v_date, v_error] = symbols.locals(["connection", "date", "error"]);
    let [v_exch, v_head, v_input] = symbols.locals(["exchange", "head", "input"]);
    let [v_method, v_result, v_parsed] = symbols.locals(["method", "outcome", "parsed"]);
    let [v_wait, v_permit, v_ready] = symbols.locals(["pending", "permit", "ready"]);
    let [v_resp, v_run, v_source] = symbols.locals(["response", "runtime", "source"]);
    let [v_stamp, v_state, v_this] = symbols.locals(["stamp", "state", "this"]);
    let [v_write, v_routes] = symbols.locals(["write", "routes"]);
    let l_env = types.names.generics.lifetime("env");
    let [g_id, g_mode] = types.names.generics.idents(["__ID", "__M"]);
    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    let ns_http = &paths.http;
    use quote::quote;

    let state = types.names.state_ty;
    let core = types.names.core_ident;
    let args = &types.generics;
    let bounds = &args.bounds;
    let def = &args.application_params;
    let uses = &args.application_generic_use;
    let route = &types.routing;
    let mask = route.method_mask;
    let headers = &route.resume_head;
    let request = quote!(#ns_http::h1::head::line::error::Request);
    let proto = &types.protocol;
    let wait = &proto.connections.pending;
    let policy = &proto.transition_policy;
    let egress = &types.routing.egress_bound;
    let raw = quote!(<#policy as #ns_h1::switch::Engine>::Raw);
    let conn = &proto.connections.http_connection;
    let terminal = terminal::routes(paths, types.names.generics, conn);
    let rejected = terminal::rejected(paths, types.names.generics);
    let rt = &route.runtime;
    let data = &routes.body_data_arms;
    let work = &routes.body_work_arms;
    let pending = &routes.pending_resume_arms;
    let settle_pending = quote! {
        #v_wait.route = match #v_route {
            ::core::result::Result::Ok(#v_route) => #v_route,
            ::core::result::Result::Err(#v_error) => {
                #ns_core::life::sequence::Mode::reject(#v_this.#f_life, #v_exch);
                return ::core::result::Result::Err(#v_error);
            }
        };
        ::core::result::Result::Ok(#v_wait)
    };
    let finish = &routes.finish_arms;
    let ready = &proto.connections.ready;
    let head = &proto.connections.head;
    let resume = &routes.resume_arms;
    let finished = terminal::completed(paths, types.names.generics, &quote!(#v_run.session_mut()));
    let complete = quote! {
                match &#v_result {
                    #ns_h1::session::Outcome::Park { .. } => {
                        #ns_core::life::Mode::observe(
                            #v_this.#f_life,
                            #ns_core::life::Event::TaskAdmitted(#v_exch),
                        );
                    }
                    #ns_h1::session::Outcome::Streamed { .. } => {
                        #ns_core::life::sequence::Mode::commit(
                            #v_this.#f_life,
                            #v_run.session_mut(),
                            #v_exch,
                            0,
                            #ns_http::o3::buffer::hold::Charge::default(),
                        );
                        #ns_core::life::Mode::observe(
                            #v_this.#f_life,
                            #ns_core::life::Event::TaskAdmitted(#v_exch),
                        );
                    }
                    #ns_h1::session::Outcome::Complete { response: #v_resp, .. } => {
                        #finished
                    }
                    #ns_h1::session::Outcome::Switch { .. } => {
                        #ns_core::life::sequence::Mode::commit(
                            #v_this.#f_life,
                            #v_run.session_mut(),
                            #v_exch,
                            0,
                            #ns_http::o3::buffer::hold::Charge::default(),
                        );
                        #v_exch.retire(
                            #v_this.#f_life,
                            #ns_core::life::Outcome::Completed,
                        );
                    }
                    #ns_h1::session::Outcome::Close(_)
                    | #ns_h1::session::Outcome::Failed(_) => {
                        #v_exch.retire(
                            #v_this.#f_life,
                            #ns_core::life::Outcome::Rejected,
                        );
                    }
                    #ns_h1::session::Outcome::NeedMore { .. } => {}
                }
                #v_result
    };
    let build = if resume.is_empty() {
        quote!(match #v_ready {})
    } else {
        quote! {
            let mut #v_this = self.as_mut().project();
            let #v_exch = #v_ready.exchange;
            let #v_result = match #v_ready.route { #( #resume, )* };
            self.__finish_body(#v_exch, #v_result, #v_run)
        }
    };
    let consume = [false, true].map(|owned| {
        let receiver = if owned {
            quote!(mut self)
        } else {
            quote!(self)
        };
        let project = (!owned).then(|| quote!(let mut #v_this = self.project();));
        let on_error = owned.then(|| quote!(let mut #v_this = self.as_mut().project();));
        let state = (!owned).then(|| quote!(let #v_state: &#l_env #state = *#v_this.#f_state;));
        let (method, input, parse, complete, pending, error, reject, retained, body) = if owned {
            (
                quote!(consume_line),
                quote!(#v_source: #ns_h1::wire::parsing::Source),
                quote!(#ns_http::h1::head::line::Owned::try_parse(#v_source, #mask)),
                quote!(::core::ops::ControlFlow::Continue(#v_parsed)),
                quote!(::core::ops::ControlFlow::Break(_)),
                quote!(#v_error.discard()),
                quote!(#ns_h1::wire::parsing::Rejected::start_owned),
                quote!(#v_parsed),
                &route.dispatch_line,
            )
        } else {
            (
                quote!(try_consume),
                quote!(#v_input: #ns_h1::Input<'_>),
                quote!(#ns_h1::wire::parsing::Head::parse(#v_input, #mask)),
                quote!(::core::option::Option::Some(#v_parsed)),
                quote!(::core::option::Option::None),
                quote!(#v_error),
                quote!(#ns_h1::wire::parsing::Rejected::start),
                quote!(&#v_parsed),
                &route.dispatch_body,
            )
        };
        quote! {
            fn #method(
                #receiver: ::core::pin::Pin<&mut Self>,
                #v_link: #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                #v_stamp: &#ns_h1::server::date::Clock,
                #v_permit: #ns_h1::session::Permit,
                #input,
                #v_write: &mut [::core::primitive::u8],
                #v_conn: &mut #conn,
            ) -> #ns_h1::session::RouteOutcome<
                'd,
                #wait,
                #raw,
                #head,
                #ns_h1::wire::parsing::Rejected<'d>,
            > {
                #project
                let #v_parsed = match #parse {
                    ::core::result::Result::Ok(#complete) => #v_parsed,
                    ::core::result::Result::Ok(#pending) => {
                        return #ns_h1::session::Outcome::NeedMore {
                            permit: #v_permit,
                            state: #ns_h1::session::NeedMore::Line,
                        }.into();
                    }
                    ::core::result::Result::Err(#v_error) => {
                        #on_error
                        let (#request::Syntax | #request::Target(..) | #request::Span(..)) = #error;
                        let #v_exch = #ns_core::life::sequence::Mode::open(
                            #v_this.#f_life,
                            #v_conn.async_state.session_mut(),
                            #v_link,
                        );
                        #ns_core::life::sequence::Mode::reject(
                            #v_this.#f_life,
                            #v_exch,
                        );
                        return #ns_h1::session::Outcome::Close(
                            #ns_http::h1::codec::canned::BAD_REQUEST,
                        ).into();
                    }
                };
                let ::core::option::Option::Some(#v_method) = #v_parsed.request().matched() else {
                    let #v_result = #reject(
                        #retained,
                        #v_permit,
                        #ns_http::h1::codec::canned::NOT_FOUND,
                    );
                    #on_error
                    #rejected
                    return #v_result.into();
                };
                let #v_date = #v_stamp.load();
                #state
                let #v_head = #v_parsed.request();
                let #v_date = &#v_date;
                #body
            }
        }
    });
    let handoff = [
        (quote!(accept), quote!(accept)),
        (quote!(cancel), quote!(unstage)),
    ]
    .map(|(name, action)| {
        quote! {
            fn #name(
                self: ::core::pin::Pin<&mut Self>,
                #v_link: #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                #v_run: &mut #rt,
                #v_tag: Self::TransportTag,
            ) {
                let mut #v_this = self.project();
                let #v_trans = #ns_core::life::sequence::Mode::transport(
                    #v_this.#f_life,
                    #v_link,
                    #v_tag,
                );
                let ::core::option::Option::Some((#v_exch, #v_charge)) = #v_trans else {
                    return;
                };
                #ns_core::life::sequence::Mode::#action(
                    #v_this.#f_life,
                    #v_run.session_mut(),
                    #v_exch,
                    #v_charge,
                );
            }
        }
    });
    quote! {
        impl<#def> #core #uses
        where
            #bounds
            #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
        {
            #methods

            fn __finish_body(
                self: ::core::pin::Pin<&mut Self>,
                #v_exch: #ns_core::life::exchange::Id<
                    #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>, ::core::primitive::u64,
                >,
                #v_result: #ns_h1::session::Outcome<'d, #raw>,
                #v_run: &mut #rt,
            ) -> #ns_h1::session::BodyOutcome<'d, #ready, #raw> {
                let mut #v_this = self.project();
                let #v_result = { #complete };
                #ns_h1::session::BodyOutcome::Complete(#v_result)
            }
        }

        impl<#def> #ns_h1::server::conn::router::Routes<
            'd,
            #rt,
            #g_id,
            #policy,
        >
            for #core #uses
        where
            #bounds
            #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
        {
            #terminal

            type Pending = #wait;
            type Ready = #ready;
            type Head = #head;
            type Egress = #egress;
            type TransportTag = <#g_mode as
                #ns_core::life::sequence::Mode<
                    #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                >>::TransportTag;

            fn response_tag(
                self: ::core::pin::Pin<&mut Self>,
                #v_link: #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                #v_run: &mut #rt,
                #v_charge: #ns_http::o3::buffer::hold::Charge,
            ) -> Self::TransportTag {
                let #v_this = self.project();
                let ::core::option::Option::Some(#v_exch) =
                    #ns_core::life::sequence::Mode::current(
                        #v_this.#f_life,
                        #v_run.session(),
                        #v_link,
                    )
                else {
                    return ::core::default::Default::default();
                };
                #ns_core::life::sequence::Mode::tag(
                    #v_this.#f_life,
                    #v_exch,
                    #v_charge,
                )
            }

            #( #handoff )*

            #( #consume )*

            fn resume_head(
                #v_routes: #ns_h1::server::conn::router::Dispatch<'_, 'd, Self, #g_id>,
                #v_wait: #head,
                #v_permit: #ns_h1::session::Permit,
                #v_input: #ns_h1::Input<'_>,
                #v_write: &mut [::core::primitive::u8],
                #v_conn: &mut #conn,
            ) -> #ns_h1::session::RouteOutcome<'d, #wait, #raw, #head, #ns_h1::wire::parsing::Rejected<'d>> {
                let #v_link = #v_routes.id;
                let #v_date = #v_routes.date.load();
                let #v_date = &#v_date;
                let mut #v_routes = #v_routes.app;
                #headers
            }

            fn resume_error(
                self: ::core::pin::Pin<&mut Self>,
                #v_link: #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                #v_wait: #ns_h1::wire::parsing::Rejected<'d>,
                #v_permit: #ns_h1::session::Permit,
                #v_input: #ns_h1::Input<'_>,
                #v_conn: &mut #conn,
            ) -> #ns_h1::session::RouteOutcome<'d, #wait, #raw, #head, #ns_h1::wire::parsing::Rejected<'d>> {
                let mut #v_this = self.project();
                let #v_result = #v_wait.resume(#v_input, #v_permit);
                #rejected
                #v_result
            }

            fn body_data(
                self: ::core::pin::Pin<&mut Self>,
                _: #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                mut #v_wait: #wait,
                #v_body: #ns_http::payload::owned::Segments<'d>,
                _: &mut #rt,
            ) -> ::core::result::Result<
                #wait,
                #ns_http::payload::fixed::Body<'d>,
            > {
                let mut #v_this = self.project();
                let #v_exch = #v_wait.exchange;
                let #v_route = match #v_wait.route {
                    #( #data, )*
                };
                #settle_pending
            }

            fn has_body_work(#v_wait: &Self::Pending) -> ::core::primitive::bool {
                match &#v_wait.route { #( #work, )* }
            }

            fn resume_pending(
                self: ::core::pin::Pin<&mut Self>,
                _: #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                mut #v_wait: Self::Pending,
                _: &mut #rt,
            ) -> ::core::result::Result<Self::Pending, #ns_http::payload::fixed::Body<'d>> {
                let mut #v_this = self.project();
                let #v_exch = #v_wait.exchange;
                let #v_route = match #v_wait.route { #( #pending, )* };
                #settle_pending
            }

            fn finish_body(
                mut self: ::core::pin::Pin<&mut Self>,
                #v_wait: #wait,
                #v_body: #ns_h1::session::Complete,
                #v_permit: #ns_h1::session::Permit,
                #v_write: &mut [::core::primitive::u8],
                #v_date: &[::core::primitive::u8; 29],
                #v_run: &mut #rt,
            ) -> #ns_h1::session::BodyOutcome<'d, #ready, #raw> {
                let mut #v_this = self.as_mut().project();
                let #v_exch = #v_wait.exchange;
                #ns_core::life::Mode::observe(
                    #v_this.#f_life,
                    #ns_core::life::Event::InputFinished(#v_exch),
                );
                let #v_result = match #v_wait.route {
                    #( #finish, )*
                };
                self.__finish_body(#v_exch, #v_result, #v_run)
            }

            fn resume_body(
                mut self: ::core::pin::Pin<&mut Self>,
                _: #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                #v_ready: Self::Ready,
                #v_permit: #ns_h1::session::Permit,
                _: &mut [::core::primitive::u8],
                _: &[::core::primitive::u8; 29],
                #v_run: &mut #rt,
            ) -> #ns_h1::session::BodyOutcome<'d, #ready, #raw> {
                #build
            }

        }

    }
}
