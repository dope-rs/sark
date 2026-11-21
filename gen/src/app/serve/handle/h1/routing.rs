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
    let f_life = local("lifecycle");
    let symbols = &types.names.generics;
    let [v_route, v_tag, v_trans] = symbols.locals(["route", "tag", "transport"]);
    let [v_body, v_charge, v_turn, v_prior] = symbols.locals(["body", "charge", "turn", "prior"]);
    let [v_link, v_date, v_error] = symbols.locals(["connection", "date", "error"]);
    let [v_exch, v_head, v_input] = symbols.locals(["exchange", "head", "input"]);
    let [v_method, v_result, v_parsed] = symbols.locals(["method", "outcome", "parsed"]);
    let [v_wait, v_ready] = symbols.locals(["pending", "ready"]);
    let [v_run, v_source] = symbols.locals(["runtime", "source"]);
    let v_this = symbols.local("this");
    let [v_write, v_routes] = symbols.locals(["write", "routes"]);
    let [g_id, g_mode] = types.names.generics.idents(["__ID", "__M"]);
    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    let ns_http = &paths.http;
    use quote::quote;

    let core = types.names.core_ident;
    let dispatch = &types.names.dispatch_ident;
    let g_turn = symbols.lifetime("__dispatch");
    let g_h = symbols.ident("__H");
    let args = &types.generics;
    let bounds = &args.bounds;
    let def = &args.core_params;
    let uses = &args.core_generic_use;
    let route = &types.routing;
    let mask = route.method_mask;
    let headers = &route.resume_head;
    let request = quote!(#ns_http::h1::head::line::error::Request);
    let proto = &types.protocol;
    let wait = &proto.connections.pending;
    let policy = &proto.transition_policy;
    let egress = types.routing.egress_bound;
    let conn = &proto.connections.http_connection;
    let rt = &route.runtime;
    let turn = quote!(#ns_h1::session::Turn<'_, '_, 'd, #rt, #wait, #policy, Self>);
    let terminal = terminal::routes(paths, types.names.generics, conn, rt);
    let data = &routes.body_data_arms;
    let work = &routes.body_work_arms;
    let pending = &routes.pending_resume_arms;
    let settle_pending = (!data.is_empty()).then(|| {
        quote! {
            #v_wait.route = match #v_route {
                ::core::result::Result::Ok(#v_route) => #v_route,
                ::core::result::Result::Err(#v_error) => {
                    #ns_core::life::sequence::Mode::reject(#v_this.#f_life, #v_exch);
                    return ::core::result::Result::Err(#v_error);
                }
            };
            ::core::result::Result::Ok(#v_wait)
        }
    });
    let finish = &routes.finish_arms;
    let ready = &proto.connections.ready;
    let head = &proto.connections.head;
    let resume = &routes.resume_arms;
    let used = symbols.local("used");
    let settled = quote! {
        #v_result.settle::<false, _, _>(#v_this.#f_life, #v_run.session_mut(), #v_exch);
        #v_turn.apply(#v_result, 0)
    };
    let finish_body = quote!(match #v_wait.route { #finish });
    let finish_body = if routes.finish_settles {
        quote! { let #v_result = #finish_body; #settled }
    } else {
        finish_body
    };
    let build = if resume.is_empty() {
        quote!(match #v_ready {})
    } else {
        quote! {
            let mut #v_this = #v_turn.router.as_mut().get_mut().app.as_mut().project();
            let #v_run = &mut #v_turn.conn.async_state;
            let #used = 0;
            let #v_exch = #v_ready.exchange;
            let #v_result = match #v_ready.route { #resume };
            #settled
        }
    };
    let consume = [false, true].map(|owned| {
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
        let incomplete = if owned {
            quote!(#v_turn.failed(#ns_h1::wire::Failure::BadRequest))
        } else {
            quote!(#v_turn.await_line())
        };
        quote! {
            fn #method(
                #input,
                #v_turn: &mut #turn,
                #v_prior: ::core::primitive::usize,
            ) -> #ns_h1::session::Flow {
                let #v_parsed = match #parse {
                    ::core::result::Result::Ok(#complete) => #v_parsed,
                    ::core::result::Result::Ok(#pending) => {
                        return #incomplete;
                    }
                    ::core::result::Result::Err(#v_error) => {
                        let (#request::Syntax | #request::Target(..) | #request::Span(..)) = #error;
                        #v_turn.router.as_mut().reject_head(&mut #v_turn.conn.async_state);
                        return #v_turn.close(
                            #ns_http::h1::codec::canned::BAD_REQUEST,
                        );
                    }
                };
                let #v_head = #v_parsed.request();
                if let ::core::option::Option::Some(#v_method) = #v_head.matched() {
                    let #v_date = &#v_turn.router.date.load();
                    #body
                }
                let #v_result = #reject(#retained, #ns_http::h1::codec::canned::NOT_FOUND);
                #v_turn.reject(#v_result)
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
                #v_run: &mut #rt,
                #v_tag: Self::TransportTag,
            ) {
                let #v_this = self.get_mut();
                let #v_link = #v_this.id;
                let mut #v_this = #v_this.app.as_mut().project();
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
        struct #dispatch<#g_turn, 'd, #g_h, #g_id: #ns_fiber::Identity> {
            app: ::core::pin::Pin<&#g_turn mut #g_h>,
            id: #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
            date: &#g_turn #ns_h1::server::date::Clock,
        }

        impl<#g_turn, #def> #dispatch<#g_turn, 'd, #core #uses, #g_id>
        where
            #bounds
            #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
        {
            #methods

        }

        impl<#g_turn, #def> #ns_h1::session::Dispatch<'d, #rt, #wait, #policy>
            for #dispatch<#g_turn, 'd, #core #uses, #g_id>
        where
            #bounds
            #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
        {
            type Ready = #ready;
            type Head = #head;

            #terminal

            #( #consume )*

            fn resume_head(
                #v_wait: #head,
                #v_turn: &mut #turn,
            ) -> #ns_h1::session::Flow {
                let #v_input = #v_turn.input();
                let #v_date = &#v_turn.router.date.load();
                #headers
            }

            fn body_data(
                self: ::core::pin::Pin<&mut Self>,
                mut #v_wait: #wait,
                #v_body: #ns_http::payload::owned::Segments<'d>,
                _: &mut #rt,
            ) -> ::core::result::Result<
                #wait,
                #ns_http::payload::fixed::Body<'d>,
            > {
                let mut #v_this = self.get_mut().app.as_mut().project();
                let #v_exch = #v_wait.exchange;
                let #v_route = match #v_wait.route {
                    #data
                };
                #settle_pending
            }

            fn has_body_work(#v_wait: &#wait) -> ::core::primitive::bool {
                match #v_wait.route { #work }
            }

            fn resume_pending(
                self: ::core::pin::Pin<&mut Self>,
                mut #v_wait: #wait,
                _: &mut #rt,
            ) -> ::core::result::Result<#wait, #ns_http::payload::fixed::Body<'d>> {
                let mut #v_this = self.get_mut().app.as_mut().project();
                let #v_exch = #v_wait.exchange;
                let #v_route = match #v_wait.route { #pending };
                #settle_pending
            }

            fn finish_body(
                #v_wait: #wait,
                #v_body: #ns_h1::session::Complete,
                #v_turn: &mut #turn,
            ) -> #ns_h1::session::Flow {
                let #v_routes = #v_turn.router.as_mut().get_mut();
                let #v_date = &#v_routes.date.load();
                let mut #v_this = #v_routes.app.as_mut().project();
                let #v_run = &mut #v_turn.conn.async_state;
                let #v_write = #v_turn.output.reborrow();
                let #used = #v_body.consumed;
                let #v_exch = #v_wait.exchange;
                #ns_core::life::Mode::observe(
                    #v_this.#f_life,
                    #ns_core::life::Event::InputFinished(#v_exch),
                );
                #finish_body
            }

            fn resume_body(
                #v_ready: Self::Ready,
                #v_turn: &mut #turn,
            ) -> #ns_h1::session::Flow {
                #build
            }

        }

        impl<#g_turn, #def> #ns_h1::server::conn::router::Routes<'d, #rt, #policy>
            for #dispatch<#g_turn, 'd, #core #uses, #g_id>
        where
            #bounds
            #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
        {
            type Pending = #wait;
            type Egress = #egress;
            type TransportTag = <#g_mode as
                #ns_core::life::sequence::Mode<
                    #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                >>::TransportTag;

            fn response_tag(
                self: ::core::pin::Pin<&mut Self>,
                #v_run: &mut #rt,
                #v_charge: #ns_http::o3::buffer::hold::Charge,
            ) -> Self::TransportTag {
                let #v_this = self.get_mut();
                let #v_link = #v_this.id;
                let #v_this = #v_this.app.as_mut().project();
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
        }

    }
}
