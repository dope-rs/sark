use proc_macro2::TokenStream;
use quote::quote;

use crate::{macros::tokens::local, runtime::paths::Paths};

pub(super) fn routes(
    paths: &Paths,
    generics: &crate::types::Generics,
    conn: &TokenStream,
) -> TokenStream {
    let f_life = local("lifecycle");
    let [v_cause, v_conn, v_error] = generics.locals(["cause", "conn", "error"]);
    let [v_obs, v_wait, v_this] = generics.locals(["observer", "pending", "this"]);
    let [v_link, v_exch] = generics.locals(["connection", "exchange"]);
    let g_id = generics.ident("__ID");
    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    let ns_http = &paths.http;
    let open = open(paths, generics);
    quote! {
        fn failure(
            self: ::core::pin::Pin<&mut Self>,
            #v_cause: &#ns_h1::wire::Failure,
        ) -> #ns_http::payload::fixed::Body<'d> {
            let #v_this = self.project();
            if let ::core::option::Option::Some(#v_obs) =
                #ns_core::life::Mode::observer(#v_this.#f_life)
            {
                #ns_core::life::Observer::diagnostic(#v_obs, #v_cause);
            }
            #v_cause.body()
        }

        fn reject_line(
            mut self: ::core::pin::Pin<&mut Self>,
            #v_link: #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
            _: #ns_h1::session::Permit,
            #v_conn: &mut #conn,
            #v_error: #ns_h1::wire::parsing::LineError<'d>,
        ) -> #ns_http::payload::fixed::Body<'d> {
            let #v_this = self.as_mut().project();
            #open
            #ns_core::life::sequence::Mode::reject(#v_this.#f_life, #v_exch);
            #v_error.response(|#v_cause| {
                #ns_h1::server::conn::router::Routes::failure(self, #v_cause)
            })
        }

        fn abort_body(
            mut self: ::core::pin::Pin<&mut Self>,
            #v_conn: &mut #conn,
            #v_error: #ns_h1::wire::Failure,
        ) -> #ns_http::payload::fixed::Body<'d> {
            if let ::core::option::Option::Some(#v_wait) = #v_conn.ingress.take_pending() {
                self.as_mut().__cancel_pending(#v_wait);
            }
            #v_conn.ingress.finish_frame();
            #ns_h1::server::conn::router::Routes::failure(self, &#v_error)
        }
    }
}

pub(super) fn open(paths: &Paths, generics: &crate::types::Generics) -> TokenStream {
    let f_life = local("lifecycle");
    let [v_conn, v_link] = generics.locals(["conn", "connection"]);
    let [v_exch, v_this] = generics.locals(["exchange", "this"]);
    let ns_core = &paths.core;
    quote! {
        let #v_exch = #ns_core::life::sequence::Mode::open(
            #v_this.#f_life,
            #v_conn.async_state.session_mut(),
            #v_link,
        );
    }
}

pub(super) fn failed(paths: &Paths, generics: &crate::types::Generics) -> TokenStream {
    let f_life = local("lifecycle");
    let [v_error, v_exch, v_this] = generics.locals(["error", "exchange", "this"]);
    let ns_core = &paths.core;
    let open = open(paths, generics);
    quote! {
        #open
        #ns_core::life::sequence::Mode::reject(#v_this.#f_life, #v_exch);
        #v_error.into_consume().into()
    }
}

pub(super) fn rejected(paths: &Paths, generics: &crate::types::Generics) -> TokenStream {
    let f_life = local("lifecycle");
    let [v_exch, v_result, v_this] = generics.locals(["exchange", "outcome", "this"]);
    let ns_core = &paths.core;
    let ns_h1 = &paths.h1;
    let open = open(paths, generics);
    quote! {
        if ::core::matches!(
            &#v_result,
            #ns_h1::session::RouteOutcome::Outcome(
                #ns_h1::session::Outcome::Close(_)
                | #ns_h1::session::Outcome::Failed(_),
            )
        ) {
            #open
            #ns_core::life::sequence::Mode::reject(#v_this.#f_life, #v_exch);
        }
    }
}

pub(super) fn copied(
    paths: &Paths,
    generics: &crate::types::Generics,
    response: &TokenStream,
) -> TokenStream {
    let f_life = local("lifecycle");
    let [n_copy, n_out, v_exch] = generics.locals(["__copy", "__out", "exchange"]);
    let [v_this] = generics.locals(["this"]);
    let ns_core = &paths.core;
    let ns_h1 = &paths.h1;
    quote! {
        if let ::core::option::Option::Some(#n_out) =
            #ns_core::life::Mode::observer(#v_this.#f_life)
        {
            if let #ns_h1::wire::output::Copy::Copied(#n_copy) = #response.copy() {
                #ns_core::life::Observer::observe(
                    #n_out,
                    #ns_core::life::Event::ResponseBodyCopied {
                        exchange: #v_exch,
                        read: #n_copy.read,
                        written: #n_copy.written,
                        cause: #n_copy.cause,
                    },
                );
            }
        }
    }
}

pub(super) fn retention_arms(
    paths: &Paths,
    generics: &crate::types::Generics,
    origin: &TokenStream,
    session: &TokenStream,
) -> TokenStream {
    let f_life = local("lifecycle");
    let [n_retain, v_exch] = generics.locals(["__retention", "exchange"]);
    let [v_this] = generics.locals(["this"]);
    let ns_core = &paths.core;
    let ns_http = &paths.http;
    let commit = |linked: TokenStream| {
        quote! {
            #ns_core::life::sequence::Mode::commit_link(
                #v_this.#f_life,
                #session,
                #v_exch,
                #n_retain.len,
                #n_retain.charge,
                #linked,
            );
        }
    };
    let response = commit(quote!(#ns_http::o3::buffer::hold::Charge::default()));
    let linked = commit(quote!(#n_retain.charge));
    quote! {
        #origin::Response(#n_retain) => { #response }
        #origin::Linked(#n_retain) => { #linked }
    }
}

pub(super) fn completed(
    paths: &Paths,
    generics: &crate::types::Generics,
    session: &TokenStream,
) -> TokenStream {
    let f_life = local("lifecycle");
    let [v_exch, v_resp, v_this] = generics.locals(["exchange", "response", "this"]);
    let ns_core = &paths.core;
    let ns_h1 = &paths.h1;
    let copied = copied(paths, generics, &quote!(#v_resp));
    let retained = retention_arms(
        paths,
        generics,
        &quote!(#ns_h1::wire::output::Origin),
        session,
    );
    quote! {
        #copied
        match #v_resp.retention() { #retained }
        #v_exch.retire(#v_this.#f_life, #ns_core::life::Outcome::Completed);
    }
}

pub(super) fn guard(
    paths: &Paths,
    generics: &crate::types::Generics,
    method: &impl quote::ToTokens,
    slot: &impl quote::ToTokens,
    state: &syn::Type,
) -> TokenStream {
    let f_life = local("lifecycle");
    let [n_gform] = generics.locals(["__guard_form"]);
    let [n_ghead] = generics.locals(["__guard_head"]);
    let [n_gmeth] = generics.locals(["__guard_method"]);
    let [n_gout, n_reject] = generics.locals(["__guard_outcome", "__reject"]);
    let [n_resp, v_conn, v_date] = generics.locals(["__response", "conn", "date"]);
    let [v_exch, v_head, v_input] = generics.locals(["exchange", "head", "input"]);
    let [v_permit, v_state, v_this] = generics.locals(["permit", "state", "this"]);
    let [v_write] = generics.locals(["write"]);
    let ns_core = &paths.core;
    let ns_h1 = &paths.h1;
    let ns_http = &paths.http;
    let open = open(paths, generics);
    let copied = copied(paths, generics, &quote!(#n_gout));
    let retained = retention_arms(
        paths,
        generics,
        &quote!(#ns_h1::wire::output::Retention),
        &quote!(#v_conn.async_state.session_mut()),
    );
    quote! {
        let #n_gmeth = #method;
        let #n_gform = match #v_head.plan().form() {
            #ns_http::h1::target::Form::Origin
            | #ns_http::h1::target::Form::Absolute =>
                #ns_http::request::Form::Path,
            #ns_http::h1::target::Form::Authority =>
                #ns_http::request::Form::Authority,
            #ns_http::h1::target::Form::Asterisk =>
                #ns_http::request::Form::Asterisk,
        };
        let #n_ghead = #ns_core::route::guard::Head::from_parts(
            &#n_gmeth,
            #n_gform,
            #v_head.path(),
            #v_head.authority(),
        );
        let (#v_write, #v_date) = match <#slot as #ns_core::route::guard::Slot<#state>>::try_admit(
            #n_ghead,
            #v_state,
            #ns_h1::wire::output::Egress::bind(&mut *#v_write, #v_date, #v_input, #n_resp),
        ) {
            ::core::ops::ControlFlow::Continue(#n_reject) => #n_reject.into_parts(),
            ::core::ops::ControlFlow::Break(#n_gout) => {
                #open
                #ns_core::life::Mode::observe(
                    #v_this.#f_life,
                    #ns_core::life::Event::HeadAdmitted(#v_exch),
                );
                #copied
                match #n_gout.retention() {
                    #retained
                    #ns_h1::wire::output::Retention::Rejected => {}
                }
                #v_exch.retire(
                    #v_this.#f_life,
                    #ns_core::life::Outcome::Rejected,
                );
                return #n_gout.into_consume(
                    #v_permit,
                    #ns_h1::session::Consumption::Buffered(
                        #ns_h1::session::Buffered::new(0),
                    ),
                    #ns_h1::wire::head::Connection::Close,
                ).into();
            }
        };
    }
}
