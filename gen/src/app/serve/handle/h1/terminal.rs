use proc_macro2::TokenStream;
use quote::quote;

use crate::{
    app::{serve::handle, spec},
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(super) fn routes(
    paths: &Paths,
    generics: &crate::types::Generics,
    conn: &TokenStream,
    runtime: &TokenStream,
) -> TokenStream {
    let f_life = local("lifecycle");
    let [v_cause, v_conn, v_error] = generics.locals(["cause", "conn", "error"]);
    let [v_wait, v_this] = generics.locals(["pending", "this"]);
    let [v_link, v_exch] = generics.locals(["connection", "exchange"]);
    let v_run = generics.local("runtime");
    let ns_core = &paths.core;
    let ns_h1 = &paths.h1;
    let ns_http = &paths.http;
    quote! {
        fn failure(
            self: ::core::pin::Pin<&mut Self>,
            #v_cause: &#ns_h1::wire::Failure,
        ) -> #ns_http::payload::fixed::Body<'d> {
            let #v_this = self.get_mut().app.as_mut().project();
            #ns_core::life::Mode::diagnostic(#v_this.#f_life, #v_cause);
            #v_cause.body()
        }

        fn reject_head(self: ::core::pin::Pin<&mut Self>, #v_run: &mut #runtime) {
            let #v_this = self.get_mut();
            let #v_link = #v_this.id;
            let #v_this = #v_this.app.as_mut().project();
            let #v_exch = #ns_core::life::sequence::Mode::open(
                #v_this.#f_life, #v_run.session_mut(), #v_link,
            );
            #ns_core::life::sequence::Mode::reject(#v_this.#f_life, #v_exch);
        }

        fn abort_body(
            mut self: ::core::pin::Pin<&mut Self>,
            #v_conn: &mut #conn,
            #v_error: #ns_h1::wire::Failure,
        ) -> #ns_http::payload::fixed::Body<'d> {
            if let ::core::option::Option::Some(#v_wait) = #v_conn.ingress.take_pending() {
                self.as_mut().get_mut().app.as_mut().__cancel_pending(#v_wait);
            }
            #v_conn.ingress.finish_frame();
            #ns_h1::session::Dispatch::failure(self, &#v_error)
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

pub(super) fn guard(
    paths: &Paths,
    generics: &crate::types::Generics,
    entry: &spec::Entry,
    state: &syn::Type,
) -> Option<TokenStream> {
    if entry.guards.is_empty() {
        return None;
    }
    let method = entry.method.http_token(paths);
    let f_life = local("lifecycle");
    let [n_gform] = generics.locals(["__guard_form"]);
    let [n_ghead] = generics.locals(["__guard_head"]);
    let [n_gmeth] = generics.locals(["__guard_method"]);
    let [n_gout, n_body, n_error] =
        generics.locals(["__guard_outcome", "__guard_body", "__guard_error"]);
    let [n_resp, v_conn, v_date] = generics.locals(["__response", "conn", "date"]);
    let [v_exch, v_head, v_parsed, n_shead] =
        generics.locals(["exchange", "head", "parsed", "__sark_head"]);
    let [v_permit, v_state, v_this] = generics.locals(["permit", "state", "this"]);
    let [v_write] = generics.locals(["write"]);
    let ns_core = &paths.core;
    let ns_h1 = &paths.h1;
    let ns_http = &paths.http;
    let pass = generics.lifetime("__guard_passed");
    let rejection = handle::guard_rejection(
        paths,
        generics,
        &entry.guards,
        state,
        &v_state,
        quote!(#ns_h1::wire::output::Egress::bind(#v_write, #v_date, #v_parsed.input(), #n_resp)),
        quote!(break #pass),
    );
    let open = open(paths, generics);
    let observer = generics.local("__observer");
    Some(quote! { #pass: {
        let #v_parsed = #n_shead.source();
        let #v_head = #v_parsed.request();
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
        let #n_gout = #rejection;
        #open
        #ns_core::life::Mode::observe(
            #v_this.#f_life,
            #ns_core::life::Event::HeadAdmitted(#v_exch),
        );
        let #observer = #ns_core::life::Mode::observer(#v_this.#f_life);
        let #n_gout = match #n_gout {
            ::core::result::Result::Ok(#n_body) => {
                #n_body.observe_copy(#observer, #v_exch);
                #n_body.commit_retention(#v_this.#f_life, #v_conn.async_state.session_mut(), #v_exch);
                #n_body.into_consume(
                    #v_permit,
                    #ns_h1::session::Consumption::Buffered(#ns_h1::session::Buffered::new(0)),
                    #ns_h1::wire::head::Connection::Close,
                )
            }
            ::core::result::Result::Err(#n_error) => #ns_h1::session::Outcome::Failed(#n_error),
        };
        #v_exch.retire(#v_this.#f_life, #ns_core::life::Outcome::Rejected);
        return #n_gout.into();
    } })
}
