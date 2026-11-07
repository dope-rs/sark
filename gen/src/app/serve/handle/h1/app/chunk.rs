use crate::{
    app::serve::{duplex, handle::h1::types},
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(super) fn render(
    paths: &Paths,
    types: &types::Types<'_>,
    scratch: &proc_macro2::TokenStream,
    stamp: Option<&proc_macro2::TokenStream>,
) -> proc_macro2::TokenStream {
    use quote::quote;

    let [f_core, f_life, f_stamp] = ["core", "lifecycle", "stamp"].map(local);
    let symbols = types.names.generics;
    let dispatch = &types.names.dispatch_ident;
    let v_routes = symbols.local("routes");
    let [v_app, v_input, v_bytes] = symbols.locals(["app", "input", "bytes"]);
    let [v_link, v_driver, v_date] = symbols.locals(["connection", "driver", "date"]);
    let [v_this, v_error, v_range, v_staged] = symbols.locals(["this", "error", "range", "staged"]);
    let [n_link, n_now, n_drive] = symbols.locals(["__connection", "__now", "__drive"]);
    let [n_flow, n_tail, n_prog] = symbols.locals(["__flow", "__tail", "__progress"]);
    let [n_active, n_root] = symbols.locals(["__activity", "__root"]);
    let ns_http = &paths.http;
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    let clock = if stamp.is_some() || matches!(types.protocol.duplex, duplex::State::Enabled) {
        quote!(#v_app.as_ref().project_ref().#f_stamp.get_ref()#stamp.0)
    } else {
        quote!(*#v_app.as_ref().project_ref().#f_stamp.get_ref())
    };
    let (tail, handoff) = match types.protocol.duplex {
        duplex::State::Enabled => (
            quote!(staged: (), range: #v_range),
            quote! {
                match #v_input.try_plan(#v_range) {
                    ::core::result::Result::Ok(#n_tail) => #ns_h1::server::duplex::Runtime::new(
                        #v_link, #scratch,
                    ).handoff(#n_tail),
                    ::core::result::Result::Err(#v_error) => Outcome::Failed(
                        #ns_fiber::dope::manifold::app::receive::Failure::Hold(
                            #ns_http::o3::buffer::hold::Error::Range(#v_error),
                        ),
                    ),
                }
            },
        ),
        duplex::State::Disabled => (quote!(staged: #v_staged, ..), quote!(match #v_staged {})),
    };
    quote! {
        use #ns_h1::{
            pump::Tail,
            server::{conn::driver::Flow, deadline},
        };
        let #v_date = #clock;
        let #n_link = #v_link.id();
        let #n_now = {
            let #v_this = #v_app.as_mut().project().#f_core.project();
            deadline::Mode::begin(#v_this.#f_life, #v_driver.driver_ref().clock().now())
        };
        let #n_drive = {
            let mut #v_routes = #dispatch {
                app: #v_app.as_mut().project().#f_core,
                id: #n_link,
                date: #v_date,
            };
            #ns_h1::server::conn::driver::run(
                ::core::pin::Pin::new(&mut #v_routes),
                #v_bytes,
                #v_link,
                #v_driver,
            )
        };
        let #n_active = #ns_h1::pump::Tasks::activity(&#v_link.state().async_state);
        let (#n_flow, #n_tail, #n_prog) = #n_drive.into_parts();
        if #n_flow.is_runnable(#n_active) {
            #v_app.as_mut().project().#f_core.activate(#v_date, #v_link, #v_driver);
        }
        let #n_root = {
            let #v_this = #v_app.as_mut().project().#f_core.project();
            deadline::Mode::finish(
                #v_this.#f_life,
                #n_link,
                #n_now,
                #n_prog.head(),
                #n_prog.body(),
            )
        };
        match (#n_flow, #n_root.try_apply(#v_link, #n_now), #n_tail) {
            (Flow::Closed, _, _) => Outcome::CloseAfter,
            (Flow::Continue, ::core::result::Result::Ok(()), Tail::Empty) => {
                Outcome::Continue
            }
            (
                Flow::Continue,
                ::core::result::Result::Ok(()),
                Tail::Handoff { #tail },
            ) => {
                #handoff
            }
            (Flow::Continue, ::core::result::Result::Err(#v_error), _) => {
                #v_link.deadline().reject(#v_error);
                Outcome::CloseAfter
            }
        }
    }
}
