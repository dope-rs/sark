use crate::{
    app::serve::{
        self, duplex,
        handle::{self, decode},
    },
    runtime::paths::Paths,
};

pub(super) fn build(
    paths: &Paths,
    source: &serve::ProtocolPlan<'_>,
    plan: &handle::H1Plan<'_, '_>,
    create: &proc_macro2::TokenStream,
    bounds: &proc_macro2::TokenStream,
    branded: bool,
) -> proc_macro2::TokenStream {
    use quote::quote;

    let spec = source.spec;
    let symbols = &spec.state_generics;
    let [l_env, l_brand, l_wire, l_n, l_p, l_s] =
        symbols.lives(["env", "brand", "__w", "__n", "__p", "__s"]);
    let [l_issued, l_life, l_watch] = symbols.lives(["__issued", "__life", "__watch"]);
    let [g_id, g_wire, g_date, g_prof, g_recv, g_until] =
        symbols.idents(["__ID", "__W", "__DATE", "__PROFILE", "__RECEIVER", "__D"]);
    let [g_owner, g_shutdown] = symbols.idents(["__OWNER", "__SHUTDOWN"]);
    let [v_state, v_brand, v_config, v_prep, v_driver, v_app] =
        symbols.locals(["state", "brand", "config", "prepared", "driver", "app"]);
    let [v_permit, v_wait] = symbols.locals(["permit", "wait"]);
    let lives = &symbols.lifetimes;
    let decls = &symbols.parameters;
    let state = spec.state.ty();
    let name = &spec.name;
    let vis = &spec.vis;
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    let ns_http = &paths.http;
    let duplex = plan.duplex.state();
    let config = duplex.config(paths);
    let input = plan.duplex.state().input_type(paths);
    let brand = if branded { quote!(#v_brand) } else { quote!(_) };
    let bound = match duplex {
        duplex::State::Enabled => quote!(#ns_http::o3::buffer::resident::bound::Unbounded),
        duplex::State::Disabled => decode::egress(
            paths,
            spec,
            &source.entries,
            &syn::Lifetime::new("'d", proc_macro2::Span::call_site()),
        ),
    };
    let egress = quote! {
        #ns_fiber::dope::net::link::egress::data::Bound<
            #ns_fiber::dope::net::link::egress::data::compose::Either<
                #ns_h1::server::payload::Body<'d, ()>,
                #ns_h1::server::payload::Stream<'d, ()>,
            >,
            #bound,
        >
    };
    let prepared = quote! {
        #ns_h1::server::run::Prepared<
            #l_watch, #l_n, #l_p, #l_s, #l_wire, 'd, #g_id, #g_date,
            #ns_fiber::dope::manifold::app::env::Bundle<
                #ns_fiber::dope::net::tcp::Tcp, #g_wire, #g_prof,
            >,
            #g_recv, #egress, #input, #g_until,
        >
    };
    quote! {
        impl #name {
            /// Builds and runs this application while consuming its provider permit.
            #vis fn run_prepared<
                #l_env, #l_brand: #l_env, #l_wire, #l_n, #l_p, #l_s,
                #l_issued, #l_life: #l_issued, #l_watch,
                #( #lives: #l_env, )* 'd: #l_wire + #l_env + #l_life + #l_watch,
                #( #decls, )*
                #g_id: #ns_fiber::Identity + 'd,
                #g_wire: #ns_fiber::dope::net::link::pool::recv::input::Execution + 'd,
                #g_date: #ns_fiber::Identity,
                #g_prof, #g_recv, #g_until, #g_owner, #g_shutdown,
            >(
                #brand: #ns_fiber::dope_fiber::task::storage::scope::Brand<#l_brand>,
                #v_state: ::core::pin::Pin<&#l_env #state>,
                #v_config: #config,
                #v_prep: #prepared,
                #v_wait: #ns_h1::server::run::Wait<#l_watch, 'd>,
                #v_permit: #ns_fiber::dope::runtime::topology::lending::Permit<
                    #l_issued, #l_life, 'd, #g_owner, #g_shutdown,
                >,
            ) -> #ns_fiber::dope::runtime::topology::lending::Outcome<
                #l_issued, #l_life, 'd, #g_owner, #g_shutdown,
                #ns_fiber::dope::manifold::BuildError<#ns_h1::server::listener::bind::Error>,
                #ns_h1::server::run::Pending<
                    #l_watch, 'd,
                    #ns_fiber::dope::runtime::shutdown::Requested<#g_shutdown>,
                >,
            >
            where
                #bounds
                #g_prof: #ns_fiber::dope::core::driver::settings::Profile
                    + #ns_fiber::dope::manifold::timing::Policy,
                #g_recv: #ns_fiber::dope::net::link::pool::change::Prepare<
                    #l_wire, 'd, #g_id, #g_wire,
                >,
                #g_until: #ns_h1::server::deadline::policies::Policy<'d, #g_id>,
                #egress: #ns_fiber::dope::net::link::egress::data::raw::Payload<'d> + 'd,
                #prepared: #l_env,
                #g_owner: #ns_fiber::dope::runtime::topology::lending::raw::Resume<'d>,
                #g_shutdown: #ns_fiber::dope::runtime::executor::Startup,
            {
                #v_permit.try_run(
                    |mut #v_driver| {
                        let #v_state = #v_state.get_ref();
                        let #v_app = { #create };
                        #v_prep.build(#v_app, &mut #v_driver)
                            .map(|#v_app| (#v_app, ()))
                    },
                    |(), #v_app| #v_wait.run(#v_app),
                )
            }
        }
    }
}
