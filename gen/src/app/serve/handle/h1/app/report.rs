use crate::{app::serve::handle::h1::types, macros::tokens::local, runtime::paths::Paths};

pub(super) fn render(paths: &Paths, types: &types::Types<'_>) -> proc_macro2::TokenStream {
    let f_life = local("lifecycle");
    let symbols = &types.names.generics;
    let [v_body, v_cause, v_charge] = symbols.locals(["body", "cause", "charge"]);
    let [v_link, v_core, v_egress] = symbols.locals(["connection", "core", "egress"]);
    let [v_exch, v_free, v_tag] = symbols.locals(["exchange", "release", "tag"]);
    let [v_this, v_trans] = symbols.locals(["this", "transport"]);
    let [g_id, g_mode] = types.names.generics.idents(["__ID", "__M"]);
    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    let ns_http = &paths.http;
    use quote::quote;

    let name = types.names.name;
    let bounds = &types.generics.bounds;
    let def = &types.generics.application_params;
    let args = &types.generics.application_generic_use;
    quote! {
        impl<#def>
            #ns_fiber::dope::manifold::network::listener::handler::send::Report<'d, #g_id>
            for #name #args
        where
            #bounds
            #g_mode: #ns_h1::server::deadline::Mode<'d, #g_id>,
        {
            const ENABLED: ::core::primitive::bool = <#g_mode as #ns_core::life::Mode<
                #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                ::core::primitive::u64,
            >>::ENABLED;

            fn released(
                self: ::core::pin::Pin<&mut Self>,
                #v_free: #ns_fiber::dope::manifold::network::listener::handler::Release<
                    'd,
                     #g_id,
                    Self::Egress,
                >,
            ) {
                let (#v_link, #v_egress, #v_cause) = #v_free.into_parts();
                let #v_tag = match #v_egress.into_inner() {
                    #ns_fiber::dope::net::link::egress::data::compose::Either::Left(#v_body) => {
                        #v_body.release()
                    }
                    #ns_fiber::dope::net::link::egress::data::compose::Either::Right(#v_body) => {
                        #v_body.release()
                    }
                };
                let #v_cause = match #v_cause {
                    #ns_fiber::dope::manifold::network::listener::handler::Disposition::Sent => {
                        #ns_http::owner::cause::Transport::Sent
                    }
                    #ns_fiber::dope::manifold::network::listener::handler::Disposition::Closed => {
                        #ns_http::owner::cause::Transport::Closed
                    }
                };
                let (#v_core, _) = self.__project();
                let #v_this = #v_core.project();
                let #v_trans = #ns_core::life::sequence::Mode::transport(
                    #v_this.#f_life,
                    #v_link,
                    #v_tag,
                );
                let ::core::option::Option::Some((#v_exch, #v_charge)) = #v_trans else {
                    return;
                };
                #ns_core::life::sequence::Mode::release(
                    #v_this.#f_life,
                    #v_exch,
                    #v_charge,
                    #v_cause,
                );
            }
        }
    }
}
