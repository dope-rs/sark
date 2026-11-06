use crate::{
    app::serve::{duplex, handle::h1::types},
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(super) fn render(paths: &Paths, types: &types::Types<'_>) -> proc_macro2::TokenStream {
    let f_life = local("lifecycle");
    let generics = types.names.generics;
    let event = generics.lifetime("e");
    let slot = &types.protocol.h1_slot;
    let view = &types.http;
    let [n_active, n_link] = generics.locals(["__activity", "__connection"]);
    let [n_until, n_drive] = generics.locals(["__deadline", "__drive"]);
    let [n_flow, n_now] = generics.locals(["__flow", "__now"]);
    let [n_prog, n_root] = generics.locals(["__progress", "__root"]);
    let [n_tail, v_bytes] = generics.locals(["__tail", "bytes"]);
    let [v_link, v_date, v_driver] = generics.locals(["connection", "date", "driver"]);
    let [v_range, v_staged, v_this] = generics.locals(["range", "staged", "this"]);
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    use quote::quote;

    let staged = match types.protocol.duplex {
        duplex::State::Enabled => quote!(()),
        duplex::State::Disabled => quote!(::core::convert::Infallible),
    };
    quote! {
            fn chunk<#event>(
                mut self: ::core::pin::Pin<&mut Self>,
                #v_date: &#ns_h1::server::date::Clock,
                #v_link: &mut #slot,
                #v_bytes: &[::core::primitive::u8],
                #v_driver: &mut #ns_fiber::dope::core::driver::Context<'_, 'd>,
            ) -> #ns_h1::server::conn::driver::Ingress<#staged> {
                let #n_link = #v_link.id();
                let #n_now = {
                    let mut #v_this = self.as_mut().project();
                    #ns_h1::server::deadline::Mode::begin(
                        #v_this.#f_life,
                        #v_driver.driver_ref().clock().now(),
                    )
                };
                let #n_drive = {
                    #ns_h1::server::conn::driver::Driver::new(self.as_mut(), #v_date).run(
                        #v_bytes,
                        #v_link,
                        #v_driver,
                    )
                };
                let #n_active = #ns_h1::pump::Tasks::activity(&#view.async_state);
                let (#n_flow, #n_tail, #n_prog) = #n_drive.into_parts();
                if #n_flow.is_runnable(#n_active) {
                    self.as_mut().activate(#v_date, #v_link, #v_driver);
                }
                let #n_root = {
                    let mut #v_this = self.as_mut().project();
                    #ns_h1::server::deadline::Mode::finish(
                        #v_this.#f_life,
                        #n_link,
                        #n_now,
                        #n_prog.head(),
                        #n_prog.body(),
                    )
                };
                use #ns_h1::pump::Tail;
                use #ns_h1::server::conn::driver::{Flow, Ingress};
                let #n_until = #n_root.try_apply(#v_link, #n_now);
                match (#n_flow, #n_until, #n_tail) {
                    (Flow::Closed, _, _) => Ingress::CloseAfter,
                    (
                        Flow::Continue,
                        ::core::result::Result::Ok(()),
                        Tail::Empty,
                    ) => Ingress::Continue,
                    (
                        Flow::Continue,
                        ::core::result::Result::Ok(()),
                        Tail::Handoff { staged: #v_staged, range: #v_range },
                    ) => Ingress::Handoff { staged: #v_staged, range: #v_range },
                    (
                        Flow::Continue,
                        ::core::result::Result::Err(_),
                        _,
                    ) => {
                        #v_link.close_after();
                        Ingress::CloseAfter
                    }
                }
            }
    }
}
