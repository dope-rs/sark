use decode::state;
use handle::decode;

use crate::{
    app::{serve::handle, spec},
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(super) fn generate(
    paths: &Paths,
    ctx: &handle::Context<'_, '_, '_>,
    state: &state::State,
) -> Vec<proc_macro2::TokenStream> {
    let f_state = local("state");
    let symbols = &ctx.plan.spec.state_generics;
    let [n_body, n_call] = symbols.locals(["__body", "__call"]);
    let [n_sess, v_cause] = symbols.locals(["__session", "cause"]);
    let [v_len, v_head] = symbols.locals(["content_length", "head"]);
    use quote::{format_ident, quote};

    let ns_core = &paths.core;

    let prep = &state.prepared;
    let error = format_ident!("{}ProjectError", ctx.plan.spec.name);
    let state_ty = ctx.plan.spec.state.ty();
    let rejects = ctx
        .plan
        .entries
        .iter()
        .filter(|item| item.kind != spec::Kind::Duplex)
        .map(|item| {
            let variant = &item.prepared;
            quote! {
                #prep::#variant(_) =>
                    ::core::result::Result::Err(
                        #ns_core::duplex::OpenError::Unsupported,
                    )
            }
        });
    rejects
        .chain(ctx.exec.duplexes().iter().map(|(item, family)| {
            let entry = item.entry;
            let variant = &item.prepared;
            let slot = &item.request;
            let route = &entry.route;
            let wrapped = ctx.plan.duplex.wrap(*family, quote!(#n_sess));
            quote! {
                #prep::#variant(#slot {
                    head: #v_head,
                    content_length: #v_len,
                }) => {
                    match #v_len {
                        ::core::option::Option::Some(0)
                        | ::core::option::Option::None => {}
                        ::core::option::Option::Some(1..) => {
                            return ::core::result::Result::Err(
                                #ns_core::duplex::OpenError::Length,
                            );
                        }
                    }
                    let #n_body =
                        <<<#route as #ns_core::route::Spec>::Request
                            as #ns_core::route::Schema>::Body
                            as #ns_core::body::Project>::empty::<'_, 'd>();
                    let #n_call = #ns_core::decode::Call::new(
                        #v_head.borrow(),
                        #n_body,
                    );
                    let #n_sess = #n_call.try_open::<#route, #state_ty>(
                        self.#f_state,
                    ).map_err(|#v_cause| {
                        #ns_core::duplex::OpenError::InvalidRequest(
                            #error::#variant(#v_cause),
                        )
                    })?;
                    ::core::result::Result::Ok(#wrapped)
                }
            }
        }))
        .collect()
}
