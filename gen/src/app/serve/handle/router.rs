use crate::{
    app::{
        serve::{
            self, duplex,
            handle::{decode, dispatch, head},
        },
        spec,
    },
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(in crate::app) fn build(paths: &Paths, spec: &spec::Spec) -> proc_macro2::TokenStream {
    let f_state = local("state");
    let symbols = &spec.state_generics;
    let v_state = symbols.local("state");
    let l_state = symbols.lifetime("__state");
    use quote::quote;

    let name = &spec.name;
    let entries = spec
        .route_specs
        .iter()
        .enumerate()
        .map(|(index, entry)| serve::RouteSpec::new(name, index, entry, spec::Kind::Sync, ()))
        .collect::<Vec<_>>();
    let entries = &entries;
    let methods = dispatch::Methods::build(paths, spec);
    let connect = entries
        .iter()
        .fold(duplex::Connect::Disabled, |connect, item| {
            connect.register(item.entry)
        });
    let head = head::Head::build(paths, spec, &methods, entries, connect);
    let decode = decode::Decode::build(paths, spec, &methods, &head, entries, quote!(self));
    let vis = &spec.vis;
    let state_ty = spec.state.ty();
    let lives = &spec.state_generics.lifetimes;
    let decls = &spec.state_generics.parameters;
    let values = &spec.state_generics.arguments;
    let clauses = &spec.state_generics.predicates;
    let routes = &spec.route_bounds;
    let bounds = quote! {
        #( #clauses, )*
        #( #routes )*
    };
    let (decl, args, router) = match &spec.state {
        spec::State::Empty(_) => (
            quote!(),
            quote!(),
            quote! {
                #[derive(::core::clone::Clone, ::core::marker::Copy, ::core::default::Default)]
                #vis struct #name;

                impl #name {
                    #vis const fn new() -> Self {
                        Self
                    }
                }

                const _: () = ::core::assert!(::core::mem::size_of::<#name>() == 0);
            },
        ),
        spec::State::Bound(_) => {
            let decl = quote!(<#l_state, #( #lives: #l_state, )* #( #decls, )*>);
            let args = quote!(<#l_state, #( #lives, )* #( #values, )*>);
            let router = quote! {
                #[repr(transparent)]
                #vis struct #name #decl
                where
                    #( #clauses, )*
                {
                    #f_state: &#l_state #state_ty,
                }

                impl #decl #name #args
                where
                    #( #clauses, )*
                {
                    #[inline(always)]
                    #vis const fn new(#v_state: &#l_state #state_ty) -> Self {
                        Self { #f_state: #v_state }
                    }
                }
            };
            (decl, args, router)
        }
    };
    let decoder = decode.implement(paths, &decl, quote!(#name #args where #bounds));

    quote! {
        #decoder
        #router
    }
}
