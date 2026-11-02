use crate::{macros::tokens::local, request::values::FieldPlan, runtime::paths::Paths};

pub(in crate::request) fn try_ordered(
    paths: &Paths,
    plan: &FieldPlan,
    identity: &proc_macro2::Ident,
    symbols: &crate::types::Generics,
) -> syn::Result<proc_macro2::TokenStream> {
    let ns_core = &paths.core;
    let [v_heads, v_entry, v_state, v_range] = ["headers", "entry", "state", "range"].map(local);
    let query = symbols.ident("__query");
    let fields = plan.entries();
    if let Some(field) = fields.get(usize::from(u8::MAX)) {
        return Err(syn::Error::new(
            field.span,
            "request supports at most 255 query fields",
        ));
    }
    let states: Vec<_> = fields
        .iter()
        .zip((0u8..u8::MAX).zip(1u8..=u8::MAX))
        .map(|(field, (state, next))| {
            use crate::request::{header::bytes, values::emit_query};
            let ident = &field.ident;
            let name = bytes::Case::Exact.emit(&local("name"), &field.bytes);
            let slot = quote::format_ident!("Query{state}");
            let assign = emit_query(paths, field.kind, quote::quote!(#identity::#slot));
            quote::quote! {
                #state => {
                    if #name {
                        #v_heads.#ident = #assign;
                        #v_heads.#query = #next;
                        break;
                    }
                    #v_heads.#query = #next;
                    continue;
                }
            }
        })
        .collect();
    let count = fields.len();
    Ok(build(
        paths,
        quote::quote! {
                loop {
                    match #v_heads.#query {
                        #( #states, )*
                        #v_state => {
                            let #v_range = #v_entry.name_range();
                            return ::core::result::Result::Err(#ns_core::route::Error::QueryExhausted {
                                state: #v_state,
                                fields: #count,
                                start: #v_range.start,
                                end: #v_range.end,
                            });
                        }
                    }
                }
        },
    ))
}

pub(in crate::request) fn build(
    paths: &Paths,
    segment: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let ns_core = &paths.core;
    let [
        v_items,
        v_input,
        v_range,
        v_err,
        v_start,
        v_end,
        v_cap,
        v_entry,
        v_name,
    ] = [
        "entries", "input", "range", "error", "start", "end", "capacity", "entry", "name",
    ]
    .map(local);
    quote::quote! {
            let mut #v_items = #ns_core::route::field::query::Entries::try_new(#v_input, #v_range)
                .map_err(|#v_err| match #v_err {
                    #ns_core::route::field::query::Error::Order {
                        start: #v_start, end: #v_end, capacity: #v_cap,
                    } => #ns_core::route::Error::QueryOrder {
                        start: #v_start, end: #v_end, capacity: #v_cap,
                    },
                    #ns_core::route::field::query::Error::Bounds {
                        start: #v_start, end: #v_end, capacity: #v_cap,
                    } => #ns_core::route::Error::QueryBounds {
                        start: #v_start, end: #v_end, capacity: #v_cap,
                    },
                })?;
            ::core::iter::Iterator::try_for_each(&mut #v_items, |#v_entry| {
                let #v_name = #v_entry.name();
                #segment
                ::core::result::Result::Ok(())
            })
    }
}
