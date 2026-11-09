use crate::{
    json::{field, scalar},
    macros::tokens,
    runtime::paths::Paths,
    types,
};

pub(super) fn expr(
    paths: &Paths,
    ty: &syn::Type,
    mode: field::FieldMode,
) -> syn::Result<proc_macro2::TokenStream> {
    let ns_json = &paths.json;
    let [v_raw, v_idx, v_depth] = ["__raw", "__idx", "__depth"].map(tokens::local);
    let [v_value, v_end, v_vec] = ["__value", "__end", "__v"].map(tokens::local);
    if mode.seq {
        let elem = types::View::new(ty)
            .vector()
            .ok_or_else(|| syn::Error::new_spanned(ty, "#[field(seq)] requires a Vec<T> field"))?;
        let value = if mode.nested {
            quote::quote! {{
                let (#v_value, #v_end) =
                    <#elem as #ns_json::parsing::Request>::prefix(
                        #v_raw, #v_idx, #v_depth.child()?,
                    )?;
                #v_idx = #v_end;
                #v_value
            }}
        } else {
            let class = scalar::Classified::of(elem)?;
            if class.optional {
                return Err(syn::Error::new_spanned(
                    elem,
                    "optional JSON sequence elements are unsupported",
                ));
            }
            match class.scalar {
                scalar::Scalar::Bytes => {
                    quote::quote!(#ns_json::Source::frame(#v_raw, &mut #v_idx)?)
                }
                scalar::Scalar::Borrowed => {
                    if mode.raw {
                        quote::quote!(#ns_json::Source::raw_depth(
                                #v_raw,
                                &mut #v_idx,
                                #v_depth,
                            )?)
                    } else if mode.plain {
                        quote::quote!(#ns_json::Source::frame_plain(
                                #v_raw,
                                &mut #v_idx,
                            )?)
                    } else {
                        return Err(syn::Error::new_spanned(
                            elem,
                            "borrowed JSON byte sequences require `raw` or `plain`",
                        ));
                    }
                }
                scalar::Scalar::U64
                | scalar::Scalar::I64
                | scalar::Scalar::F64
                | scalar::Scalar::Bool
                | scalar::Scalar::String
                | scalar::Scalar::Token
                | scalar::Scalar::Shared => {
                    return Err(syn::Error::new_spanned(
                        elem,
                        "unsupported JSON sequence field type",
                    ));
                }
            }
        };
        return Ok(quote::quote! {{
            #ns_json::input::Input::ws(#v_raw, &mut #v_idx);
            #ns_json::input::Input::expect_byte(#v_raw, &mut #v_idx, b'[')?;
            let #v_depth = #v_depth.child()?;
            let mut #v_vec = ::std::vec::Vec::new();
            #ns_json::input::Input::ws(#v_raw, &mut #v_idx);
            if #ns_json::input::Input::take_byte(#v_raw, &mut #v_idx, b']')
                == #ns_json::input::Taken::No
            {
                loop {
                    #ns_json::input::Input::ws(#v_raw, &mut #v_idx);
                    #ns_json::o3::alloc::try_vacant(&mut #v_vec)
                        .map_err(#ns_json::Error::Allocation)?(#value);
                    #ns_json::input::Input::ws(#v_raw, &mut #v_idx);
                    if #ns_json::input::Input::take_byte(#v_raw, &mut #v_idx, b',')
                        == #ns_json::input::Taken::Yes
                    {
                        continue;
                    }
                    #ns_json::input::Input::expect_byte(#v_raw, &mut #v_idx, b']')?;
                    break;
                }
            }
            #v_vec
        }});
    }
    if mode.nested {
        return Ok(quote::quote! {{
            #ns_json::input::Input::ws(#v_raw, &mut #v_idx);
            let (#v_value, #v_end) =
                <#ty as #ns_json::parsing::Request>::prefix(
                    #v_raw, #v_idx, #v_depth.child()?,
                )?;
            #v_idx = #v_end;
            #v_value
        }});
    }
    let class = scalar::Classified::of(ty)?;
    let decode = match class.scalar {
        scalar::Scalar::U64 => {
            quote::quote!(#ns_json::input::Input::parse_u64(#v_raw, &mut #v_idx)?)
        }
        scalar::Scalar::I64 => {
            quote::quote!(#ns_json::input::parse_i64(#v_raw, &mut #v_idx)?)
        }
        scalar::Scalar::F64 | scalar::Scalar::String => {
            return Err(syn::Error::new_spanned(
                ty,
                "this JSON field type requires an encoding-only mode",
            ));
        }
        scalar::Scalar::Bool => {
            quote::quote!(#ns_json::input::Input::parse_bool(#v_raw, &mut #v_idx)?)
        }
        scalar::Scalar::Borrowed => {
            if mode.raw {
                quote::quote!(#ns_json::Source::raw_depth(#v_raw, &mut #v_idx, #v_depth)?)
            } else {
                quote::quote!(#ns_json::Source::frame_plain(#v_raw, &mut #v_idx)?)
            }
        }
        scalar::Scalar::Shared => {
            return Err(syn::Error::new_spanned(
                ty,
                "retained JSON bytes must be rewritten for request views",
            ));
        }
        scalar::Scalar::Bytes => quote::quote!(#ns_json::Source::frame(#v_raw, &mut #v_idx)?),
        scalar::Scalar::Token => {
            if mode.plain && !mode.raw {
                quote::quote!(#ns_json::Source::inline_plain(#v_raw, &mut #v_idx)?)
            } else {
                quote::quote!(#ns_json::Source::inline_depth(
                    #v_raw, &mut #v_idx, #v_depth,
                )?)
            }
        }
    };
    let decode = if matches!(
        class.scalar,
        scalar::Scalar::Token | scalar::Scalar::Borrowed
    ) && !mode.raw
        && !mode.plain
    {
        quote::quote!((#decode).into())
    } else {
        decode
    };
    Ok(if class.optional {
        quote::quote!({
            if #ns_json::input::Input::take_null(#v_raw, &mut #v_idx)
                == #ns_json::input::Taken::Yes
            {
                ::core::option::Option::None
            } else {
                ::core::option::Option::Some(#decode)
            }
        })
    } else {
        decode
    })
}
