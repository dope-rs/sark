use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Ident, LitStr, Result, Token, Type, TypePath, Visibility, braced};

pub(super) struct DefineRouteInput {
    pub(super) vis: Visibility,
    pub(super) name: Ident,
    pub(super) state_ty: Type,
    pub(super) entries: Vec<DefineRouteEntry>,
}

pub(super) enum DefineRouteEntry {
    Service {
        method: Ident,
        path: LitStr,
        ty: TypePath,
    },
    Scope {
        prefix: LitStr,
        wraps: Vec<TypePath>,
        children: Vec<DefineRouteEntry>,
    },
}

impl Parse for DefineRouteInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let vis = input.parse::<Visibility>()?;
        let name = input.parse::<Ident>()?;
        input.parse::<Token![:]>()?;
        let state_ty = input.parse::<Type>()?;
        input.parse::<Token![=>]>()?;
        let body;
        braced!(body in input);
        let entries = parse_define_route_entries(&body)?;
        Ok(Self {
            vis,
            name,
            state_ty,
            entries,
        })
    }
}

fn parse_define_route_entries(input: ParseStream<'_>) -> Result<Vec<DefineRouteEntry>> {
    let mut out = Vec::new();
    while !input.is_empty() {
        out.push(parse_define_route_entry(input)?);
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
        }
    }
    Ok(out)
}

fn parse_define_route_entry(input: ParseStream<'_>) -> Result<DefineRouteEntry> {
    if next_ident_is(input, "scope")? {
        let _ = input.parse::<Ident>()?;
        let prefix = input.parse::<LitStr>()?;
        let mut wraps = Vec::new();
        if next_ident_is(input, "with")? {
            input.parse::<Ident>()?;
            let wrap_body;
            syn::parenthesized!(wrap_body in input);
            let punctuated: syn::punctuated::Punctuated<syn::TypePath, Token![,]> =
                Punctuated::parse_terminated(&wrap_body)?;
            wraps = punctuated.into_iter().collect();
        }
        input.parse::<Token![=>]>()?;
        let children = if input.peek(syn::token::Bracket) {
            let body;
            syn::bracketed!(body in input);
            let mut services = Vec::new();
            while !body.is_empty() {
                let method = body.parse::<Ident>()?;
                let path = body.parse::<LitStr>()?;
                body.parse::<Token![=>]>()?;
                let ty = parse_route_type(&body)?;
                services.push(DefineRouteEntry::Service { method, path, ty });
                if body.peek(Token![,]) {
                    body.parse::<Token![,]>()?;
                }
            }
            services
        } else if input.peek(syn::token::Brace) {
            let body;
            braced!(body in input);
            parse_define_route_entries(&body)?
        } else {
            return Err(input.error("expected `[ ... ]` or `{ ... }` after scope `=>`"));
        };
        Ok(DefineRouteEntry::Scope {
            prefix,
            wraps,
            children,
        })
    } else {
        let method = input.parse::<Ident>()?;
        let path = input.parse::<LitStr>()?;
        input.parse::<Token![=>]>()?;
        let ty = parse_route_type(input)?;
        Ok(DefineRouteEntry::Service { method, path, ty })
    }
}

fn parse_route_type(input: ParseStream<'_>) -> Result<syn::TypePath> {
    let marker = if input.peek(Token![async]) {
        input.parse::<Token![async]>()?;
        Some("__SarkAsyncRoute")
    } else if next_ident_is(input, "stream")? {
        input.parse::<Ident>()?;
        Some("__SarkStreamRoute")
    } else {
        None
    };
    let Some(marker) = marker else {
        return input.parse();
    };
    if !input.peek(syn::token::Paren) {
        return Err(input.error("async and stream routes require `(capacity = N)`"));
    }
    let options;
    syn::parenthesized!(options in input);
    let key = options.parse::<Ident>()?;
    if key != "capacity" {
        return Err(syn::Error::new(key.span(), "expected `capacity`"));
    }
    options.parse::<Token![=]>()?;
    let capacity = options.parse::<syn::Expr>()?;
    if !options.is_empty() {
        return Err(options.error("trailing tokens after `capacity = N`"));
    }
    let route = input.parse::<syn::TypePath>()?;
    let marker = Ident::new(marker, proc_macro2::Span::call_site());
    Ok(syn::parse_quote!(#marker<#route, { #capacity }>))
}

fn next_ident_is(input: ParseStream<'_>, expected: &str) -> Result<bool> {
    if !input.peek(Ident) {
        return Ok(false);
    }
    Ok(input.fork().parse::<Ident>()? == expected)
}
