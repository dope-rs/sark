use proc_macro::TokenStream;
use proc_macro2::{self, Literal, Span};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Expr, LitStr, Result, Token};

pub(super) struct TextInput {
    format: LitStr,
    args: Punctuated<Expr, Token![,]>,
}

impl TextInput {
    pub(super) fn body(input: TokenStream) -> TokenStream {
        let input = syn::parse_macro_input!(input as TextInput);
        input
            .expand()
            .unwrap_or_else(|err| err.to_compile_error())
            .into()
    }

    fn expand(self) -> Result<proc_macro2::TokenStream> {
        let segs = Seg::split(&self.format.value(), self.format.span())?;
        let holes = segs.iter().filter(|seg| matches!(seg, Seg::Hole)).count();
        if holes != self.args.len() {
            return Err(syn::Error::new(
                self.format.span(),
                format!(
                    "body! placeholder count mismatch: expected {holes}, got {}",
                    self.args.len()
                ),
            ));
        }

        let args: Vec<Expr> = self.args.into_iter().collect();
        let vals: Vec<_> = (0..args.len())
            .map(|idx| format_ident!("__body_arg_{idx}"))
            .collect();
        let bytes: Vec<_> = (0..args.len())
            .map(|idx| format_ident!("__body_arg_{idx}_bytes"))
            .collect();
        let binds = vals
            .iter()
            .zip(bytes.iter())
            .zip(args.iter())
            .map(|((value, raw), expr)| {
                quote! {
                    let #value = (#expr);
                    let #raw: &[u8] = ::core::convert::AsRef::<[u8]>::as_ref(&#value);
                }
            });

        let mut hole_idx = 0usize;
        let caps = segs.iter().map(|seg| match seg {
            Seg::Lit(text) => {
                let len = text.len();
                quote!(#len)
            }
            Seg::Hole => {
                let ident = &bytes[hole_idx];
                hole_idx += 1;
                quote!(#ident.len())
            }
        });

        let mut writes = Vec::new();
        let mut write_idx = 0usize;
        for seg in &segs {
            match seg {
                Seg::Lit(text) => {
                    let lit = Literal::byte_string(text.as_bytes());
                    writes.push(quote!(__body_buf.extend_from_slice(#lit);));
                }
                Seg::Hole => {
                    let ident = &bytes[write_idx];
                    write_idx += 1;
                    writes.push(quote!(__body_buf.extend_from_slice(#ident);));
                }
            }
        }

        Ok(quote!({
            #(#binds)*
            let __body_cap = 0usize #(+ #caps)*;
            let mut __body_buf = Vec::with_capacity(__body_cap);
            #(#writes)*
            __body_buf
        }))
    }
}

impl Parse for TextInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let format = input.parse::<LitStr>()?;
        let mut args = Punctuated::new();
        if input.peek(Token![,]) {
            let _ = input.parse::<Token![,]>()?;
            args = Punctuated::parse_terminated(input)?;
        }
        Ok(Self { format, args })
    }
}

enum Seg {
    Lit(String),
    Hole,
}

impl Seg {
    fn split(value: &str, span: Span) -> Result<Vec<Seg>> {
        let bytes = value.as_bytes();
        let mut out = Vec::new();
        let mut lit = Vec::new();
        let mut idx = 0usize;
        while idx < bytes.len() {
            match bytes[idx] {
                b'{' => {
                    if idx + 1 >= bytes.len() {
                        return Err(syn::Error::new(span, "body! has unmatched '{'"));
                    }
                    match bytes[idx + 1] {
                        b'{' => {
                            lit.push(b'{');
                            idx += 2;
                        }
                        b'}' => {
                            if !lit.is_empty() {
                                out.push(Seg::Lit(String::from_utf8(lit).expect("literal utf8")));
                                lit = Vec::new();
                            }
                            out.push(Seg::Hole);
                            idx += 2;
                        }
                        _ => {
                            return Err(syn::Error::new(
                                span,
                                "body! supports only '{}' placeholders",
                            ));
                        }
                    }
                }
                b'}' => {
                    if idx + 1 < bytes.len() && bytes[idx + 1] == b'}' {
                        lit.push(b'}');
                        idx += 2;
                    } else {
                        return Err(syn::Error::new(span, "body! has unmatched '}'"));
                    }
                }
                byte => {
                    lit.push(byte);
                    idx += 1;
                }
            }
        }
        if !lit.is_empty() {
            out.push(Seg::Lit(String::from_utf8(lit).expect("literal utf8")));
        }
        Ok(out)
    }
}
