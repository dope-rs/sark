use proc_macro2::{Literal, Span, TokenStream};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Expr, LitStr, Result, Token};

pub(super) struct BodyInput {
    segments: Vec<Segment>,
    args: Vec<Expr>,
}

impl BodyInput {
    pub(super) fn expand(self) -> TokenStream {
        let values: Vec<_> = (0..self.args.len())
            .map(|idx| format_ident!("__body_arg_{idx}"))
            .collect();
        let bytes: Vec<_> = (0..self.args.len())
            .map(|idx| format_ident!("__body_arg_{idx}_bytes"))
            .collect();
        let binds =
            values
                .iter()
                .zip(bytes.iter())
                .zip(self.args.iter())
                .map(|((value, raw), expr)| {
                    quote! {
                        let #value = (#expr);
                        let #raw: &[u8] = ::core::convert::AsRef::<[u8]>::as_ref(&#value);
                    }
                });

        let mut capacities = Vec::with_capacity(self.segments.len());
        let mut writes = Vec::with_capacity(self.segments.len());
        let mut hole = 0usize;
        for segment in &self.segments {
            match segment {
                Segment::Literal(text) => {
                    let len = text.len();
                    let literal = Literal::byte_string(text);
                    capacities.push(quote!(#len));
                    writes.push(quote!(__body_buf.extend_from_slice(#literal);));
                }
                Segment::Hole => {
                    let arg = &bytes[hole];
                    hole += 1;
                    capacities.push(quote!(#arg.len()));
                    writes.push(quote!(__body_buf.extend_from_slice(#arg);));
                }
            }
        }

        quote!({
            #(#binds)*
            let __body_cap = 0usize #(+ #capacities)*;
            let mut __body_buf = Vec::with_capacity(__body_cap);
            #(#writes)*
            __body_buf
        })
    }
}

impl Parse for BodyInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let format = input.parse::<LitStr>()?;
        let args = if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            Punctuated::<Expr, Token![,]>::parse_terminated(input)?
                .into_iter()
                .collect()
        } else {
            Vec::new()
        };
        let segments = Segment::split(&format.value(), format.span())?;
        let holes = segments
            .iter()
            .filter(|segment| matches!(segment, Segment::Hole))
            .count();
        if holes != args.len() {
            return Err(syn::Error::new(
                format.span(),
                format!(
                    "body! placeholder count mismatch: expected {holes}, got {}",
                    args.len()
                ),
            ));
        }
        Ok(Self { segments, args })
    }
}

enum Segment {
    Literal(Vec<u8>),
    Hole,
}

impl Segment {
    fn split(value: &str, span: Span) -> Result<Vec<Self>> {
        let bytes = value.as_bytes();
        let mut segments = Vec::new();
        let mut literal = Vec::new();
        let mut idx = 0usize;
        while idx < bytes.len() {
            match bytes[idx] {
                b'{' => {
                    if idx + 1 >= bytes.len() {
                        return Err(syn::Error::new(span, "body! has unmatched '{'"));
                    }
                    match bytes[idx + 1] {
                        b'{' => {
                            literal.push(b'{');
                            idx += 2;
                        }
                        b'}' => {
                            if !literal.is_empty() {
                                segments.push(Self::Literal(literal));
                                literal = Vec::new();
                            }
                            segments.push(Self::Hole);
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
                        literal.push(b'}');
                        idx += 2;
                    } else {
                        return Err(syn::Error::new(span, "body! has unmatched '}'"));
                    }
                }
                byte => {
                    literal.push(byte);
                    idx += 1;
                }
            }
        }
        if !literal.is_empty() {
            segments.push(Self::Literal(literal));
        }
        Ok(segments)
    }
}
