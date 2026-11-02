use syn::parse;

#[derive(Clone, Copy)]
pub(crate) enum Mode {
    Json,
    Raw,
    Exact,
}

impl parse::Parse for Mode {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        use syn::Ident;
        if input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "#[sark_gen::response] requires an explicit `json`, `raw`, or `exact` mode",
            ));
        }
        let ident = input.parse::<Ident>()?;
        if ident == "json" || ident == "exact" {
            if !input.is_empty() {
                return Err(syn::Error::new_spanned(
                    input.parse::<proc_macro2::TokenStream>()?,
                    "#[sark_gen::response] supports only `json`, `raw`, or `exact`",
                ));
            }
            return Ok(if ident == "json" {
                Self::Json
            } else {
                Self::Exact
            });
        }
        if ident == "raw" {
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
                if !input.is_empty() {
                    return Err(syn::Error::new_spanned(
                        input.parse::<proc_macro2::TokenStream>()?,
                        "#[sark_gen::response] supports only `json`, `raw`, or `exact`",
                    ));
                }
            }
            return Ok(Self::Raw);
        }
        Err(syn::Error::new_spanned(
            ident,
            "#[sark_gen::response] supports only `json`, `raw`, or `exact`",
        ))
    }
}
