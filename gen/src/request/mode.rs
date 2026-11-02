use syn::{parse, punctuated};

pub(crate) struct Mode {
    pub(super) ordered: bool,
    pub(super) full: bool,
}

impl parse::Parse for Mode {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        use syn::Error;

        let flags = punctuated::Punctuated::<syn::Ident, syn::Token![,]>::parse_terminated(input)?;
        for flag in &flags {
            if flag != "ordered" && flag != "full" {
                return Err(Error::new_spanned(
                    flag,
                    "#[sark_gen::request] supports only `ordered` and `full`",
                ));
            }
        }
        Ok(Self {
            ordered: flags.iter().any(|flag| flag == "ordered"),
            full: flags.iter().any(|flag| flag == "full"),
        })
    }
}
