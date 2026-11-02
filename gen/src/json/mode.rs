use syn::{parse, punctuated};

#[derive(Clone, Copy)]
pub(super) enum JsonKind {
    Unordered,
    Ordered,
}

#[derive(Clone, Copy)]
pub(super) enum Purpose {
    Decode,
    Encode,
    TryEncode,
}

pub(crate) struct Mode {
    pub(super) kind: JsonKind,
    pub(super) exact: bool,
    pub(super) plain: bool,
    pub(super) purpose: Purpose,
}

impl parse::Parse for Mode {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        use syn::Error;

        let flags = punctuated::Punctuated::<syn::Ident, syn::Token![,]>::parse_terminated(input)?;
        for flag in &flags {
            if flag != "ordered"
                && flag != "unordered"
                && flag != "exact"
                && flag != "plain"
                && flag != "encode"
                && flag != "try_encode"
            {
                return Err(Error::new_spanned(
                    flag,
                    "#[sark_gen::json] supports only `ordered`, `unordered`, `exact`, `plain`, `encode`, or `try_encode`",
                ));
            }
        }
        let kind = if flags
            .iter()
            .rev()
            .find(|flag| *flag == "ordered" || *flag == "unordered")
            .is_some_and(|flag| flag == "ordered")
        {
            JsonKind::Ordered
        } else {
            JsonKind::Unordered
        };
        let exact = flags.iter().find(|flag| *flag == "exact");
        if let Some(flag) = exact
            && !matches!(kind, JsonKind::Ordered)
        {
            return Err(Error::new_spanned(flag, "`exact` requires `ordered`"));
        }
        let mut modes = flags
            .iter()
            .filter(|flag| *flag == "encode" || *flag == "try_encode");
        let purpose = if let Some(flag) = modes.next() {
            if flag == "encode" {
                Purpose::Encode
            } else {
                Purpose::TryEncode
            }
        } else {
            Purpose::Decode
        };
        if let Some(flag) = modes.next() {
            return Err(Error::new_spanned(flag, "encoding mode specified twice"));
        }
        if let Some(flag) = exact
            && !matches!(purpose, Purpose::Decode)
        {
            return Err(Error::new_spanned(
                flag,
                "encoding-only modes cannot be combined with `exact`",
            ));
        }
        Ok(Self {
            kind,
            exact: exact.is_some(),
            plain: flags.iter().any(|flag| flag == "plain"),
            purpose,
        })
    }
}
