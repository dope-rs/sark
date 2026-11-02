use syn::parse;

#[derive(Default)]
pub(crate) struct Args {
    pub(super) request: Option<syn::Type>,
    pub(super) vis: Option<syn::Visibility>,
}

impl parse::Parse for Args {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        let vis = if input.peek(syn::Token![pub]) {
            let vis = Some(input.parse()?);
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
            vis
        } else {
            None
        };
        if input.is_empty() {
            return Ok(Self { request: None, vis });
        }
        let name: syn::Ident = input.parse()?;
        if name != "request" {
            return Err(syn::Error::new(name.span(), "expected `request = Type`"));
        }
        input.parse::<syn::Token![=]>()?;
        let request = input.parse()?;
        if !input.is_empty() {
            return Err(input.error("unexpected handler argument"));
        }
        Ok(Self {
            request: Some(request),
            vis,
        })
    }
}
