use syn::parse;

use crate::runtime::paths::Paths;

pub(crate) struct Body {
    driver: syn::Lifetime,
    body: syn::ExprAsync,
}

impl parse::Parse for Body {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        let driver = input.parse()?;
        input.parse::<syn::Token![=>]>()?;
        let body = input.parse()?;
        Ok(Self { driver, body })
    }
}

impl Body {
    pub(crate) fn expand(self, paths: &Paths) -> proc_macro2::TokenStream {
        let ns_fiber = &paths.fiber;
        let Self { driver, body } = self;
        quote::quote! {
            #ns_fiber::dope_gen::fiber!(#driver, crate = #ns_fiber => #body)
        }
    }
}
