use std::num::NonZero;

pub(super) struct Field {
    name: syn::Ident,
    tag: NonZero<u32>,
}

impl Field {
    pub(super) fn new(field: &syn::Field) -> syn::Result<Self> {
        let name = field
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new_spanned(field, "Message requires named fields"))?;
        let mut tag = None;
        let mut bytes = None;
        for attr in field
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("prost"))
        {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("bytes") {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    if value.value() != "vec" || bytes.replace(()).is_some() {
                        return Err(meta.error("expected bytes = \"vec\" exactly once"));
                    }
                } else if meta.path.is_ident("tag") {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    let value = value
                        .value()
                        .parse::<u32>()
                        .map_err(|error| syn::Error::new_spanned(value, error))?;
                    let value = NonZero::new(value)
                        .filter(|value| {
                            value.get() < 1 << 29 && !(19000..20000).contains(&value.get())
                        })
                        .ok_or_else(|| meta.error("invalid protobuf field tag"))?;
                    if tag.replace(value).is_some() {
                        return Err(meta.error("duplicate protobuf field tag"));
                    }
                } else {
                    return Err(meta.error("Message requires a bytes field"));
                }
                Ok(())
            })?;
        }
        if bytes.is_none() {
            return Err(syn::Error::new_spanned(
                field,
                "expected #[prost(bytes = \"vec\", tag = \"...\")]",
            ));
        }
        let tag =
            tag.ok_or_else(|| syn::Error::new_spanned(field, "missing protobuf field tag"))?;
        Ok(Self { name, tag })
    }

    pub(super) fn tag(&self) -> NonZero<u32> {
        self.tag
    }

    pub(super) fn name(&self) -> &syn::Ident {
        &self.name
    }
}
