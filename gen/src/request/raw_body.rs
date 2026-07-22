use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, Error, FieldsNamed, Ident, Result, Type, Visibility};

use crate::util::TypeExt;

enum BodySource {
    Discarded,
    Raw(RawBodyField),
    Json(Type),
}

struct RawBodyField {
    ident: Ident,
    borrowed_ty: Type,
}

impl RawBodyField {
    fn new(ident: Ident, ty: &Type) -> Result<Self> {
        let supported = ty.is_bytes_with_storage("Retained")
            || matches!(ty, Type::Path(path) if path.path.segments.last().is_some_and(|seg| seg.ident == "Shared"));
        if !supported {
            return Err(Error::new_spanned(
                ty,
                "#[raw_body] field type must be Bytes<Retained> or Shared",
            ));
        }
        Ok(Self {
            ident,
            borrowed_ty: syn::parse_quote!(::o3::buffer::Bytes<::o3::buffer::Borrowed<'req>>),
        })
    }
}

pub(super) struct BodyPlan {
    source: BodySource,
    length: Option<Ident>,
}

impl BodyPlan {
    pub(super) fn from_attrs(attrs: &mut Vec<Attribute>) -> Result<Self> {
        let mut json = None;
        let mut kept = Vec::with_capacity(attrs.len());
        for attr in attrs.drain(..) {
            if attr.path().is_ident("json_body") {
                if json.is_some() {
                    return Err(Error::new_spanned(attr, "duplicate #[json_body(...)]"));
                }
                json = Some(attr.parse_args::<Type>()?);
            } else {
                kept.push(attr);
            }
        }
        *attrs = kept;
        Ok(Self {
            source: match json {
                Some(ty) => BodySource::Json(ty),
                None => BodySource::Discarded,
            },
            length: None,
        })
    }

    pub(super) fn register_raw(&mut self, ident: Ident, ty: &Type) -> Result<()> {
        match &self.source {
            BodySource::Raw(_) => {
                return Err(Error::new_spanned(ty, "duplicate #[raw_body]"));
            }
            BodySource::Json(_) => {
                return Err(Error::new_spanned(
                    ty,
                    "#[json_body(...)] and #[raw_body] are mutually exclusive",
                ));
            }
            BodySource::Discarded => {}
        }
        let field = RawBodyField::new(ident, ty)?;
        self.source = BodySource::Raw(field);
        Ok(())
    }

    pub(super) fn register_length(&mut self, ident: Ident, ty: &Type) -> Result<()> {
        if self.length.is_some() {
            return Err(Error::new_spanned(ty, "duplicate #[body_len]"));
        }
        if !ty.is_plain_ident("BodyLen") {
            return Err(Error::new_spanned(
                ty,
                "#[body_len] field type must be sark::request::BodyLen",
            ));
        }
        self.length = Some(ident);
        Ok(())
    }

    pub(super) fn append_json_field(&self, named: &mut FieldsNamed, vis: &Visibility) {
        let BodySource::Json(ty) = &self.source else {
            return;
        };
        named.named.push(syn::parse_quote!(#vis body: #ty));
    }

    pub(super) fn constructor_fields(&self) -> Vec<TokenStream> {
        let mut fields = Vec::with_capacity(2);
        match &self.source {
            BodySource::Raw(raw) => {
                let ident = &raw.ident;
                fields.push(quote!(#ident: req.body_frame()));
            }
            BodySource::Json(_) => fields.push(quote!(body)),
            BodySource::Discarded => {}
        }
        if let Some(ident) = &self.length {
            fields.push(
                quote!(#ident: ::sark::request::BodyLen::from_declared(req.declared_body_len())),
            );
        }
        fields
    }

    pub(super) fn rewrite_raw_field(&self, field: &mut syn::Field) {
        let BodySource::Raw(raw) = &self.source else {
            return;
        };
        if field.ident.as_ref() == Some(&raw.ident) {
            field.ty = raw.borrowed_ty.clone();
        }
    }

    pub(super) fn parsed_body_impl(&self) -> TokenStream {
        match &self.source {
            BodySource::Json(ty) => quote! {
                type ParsedBody<'req> = #ty;

                fn parse_body<'req>(raw: &'req [u8]) -> sark::error::Result<Self::ParsedBody<'req>> {
                    <#ty as sark::json::JsonDecode>::decode_json_borrowed(raw)
                }
            },
            BodySource::Discarded | BodySource::Raw(_) => quote! {
                type ParsedBody<'req> = ();

                fn parse_body<'req>(raw: &'req [u8]) -> sark::error::Result<Self::ParsedBody<'req>> {
                    let _ = raw;
                    Ok(())
                }
            },
        }
    }

    pub(super) fn parsed_body_param_ty(&self) -> TokenStream {
        match &self.source {
            BodySource::Json(ty) => quote!(#ty),
            BodySource::Discarded | BodySource::Raw(_) => quote!(()),
        }
    }

    pub(super) fn parsed_body_bind(&self) -> TokenStream {
        match self.source {
            BodySource::Json(_) => quote!(let body = parsed_body;),
            BodySource::Discarded | BodySource::Raw(_) => TokenStream::new(),
        }
    }

    pub(super) fn policy(&self) -> TokenStream {
        match self.source {
            BodySource::Raw(_) | BodySource::Json(_) => {
                quote!(sark::service::BodyPolicy::Buffered)
            }
            BodySource::Discarded => quote!(sark::service::BodyPolicy::Discarded),
        }
    }
}
