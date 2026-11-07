pub(super) mod paths;

use std::env;

use proc_macro_crate::{self, Error, FoundCrate};
use proc_macro2::Span;

#[derive(Clone, Copy)]
pub(crate) enum Requirement {
    Portable,
    Json,
    Http,
    Fiber,
}

pub(crate) enum H1 {
    Available(Option<proc_macro2::TokenStream>),
    Missing,
}

pub(crate) fn require(package: &str, context: &str) -> syn::Result<proc_macro2::TokenStream> {
    match optional(package)? {
        Some(path) => Ok(path),
        None => Err(syn::Error::new(
            Span::call_site(),
            format!("{context} requires {package} as a direct dependency"),
        )),
    }
}

fn optional(package: &str) -> syn::Result<Option<proc_macro2::TokenStream>> {
    match proc_macro_crate::crate_name(package) {
        Ok(FoundCrate::Itself) => own_path(package).map(Some),
        Ok(FoundCrate::Name(name)) => named_path(&name).map(Some),
        Err(Error::CrateNotFound {
            crate_name: reported,
            path,
        }) => {
            if reported == package {
                Ok(None)
            } else {
                Err(syn::Error::new(
                    Span::call_site(),
                    format!(
                        "dependency lookup for `{package}` reported missing `{reported}` in `{}`",
                        path.display(),
                    ),
                ))
            }
        }
        Err(Error::NotFound(path)) => Err(syn::Error::new(
            Span::call_site(),
            format!(
                "cannot inspect `{package}` runtime dependency: no Cargo manifest at `{}`",
                path.display(),
            ),
        )),
        Err(Error::CargoManifestDirNotSet) => Err(syn::Error::new(
            Span::call_site(),
            format!("cannot inspect `{package}` runtime dependency: CARGO_MANIFEST_DIR is absent"),
        )),
        Err(Error::FailedGettingWorkspaceManifestPath) => Err(syn::Error::new(
            Span::call_site(),
            format!(
                "cannot inspect `{package}` runtime dependency: Cargo did not resolve the workspace manifest"
            ),
        )),
        Err(Error::CouldNotRead { path, source }) => Err(syn::Error::new(
            Span::call_site(),
            format!(
                "cannot inspect `{package}` runtime dependency: cannot read `{}`: {source}",
                path.display(),
            ),
        )),
        Err(Error::InvalidToml { source }) => Err(syn::Error::new(
            Span::call_site(),
            format!("cannot inspect `{package}` runtime dependency: invalid manifest: {source}"),
        )),
    }
}

fn own_path(package: &str) -> syn::Result<proc_macro2::TokenStream> {
    let name = package.replace('-', "_");
    match env::var("CARGO_CRATE_NAME") {
        Ok(current) if current == name => Ok(quote::quote!(crate)),
        Ok(_) | Err(env::VarError::NotPresent) => named_path(&name),
        Err(env::VarError::NotUnicode(value)) => Err(syn::Error::new(
            Span::call_site(),
            format!("CARGO_CRATE_NAME is not Unicode: {value:?}"),
        )),
    }
}

fn named_path(name: &str) -> syn::Result<proc_macro2::TokenStream> {
    let ident = syn::parse_str::<proc_macro2::Ident>(name)?;
    Ok(quote::quote!(::#ident))
}
