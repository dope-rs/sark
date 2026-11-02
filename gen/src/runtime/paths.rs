use std::cell::Cell;

use quote::ToTokens;

use crate::runtime;

/// Admitted generator-owned paths; user syntax is never rewritten.
pub(crate) struct Paths {
    pub(crate) core: Path,
    pub(crate) http: Path,
    pub(crate) json: Path,
    pub(crate) fiber: Path,
    pub(crate) h1: Path,
}

pub(crate) enum Library {
    Protocol,
    Http,
    Json,
    Fiber,
    H1,
}

impl Library {
    fn name(&self) -> &str {
        match self {
            Self::Protocol => "sark or sark-fiber",
            Self::Http => "sark, sark-fiber, or sark-json",
            Self::Json => "sark-json",
            Self::Fiber => "sark-fiber",
            Self::H1 => "sark-h1",
        }
    }
}

pub(crate) enum Path {
    Found(proc_macro2::TokenStream),
    Missing { package: Library, used: Cell<bool> },
}

impl Path {
    fn dependency(package: Library, path: runtime::Path) -> Self {
        match path {
            runtime::Path::Found(path) => Self::Found(path),
            runtime::Path::Missing => Self::Missing {
                package,
                used: Cell::new(false),
            },
        }
    }

    pub(crate) fn require(&self) -> syn::Result<()> {
        match self {
            Self::Found(_) => Ok(()),
            Self::Missing { package, .. } => Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!(
                    "generated code requires `{}` as a direct dependency",
                    package.name()
                ),
            )),
        }
    }
}

impl ToTokens for Path {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        match self {
            Self::Found(path) => path.to_tokens(tokens),
            Self::Missing { used, .. } => {
                used.set(true);
                quote::quote!(::__sark_missing_dependency).to_tokens(tokens);
            }
        }
    }
}

impl Paths {
    pub(crate) fn resolve(need: runtime::Requirement) -> syn::Result<Self> {
        let facade = runtime::optional("sark")?;
        let fiber = runtime::optional("sark-fiber")?;
        let json = runtime::optional("sark-json")?;
        let h1 = runtime::optional("sark-h1")?;
        let (core, http) = match &facade {
            runtime::Path::Found(root) => (
                Path::Found(quote::quote!(#root::core)),
                Path::Found(quote::quote!(#root::http)),
            ),
            runtime::Path::Missing => match &fiber {
                runtime::Path::Found(root) => (
                    Path::Found(quote::quote!(#root::sark_core)),
                    Path::Found(quote::quote!(#root::sark_http)),
                ),
                runtime::Path::Missing => (
                    Path::dependency(Library::Protocol, runtime::Path::Missing),
                    match &json {
                        runtime::Path::Found(root) => Path::Found(quote::quote!(#root::sark_http)),
                        runtime::Path::Missing => {
                            Path::dependency(Library::Http, runtime::Path::Missing)
                        }
                    },
                ),
            },
        };
        let paths = Self {
            core,
            http,
            fiber: Path::dependency(Library::Fiber, fiber),
            json: Path::dependency(Library::Json, json),
            h1: Path::dependency(Library::H1, h1),
        };
        match need {
            runtime::Requirement::Portable => paths.core.require()?,
            runtime::Requirement::Fiber => paths.fiber.require()?,
            runtime::Requirement::Json => paths.json.require()?,
            runtime::Requirement::Http => paths.http.require()?,
        }
        Ok(paths)
    }

    pub(crate) fn finish(
        &self,
        tokens: proc_macro2::TokenStream,
    ) -> syn::Result<proc_macro2::TokenStream> {
        for path in [&self.core, &self.http, &self.json, &self.fiber, &self.h1] {
            if let Path::Missing { used, .. } = path
                && used.get()
            {
                path.require()?;
            }
        }
        Ok(tokens)
    }

    pub(crate) fn h1(&self) -> runtime::H1 {
        match self.h1 {
            Path::Found(_) => runtime::H1::Available,
            Path::Missing { .. } => runtime::H1::Missing,
        }
    }
}
