mod emit;
pub(super) mod plan;
mod spec;

use proc_macro2::TokenStream;
use syn::{LitStr, Result};

use crate::model::{
    AppDispatchInput, AppRouteInput, DefineRouteEntry, DefineRouteInput, RouteKind,
};

pub(super) fn define_route(input: DefineRouteInput) -> Result<TokenStream> {
    let DefineRouteInput {
        vis,
        name,
        state_ty,
        entries,
    } = input;
    let mut routes = Vec::new();
    flatten_entries("", &[], entries, &mut routes)?;
    let mut seen = std::collections::HashSet::new();
    for r in &routes {
        if !seen.insert((r.method.to_string(), r.path.value())) {
            return Err(syn::Error::new_spanned(
                &r.path,
                format!(
                    "duplicate route: `{} {}` is already defined",
                    r.method,
                    r.path.value()
                ),
            ));
        }
    }
    let dispatch = spec::Gen::new(AppDispatchInput {
        vis,
        name,
        state_ty,
        routes,
    })?;
    Ok(emit::render(&dispatch))
}

fn flatten_entries(
    prefix: &str,
    inherited_wraps: &[syn::TypePath],
    entries: Vec<DefineRouteEntry>,
    out: &mut Vec<AppRouteInput>,
) -> Result<()> {
    for entry in entries {
        match entry {
            DefineRouteEntry::Service {
                method,
                path,
                ty,
                kind,
                capacity,
            } => {
                if let Some(cap) = capacity.as_ref()
                    && matches!(kind, RouteKind::Sync)
                {
                    return Err(syn::Error::new_spanned(
                        cap,
                        "`capacity` applies only to `async` or `stream` routes",
                    ));
                }
                let full = format!("{prefix}{}", path.value());
                out.push(AppRouteInput {
                    route: ty,
                    method,
                    path: LitStr::new(&full, path.span()),
                    wraps: inherited_wraps.to_vec(),
                    kind,
                    capacity,
                });
            }
            DefineRouteEntry::Scope {
                prefix: scope_prefix,
                wraps: scope_wraps,
                children,
            } => {
                let new_prefix = format!("{prefix}{}", scope_prefix.value());
                let mut new_wraps = inherited_wraps.to_vec();
                new_wraps.extend(scope_wraps);
                flatten_entries(&new_prefix, &new_wraps, children, out)?;
            }
        }
    }
    Ok(())
}
