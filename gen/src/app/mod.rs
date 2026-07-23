mod emit;
pub(super) mod plan;
mod spec;

use proc_macro2::TokenStream;
use syn::{LitStr, Result};

use crate::define_route_input::{DefineRouteEntry, DefineRouteInput};
use crate::model::{AppDispatchInput, AppRouteInput};

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
            DefineRouteEntry::Service { method, path, ty } => {
                let full = format!("{prefix}{}", path.value());
                out.push(AppRouteInput {
                    route: ty,
                    method,
                    path: LitStr::new(&full, path.span()),
                    wraps: inherited_wraps.to_vec(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_task_storage_uses_safe_structural_projection() {
        let input: DefineRouteInput = syn::parse_quote! {
            ProjectionApp: () => {
                GET "/sync" => SyncRoute,
                GET "/async" => async(capacity = 2) AsyncRoute,
                GET "/stream" => stream(capacity = 3) StreamRoute,
            }
        };
        let generated = define_route(input).expect("route generation").to_string();

        assert!(generated.contains("__pin_project"));
        assert!(generated.contains("__task_slot_0000"));
        assert!(generated.contains("__task_slot_0001"));
        assert!(generated.contains("try_from_split_task"));
        assert!(generated.contains("RequestTask"));
        assert!(generated.contains("state : & 'env"));
        for forbidden in [
            "async move",
            "unsafe",
            "OwnerFiber",
            "FiberScope",
            "routes :",
            "get_unchecked_mut",
            "into_inner_unchecked",
            "map_unchecked",
            "new_unchecked",
            "unreachable_unchecked",
        ] {
            assert!(
                !generated.contains(forbidden),
                "generated app contains manual projection `{forbidden}`",
            );
        }
    }
}
