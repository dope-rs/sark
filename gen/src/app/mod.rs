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
        assert!(generated.contains("invoke_task"));
        assert!(generated.contains("FiberRoute"));
        assert!(generated.contains("StreamRoute"));
        assert!(generated.contains("state : & 'env"));
        assert!(!generated.contains("__F0001"));
        assert!(!generated.contains("__MK0001"));
        for forbidden in [
            "async move",
            "unsafe",
            "dispatch :: Dispatch",
            "manifold :: Kind",
            "manifold :: ready",
            "OwnerFiber",
            "FiberScope",
            "RequestTask",
            "try_into_task",
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

    #[test]
    fn generated_stream_storage_has_no_task_producer() {
        let input: DefineRouteInput = syn::parse_quote! {
            StreamApp: () => {
                GET "/stream" => stream(capacity = 3) StreamRoute,
            }
        };
        let generated = define_route(input).expect("route generation").to_string();

        assert!(generated.contains("__task_slot_0000"));
        assert!(generated.contains("StreamRoute"));
        for forbidden in ["__F0000", "__MK0000", "task_producers", "try_into_task"] {
            assert!(
                !generated.contains(forbidden),
                "stream app contains fiber-only storage `{forbidden}`",
            );
        }
    }
}
