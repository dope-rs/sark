use std::collections;

use crate::runtime::paths::Paths;

mod machine;
mod radix;

use crate::app::{route, spec};

/// Canonical route language and generated matcher plan.
pub(in crate::app) struct Plan {
    routes: Vec<Route>,
    machine: machine::Machine,
}

pub(super) struct Route {
    pub(super) method: route::Method,
    protocol: Option<Vec<u8>>,
    path: syn::LitStr,
    pub(super) target: Target,
    params: Vec<Param>,
    pub(super) literal_count: usize,
}

struct Param {
    name: Vec<u8>,
    depth: usize,
}

pub(super) enum Target {
    Path(Vec<route::Seg>),
    Asterisk,
}

enum Relation {
    Disjoint,
    Same,
    Ordered,
    Ambiguous,
}

impl Target {
    pub(super) fn path(&self) -> Option<&[route::Seg]> {
        match self {
            Self::Path(segments) => Some(segments),
            Self::Asterisk => None,
        }
    }

    fn relation(&self, other: &Self) -> Relation {
        let Some(left) = self.path() else {
            return match other.path().is_none() {
                true => Relation::Same,
                false => Relation::Disjoint,
            };
        };
        let Some(right) = other.path() else {
            return Relation::Disjoint;
        };
        if left.len() != right.len()
            || !left
                .iter()
                .zip(right)
                .all(|(left, right)| left.is_overlap(right))
        {
            return Relation::Disjoint;
        }
        let a_in_b = left
            .iter()
            .zip(right)
            .all(|(left, right)| left.is_within(right));
        let b_in_a = right
            .iter()
            .zip(left)
            .all(|(right, left)| right.is_within(left));
        match (a_in_b, b_in_a) {
            (true, true) => Relation::Same,
            (true, false) | (false, true) => Relation::Ordered,
            (false, false) => Relation::Ambiguous,
        }
    }
}

impl Plan {
    pub(in crate::app) fn new(entries: &spec::Entries) -> syn::Result<Self> {
        let routes = entries
            .iter()
            .map(|entry| {
                let raw = entry.path.value();
                let (target, params) = if raw == "*" {
                    if !matches!(
                        entry.method,
                        route::Method::Options | route::Method::Connect
                    ) {
                        return Err(syn::Error::new_spanned(
                            &entry.path,
                            "only OPTIONS or classic CONNECT may use the asterisk request target",
                        ));
                    }
                    (Target::Asterisk, Vec::new())
                } else {
                    let Some(rest) = raw.strip_prefix('/') else {
                        return Err(syn::Error::new_spanned(
                            &entry.path,
                            "an HTTP route path must start with `/`",
                        ));
                    };
                    if raw.as_bytes().contains(&b'?') || raw.as_bytes().contains(&b'#') {
                        return Err(syn::Error::new_spanned(
                            &entry.path,
                            "a route pattern cannot contain a query or fragment",
                        ));
                    }
                    let (segments, params) = route::Seg::parse(&entry.path, rest)?;
                    let params = params
                        .into_iter()
                        .map(|(name, depth)| Param { name, depth })
                        .collect();
                    (Target::Path(segments), params)
                };
                let literals = match target.path() {
                    Some(segments) => segments
                        .iter()
                        .filter(|segment| segment.is_literal())
                        .count(),
                    None => 1,
                };
                Ok(Route {
                    method: entry.method,
                    protocol: entry
                        .protocol
                        .as_ref()
                        .map(|protocol| protocol.value().into_bytes()),
                    path: entry.path.clone(),
                    target,
                    params,
                    literal_count: literals,
                })
            })
            .collect::<syn::Result<Vec<_>>>()?;
        let mut plan = Self {
            routes,
            machine: machine::Machine::default(),
        };
        plan.validate()?;
        plan.machine = machine::Machine::build(&plan);
        Ok(plan)
    }

    pub(in crate::app) fn has_params(&self, index: usize) -> bool {
        !self.route(index).params.is_empty()
    }

    pub(in crate::app) fn capture_type(
        &self,
        paths: &Paths,
        index: usize,
    ) -> proc_macro2::TokenStream {
        let ns_core = &paths.core;
        let fields = self
            .route(index)
            .params
            .iter()
            .map(|_| quote::quote!(#ns_core::route::raw::PathCapture));
        quote::quote!((#(#fields,)*))
    }

    pub(in crate::app) fn check_schema(
        &self,
        paths: &Paths,
        handlers: &spec::Entries,
    ) -> proc_macro2::TokenStream {
        use quote::quote;
        let ns_core = &paths.core;

        let checks = self
            .routes
            .iter()
            .zip(handlers.iter())
            .map(|(route, entry)| {
                let handler = &entry.route;
                let names = route
                    .params
                    .iter()
                    .map(|param| syn::LitByteStr::new(&param.name, route.path.span()));
                quote! {
                    const _: [(); 1] = [();
                        #ns_core::route::raw::PathCapture::is_schema(
                            &[ #( #names, )* ],
                            <<<#handler as #ns_core::route::Spec>::Request
                                as #ns_core::route::Schema>::RawParams
                                as #ns_core::route::RawParams>::PATH_NAMES,
                        ) as ::core::primitive::usize
                    ];
                }
            });
        quote!( #( #checks )* )
    }

    /// Emits the allocation-free matcher for the validated route language.
    pub(in crate::app) fn compile(
        &self,
        paths: &Paths,
        generics: &crate::types::Generics,
        mut leaf: impl FnMut(usize, proc_macro2::TokenStream) -> proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        use quote::quote;
        let ns_core = &paths.core;
        let [v_method, v_target] = generics.locals(["__method", "__target"]);
        let [v_proto] = generics.locals(["__route_protocol"]);
        let [v_caps, v_pend] = generics.locals(["__route_captures", "__path_end"]);
        let [v_start] = generics.locals(["__segment_start"]);
        let [v_nstate, v_rtail] = generics.locals(["__next_state", "__route_tail"]);
        let [v_rstate, v_tail] = generics.locals(["__route_state", "__tail"]);
        let [v_byte] = generics.locals(["__byte"]);

        let machine = &self.machine;
        let depths = self
            .routes
            .iter()
            .flat_map(|route| route.params.iter().map(|param| param.depth))
            .collect::<collections::BTreeSet<_>>();
        let slots = depths
            .iter()
            .copied()
            .enumerate()
            .map(|(slot, depth)| (depth, slot))
            .collect::<collections::BTreeMap<_, _>>();
        let count = slots.len();
        let init = if slots.is_empty() {
            proc_macro2::TokenStream::new()
        } else {
            quote! {
                let mut #v_caps = [
                    ::core::mem::MaybeUninit::<
                        #ns_core::route::raw::PathCapture
                    >::uninit();
                    #count
                ];
            }
        };
        let starts = machine.starts.iter().map(|(method, state)| {
            let method = method.key_token(paths);
            quote!(#method => ::core::option::Option::Some(#state))
        });
        let asterisk = machine
            .asterisk
            .iter()
            .map(|(method, routes)| {
                let method = method.key_token(paths);
                let bodies = routes.iter().map(|&index| {
                    let protocol = self.guard(index, &v_proto);
                    let body = leaf(index, quote!(()));
                    quote! {
                        if #protocol {
                            let #v_pend = 1usize;
                            #body
                        }
                    }
                });
                quote!(#method => { #( #bodies )* })
            })
            .collect::<Vec<_>>();
        let steps = machine.states.iter().enumerate().map(|(id, state)| {
            let mut radix = radix::Radix::default();
            for (literal, next) in &state.literals {
                radix.insert(literal, *next);
            }
            let scan = radix.emit_scan(paths, generics, state.fallback);
            let capture = if state.fallback.is_some() {
                let slot = slots[&state.depth];
                quote! {
                    #v_caps[#slot].write(unsafe {
                        #ns_core::route::raw::PathCapture::new(
                            #v_start,
                            #v_target.len() - #v_rtail.len(),
                        )
                    });
                }
            } else {
                proc_macro2::TokenStream::new()
            };
            quote! {
                #id => {
                    let #v_nstate = { #scan };
                    #capture
                    #v_nstate
                }
            }
        });
        let accepts = machine
            .states
            .iter()
            .enumerate()
            .filter_map(|(id, state)| {
                if state.accept.is_empty() {
                    return None;
                }
                let bodies = state.accept.iter().map(|&index| {
                    let protocol = self.guard(index, &v_proto);
                    let captures = self.captures(index, &slots, &v_caps);
                    let body = leaf(index, captures);
                    quote! {
                        if #protocol {
                            #body
                        }
                    }
                });
                Some(quote!(#id => { #( #bodies )* }))
            })
            .collect::<Vec<_>>();

        let paths = if machine.states.is_empty() {
            proc_macro2::TokenStream::new()
        } else {
            quote! {
                let ::core::option::Option::Some((&b'/', mut #v_rtail)) =
                    #v_target.split_first()
                else {
                    break '__sark_route_match;
                };
                let ::core::option::Option::Some(mut #v_rstate) = (match #v_method {
                    #( #starts, )*
                    _ => ::core::option::Option::None,
                }) else {
                    break '__sark_route_match;
                };
                #init
                let mut #v_pend = 1usize;

                if ::core::matches!(
                    #v_rtail.first(),
                    ::core::option::Option::Some(#v_byte) if *#v_byte != b'?'
                ) {
                    loop {
                        let #v_start = #v_target.len() - #v_rtail.len();
                        let #v_nstate = match #v_rstate {
                            #( #steps, )*
                            _ => ::core::option::Option::None,
                        };
                        let ::core::option::Option::Some(#v_nstate) = #v_nstate else {
                            break '__sark_route_match;
                        };
                        #v_rstate = #v_nstate;
                        #v_pend = #v_target.len() - #v_rtail.len();
                        let ::core::option::Option::Some((&b'/', #v_tail)) =
                            #v_rtail.split_first()
                        else {
                            break;
                        };
                        #v_rtail = #v_tail;
                    }
                }
                match #v_rstate {
                    #( #accepts, )*
                    _ => {}
                }
            }
        };

        quote! {
            '__sark_route_match: {
                if #v_target == b"*" {
                    match #v_method {
                        #( #asterisk, )*
                        _ => {}
                    }
                    break '__sark_route_match;
                }
                #paths
            }
        }
    }

    pub(super) fn route(&self, index: usize) -> &Route {
        &self.routes[index]
    }

    fn guard(&self, index: usize, protocol: &syn::Ident) -> proc_macro2::TokenStream {
        use quote::quote;

        match &self.route(index).protocol {
            Some(value) => {
                let literal = syn::LitByteStr::new(value, self.route(index).path.span());
                quote!(#protocol == ::core::option::Option::Some(#literal))
            }
            None => quote!(#protocol.is_none()),
        }
    }

    fn captures(
        &self,
        index: usize,
        slots: &collections::BTreeMap<usize, usize>,
        captures: &syn::Ident,
    ) -> proc_macro2::TokenStream {
        use quote::quote;

        let captures = self.route(index).params.iter().map(|param| {
            let slot = slots[&param.depth];
            quote!(unsafe { #captures[#slot].assume_init() })
        });
        quote!(( #( #captures, )* ))
    }

    fn validate(&self) -> syn::Result<()> {
        for left in 0..self.routes.len() {
            for right in left + 1..self.routes.len() {
                let a = self.route(left);
                let b = self.route(right);
                if a.method != b.method || a.protocol != b.protocol {
                    continue;
                }
                match a.target.relation(&b.target) {
                    Relation::Same => {
                        return Err(syn::Error::new_spanned(
                            &b.path,
                            format!(
                                "duplicate route language: `{}` matches the same targets as `{}`",
                                b.path.value(),
                                a.path.value(),
                            ),
                        ));
                    }
                    Relation::Ambiguous => {
                        return Err(syn::Error::new_spanned(
                            &b.path,
                            format!(
                                "ambiguous route language overlaps `{}`; make one pattern strictly more specific or make them disjoint",
                                a.path.value(),
                            ),
                        ));
                    }
                    Relation::Disjoint | Relation::Ordered => {}
                }
            }
        }
        Ok(())
    }
}
