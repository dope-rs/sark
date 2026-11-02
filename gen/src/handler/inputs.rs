use syn::{
    FnArg, ItemFn, Type,
    ext::IdentExt,
    visit::{self, Visit},
};

use crate::types::Lifetimes;

pub(super) struct Inputs {
    pub(super) request: Request,
    pub(super) state: State,
    pub(super) timer: Timer,
}

pub(super) enum Request {
    Declared(Type),
    Argument { ty: Type },
}

pub(super) enum State {
    Generic(Type),
    Argument {
        ty: Type,
        lifetime: Option<syn::Lifetime>,
        index: usize,
    },
}

pub(super) enum Timer {
    Absent,
    Present,
}

impl Inputs {
    pub(super) fn validate(sig: &syn::Signature) -> syn::Result<()> {
        let mut reserved = Reserved(None);
        reserved.visit_signature(sig);
        match reserved.0 {
            Some(lifetime) => Err(syn::Error::new_spanned(
                lifetime,
                "handler signature higher-ranked lifetimes cannot be named 'req or 'd; choose another name",
            )),
            None => Ok(()),
        }
    }

    pub(super) fn parse(fun: &mut ItemFn, declared: Option<Type>) -> syn::Result<Self> {
        let task = fun.sig.asyncness.is_some();
        let mut index = 0;
        let request = match declared {
            Some(request) => Request::Declared(Self::schema(request)?),
            None => {
                let argument = Self::typed(fun, index, "request")?;
                index += 1;
                Request::Argument {
                    ty: Self::schema((*argument.ty).clone())?,
                }
            }
        };
        let next = Self::optional(fun, index)?;
        let (state, timer) = match next {
            Some(argument) => match argument.ty.as_ref() {
                Type::Reference(refer) => {
                    index += 1;
                    (
                        State::Argument {
                            ty: (*refer.elem).clone(),
                            lifetime: refer.lifetime.clone(),
                            index: index - 1,
                        },
                        Self::optional(fun, index)?,
                    )
                }
                _ => (
                    State::Generic(syn::parse_quote!(__SarkState)),
                    Some(argument),
                ),
            },
            None => (State::Generic(syn::parse_quote!(__SarkState)), None),
        };
        let timer = match (timer, task) {
            (Some(_), false) => {
                return Err(syn::Error::new_spanned(
                    &fun.sig.inputs,
                    "timer argument is only valid on async handlers",
                ));
            }
            (Some(argument), true) => {
                *argument.ty = Lifetimes::new(&argument.ty).anonymous("d");
                Timer::Present
            }
            (None, _) => Timer::Absent,
        };
        let consumed = match timer {
            Timer::Absent => index,
            Timer::Present => index + 1,
        };
        if consumed != fun.sig.inputs.len() {
            return Err(syn::Error::new_spanned(
                &fun.sig.inputs,
                "handler accepts request, state, and timer at most once",
            ));
        }
        Ok(Self {
            request,
            state,
            timer,
        })
    }

    fn schema(mut request: Type) -> syn::Result<Type> {
        let message = "handler request must be a generated request path without generic arguments";
        let path = match &mut request {
            Type::Path(path) => path,
            other => return Err(syn::Error::new_spanned(other, message)),
        };
        if path.qself.is_some()
            || path
                .path
                .segments
                .iter()
                .any(|segment| !matches!(segment.arguments, syn::PathArguments::None))
        {
            return Err(syn::Error::new_spanned(path, message));
        }
        let Some(segment) = path.path.segments.last_mut() else {
            return Err(syn::Error::new(proc_macro2::Span::call_site(), message));
        };
        segment.arguments = syn::PathArguments::AngleBracketed(syn::parse_quote!(<'static>));
        Ok(request)
    }

    fn typed<'a>(fun: &'a ItemFn, index: usize, role: &str) -> syn::Result<&'a syn::PatType> {
        match fun.sig.inputs.iter().nth(index) {
            Some(FnArg::Typed(argument)) => Ok(argument),
            argument => Err(syn::Error::new_spanned(
                argument,
                format!("handler {role} argument must be typed"),
            )),
        }
    }

    fn optional(fun: &mut ItemFn, index: usize) -> syn::Result<Option<&mut syn::PatType>> {
        match fun.sig.inputs.iter_mut().nth(index) {
            Some(FnArg::Typed(argument)) => Ok(Some(argument)),
            Some(FnArg::Receiver(receiver)) => Err(syn::Error::new_spanned(
                receiver,
                "handler arguments must be typed",
            )),
            None => Ok(None),
        }
    }
}

struct Reserved<'a>(Option<&'a syn::Lifetime>);

impl<'ast> Visit<'ast> for Reserved<'ast> {
    fn visit_bound_lifetimes(&mut self, bound: &'ast syn::BoundLifetimes) {
        self.0 = self.0.or_else(|| {
            bound.lifetimes.iter().find_map(|param| {
                let syn::GenericParam::Lifetime(param) = param else {
                    return None;
                };
                let lifetime = &param.lifetime;
                let name = lifetime.ident.unraw();
                (name == "req" || name == "d").then_some(lifetime)
            })
        });
    }

    fn visit_stmt(&mut self, stmt: &'ast syn::Stmt) {
        if !matches!(stmt, syn::Stmt::Item(_)) {
            visit::visit_stmt(self, stmt);
        }
    }
}
