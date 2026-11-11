use syn::{parse, token};

use crate::macros::tokens::Word;

pub(crate) struct Input {
    pub(super) vis: syn::Visibility,
    pub(super) name: syn::Ident,
    pub(super) generics: syn::Generics,
    pub(super) state_ty: Option<syn::Type>,
    pub(super) entries: Vec<Entry>,
}

pub(super) enum Entry {
    Service(Box<Service>),
    Scope {
        prefix: syn::LitStr,
        guards: Vec<syn::TypePath>,
        children: Vec<Entry>,
    },
}

pub(super) struct Service {
    pub(super) method: syn::Ident,
    pub(super) protocol: Option<syn::LitStr>,
    pub(super) path: syn::LitStr,
    pub(super) request: Request,
    pub(super) route: Route,
}

#[derive(Clone, Copy, Default)]
pub(super) struct Request {
    pub(super) path: bool,
    pub(super) header: bool,
    pub(super) query: bool,
}

impl Request {
    pub(super) const fn any(self) -> bool {
        self.path || self.header || self.query
    }

    fn insert(&mut self, field: syn::Ident) -> syn::Result<()> {
        let present = if field == "path" {
            &mut self.path
        } else if field == "header" {
            &mut self.header
        } else if field == "query" {
            &mut self.query
        } else {
            return Err(syn::Error::new(
                field.span(),
                "expected `path`, `header`, or `query`",
            ));
        };
        if *present {
            return Err(syn::Error::new(
                field.span(),
                "duplicate request field kind",
            ));
        }
        *present = true;
        Ok(())
    }
}

pub(super) struct Route {
    pub(super) ty: syn::TypePath,
    pub(super) kind: Kind,
}

pub(super) enum Kind {
    Sync,
    Async {
        capacity: Option<syn::Expr>,
        body: Body,
        stream: bool,
    },
    Stream {
        capacity: Option<syn::Expr>,
    },
}

pub(super) enum Body {
    Discarded,
    Json,
    Segmented,
    Contiguous,
}

impl Body {
    pub(super) const fn as_str(&self) -> &str {
        match self {
            Self::Discarded => "discarded",
            Self::Json => "json",
            Self::Segmented => "segmented",
            Self::Contiguous => "contiguous",
        }
    }
}

impl parse::Parse for Input {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        let vis = input.parse::<syn::Visibility>()?;
        let name = input.parse::<syn::Ident>()?;
        let mut generics = input.parse::<syn::Generics>()?;
        let state_ty = if input.peek(syn::Token![:]) {
            input.parse::<syn::Token![:]>()?;
            Some(input.parse()?)
        } else {
            None
        };
        generics.where_clause = input.parse()?;
        input.parse::<syn::Token![=>]>()?;
        let body;
        syn::braced!(body in input);
        let entries = parse_entries(&body)?;
        Ok(Self {
            vis,
            name,
            generics,
            state_ty,
            entries,
        })
    }
}

fn parse_entries(input: parse::ParseStream<'_>) -> syn::Result<Vec<Entry>> {
    parse_list(input, parse_entry)
}

fn parse_list<T>(
    input: parse::ParseStream<'_>,
    item: impl for<'a> Fn(parse::ParseStream<'a>) -> syn::Result<T>,
) -> syn::Result<Vec<T>> {
    let mut out = Vec::new();
    if input.is_empty() {
        return Ok(out);
    }
    out.push(item(input)?);
    while !input.is_empty() {
        input.parse::<syn::Token![,]>()?;
        if input.is_empty() {
            break;
        }
        out.push(item(input)?);
    }
    Ok(out)
}

fn parse_entry(input: parse::ParseStream<'_>) -> syn::Result<Entry> {
    match Word::Scope.try_take(input)? {
        Some(scope) => scope.parse_with(input, |input| {
            use syn::token::{Brace, Bracket};
            let prefix = input.parse::<syn::LitStr>()?;
            let guards = match Word::Guard.try_take(input)? {
                Some(guard) => guard.parse_with(input, |input| {
                    use syn::punctuated::Punctuated;
                    let args;
                    syn::parenthesized!(args in input);
                    Ok(
                        Punctuated::<syn::TypePath, syn::Token![,]>::parse_terminated(&args)?
                            .into_iter()
                            .collect(),
                    )
                })?,
                None => Vec::new(),
            };
            input.parse::<syn::Token![=>]>()?;
            let children = if input.peek(Bracket) {
                let body;
                syn::bracketed!(body in input);
                parse_list(&body, parse_service)?
            } else if input.peek(Brace) {
                let body;
                syn::braced!(body in input);
                parse_entries(&body)?
            } else {
                return Err(input.error("expected `[ ... ]` or `{ ... }` after scope `=>`"));
            };
            Ok(Entry::Scope {
                prefix,
                guards,
                children,
            })
        }),
        None => parse_service(input),
    }
}

fn parse_service(input: parse::ParseStream<'_>) -> syn::Result<Entry> {
    let (method, protocol) = parse_method(input)?;
    let path = input.parse::<syn::LitStr>()?;
    input.parse::<syn::Token![=>]>()?;
    let request = parse_request(input)?;
    let route = input.parse()?;
    Ok(Entry::Service(Box::new(Service {
        method,
        protocol,
        path,
        request,
        route,
    })))
}

fn parse_method(input: parse::ParseStream<'_>) -> syn::Result<(syn::Ident, Option<syn::LitStr>)> {
    let method = input.parse::<syn::Ident>()?;
    let protocol = if input.peek(token::Paren) {
        let content;
        syn::parenthesized!(content in input);
        let protocol = content.parse::<syn::LitStr>()?;
        if !content.is_empty() {
            return Err(content.error("expected exactly one protocol string"));
        }
        Some(protocol)
    } else {
        None
    };
    Ok((method, protocol))
}

fn parse_request(input: parse::ParseStream<'_>) -> syn::Result<Request> {
    let Some(request) = Word::Request.try_call(input)? else {
        return Ok(Request::default());
    };
    request.parse_with(input, |input| {
        use syn::punctuated::Punctuated;

        let options;
        syn::parenthesized!(options in input);
        let fields = Punctuated::<syn::Ident, syn::Token![,]>::parse_terminated(&options)?;
        if fields.is_empty() {
            return Err(options.error("request requires at least one field kind"));
        }
        let mut selected = Request::default();
        for field in fields {
            selected.insert(field)?;
        }
        match Word::Request.try_call(input)? {
            Some(again) => Err(again.error("duplicate `request(...)`")),
            None => Ok(selected),
        }
    })
}

impl parse::Parse for Route {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        use syn::token::Paren;
        let (kind, ty) = match Word::Async.try_take(input)? {
            Some(keyword) => keyword.parse_with(input, |input| {
                let stream = match Word::Stream.try_take(input)? {
                    Some(word) => word.parse_with(input, |_| Ok(true))?,
                    None => false,
                };
                if !input.peek(Paren) {
                    return Err(
                        input.error("async routes require `(capacity = EXPR, body = KIND)`")
                    );
                }
                let options;
                syn::parenthesized!(options in input);
                let capacity = parse_capacity(&options)?;
                options.parse::<syn::Token![,]>()?;
                let key: syn::Ident = options.parse()?;
                if key == "capacity" {
                    return Err(syn::Error::new(key.span(), "duplicate `capacity`"));
                }
                if key != "body" {
                    return Err(syn::Error::new(key.span(), "expected `body`"));
                }
                options.parse::<syn::Token![=]>()?;
                let body = options.parse()?;
                finish_options(&options, true)?;
                Ok((
                    Kind::Async {
                        capacity,
                        body,
                        stream,
                    },
                    input.parse()?,
                ))
            })?,
            None => match Word::Stream.try_take(input)? {
                Some(keyword) => keyword.parse_with(input, |input| {
                    if !input.peek(Paren) {
                        return Err(input.error("stream routes require `(capacity = EXPR)`"));
                    }
                    let options;
                    syn::parenthesized!(options in input);
                    let capacity = parse_capacity(&options)?;
                    finish_options(&options, false)?;
                    Ok((Kind::Stream { capacity }, input.parse()?))
                })?,
                None => (Kind::Sync, input.parse()?),
            },
        };
        Ok(Self { ty, kind })
    }
}

impl parse::Parse for Body {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        let kind = input.parse::<syn::Ident>()?;
        let value = kind.to_string();
        if value == "discarded" {
            Ok(Self::Discarded)
        } else if value == "json" {
            Ok(Self::Json)
        } else if value == "segmented" {
            Ok(Self::Segmented)
        } else if value == "contiguous" {
            Ok(Self::Contiguous)
        } else {
            Err(syn::Error::new(
                kind.span(),
                "expected `discarded`, `json`, `segmented`, or `contiguous`",
            ))
        }
    }
}

fn parse_capacity(input: parse::ParseStream<'_>) -> syn::Result<Option<syn::Expr>> {
    let key = input.parse::<syn::Ident>()?;
    if key == "body" {
        return Err(syn::Error::new(
            key.span(),
            "`body` is only valid after async route `capacity`",
        ));
    }
    if key != "capacity" {
        return Err(syn::Error::new(key.span(), "expected `capacity`"));
    }
    input.parse::<syn::Token![=]>()?;
    let capacity = input.parse::<syn::Expr>()?;
    Ok(match capacity {
        syn::Expr::Path(path) if path.qself.is_none() && path.path.is_ident("global") => None,
        capacity => Some(capacity),
    })
}

fn finish_options(input: parse::ParseStream<'_>, is_async: bool) -> syn::Result<()> {
    if input.is_empty() {
        return Ok(());
    }
    input.parse::<syn::Token![,]>()?;
    let key = input.parse::<syn::Ident>()?;
    let message = if key == "capacity" {
        "duplicate `capacity`"
    } else if key == "body" && is_async {
        "duplicate `body`"
    } else if key == "body" {
        "`body` is only valid on async routes"
    } else {
        "unknown route option"
    };
    Err(syn::Error::new(key.span(), message))
}
