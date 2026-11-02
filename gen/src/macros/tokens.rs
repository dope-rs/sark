use syn::{ext::IdentExt as _, parse, parse::discouraged::Speculative as _, token::Paren};

pub(crate) fn local(name: &str) -> syn::Ident {
    syn::Ident::new(name, proc_macro2::Span::mixed_site())
}

pub(crate) struct Taken<'a> {
    next: parse::ParseBuffer<'a>,
    span: proc_macro2::Span,
}

pub(crate) enum Word {
    Async,
    Guard,
    Request,
    Scope,
    Stream,
}

impl Word {
    pub(crate) fn try_take<'a>(
        self,
        input: parse::ParseStream<'a>,
    ) -> syn::Result<Option<Taken<'a>>> {
        let expected = self.as_str();
        if !input.peek(syn::Ident::peek_any) {
            return Ok(None);
        }
        let next = input.fork();
        let ident = next.call(syn::Ident::parse_any)?;
        if ident != expected {
            return Ok(None);
        }
        Ok(Some(Taken {
            next,
            span: ident.span(),
        }))
    }

    pub(crate) fn try_call<'a>(
        self,
        input: parse::ParseStream<'a>,
    ) -> syn::Result<Option<Taken<'a>>> {
        Ok(self.try_take(input)?.and_then(Taken::into_call))
    }

    const fn as_str(&self) -> &str {
        match self {
            Self::Async => "async",
            Self::Guard => "guard",
            Self::Request => "request",
            Self::Scope => "scope",
            Self::Stream => "stream",
        }
    }
}

impl<'a> Taken<'a> {
    pub(crate) fn parse_with<T>(
        self,
        input: parse::ParseStream<'a>,
        parser: impl for<'b> FnOnce(parse::ParseStream<'b>) -> syn::Result<T>,
    ) -> syn::Result<T> {
        let value = parser(&self.next)?;
        input.advance_to(&self.next);
        Ok(value)
    }

    pub(crate) fn error(self, message: &str) -> syn::Error {
        syn::Error::new(self.span, message)
    }

    fn into_call(self) -> Option<Self> {
        if self.next.peek(Paren) {
            Some(self)
        } else {
            None
        }
    }
}
