use syn::ext::IdentExt;

pub(crate) fn marker(handler: &syn::Ident) -> syn::Ident {
    encoded("__SarkHandler", handler)
}

pub(crate) fn route(mut route: syn::TypePath) -> syn::TypePath {
    if let Some(segment) = route.path.segments.last_mut()
        && is_function_name(&segment.ident)
    {
        segment.ident = marker(&segment.ident);
    }
    route
}

pub(crate) fn is_function_name(ident: &syn::Ident) -> bool {
    let source = ident.to_string();
    let visible = match source.strip_prefix("r#") {
        Some(visible) => visible,
        None => &source,
    };
    !visible.starts_with("__Sark")
        && visible
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || *byte == b'_')
}

pub(crate) fn encoded(prefix: &str, handler: &syn::Ident) -> syn::Ident {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let source = handler.unraw().to_string();
    let mut name = String::from(prefix);
    let mut upper = true;
    for ch in source.chars() {
        if ch == '_' {
            upper = true;
        } else if upper {
            name.extend(ch.to_uppercase());
            upper = false;
        } else {
            name.push(ch);
        }
    }
    name.push('X');
    for byte in source.bytes() {
        name.push(char::from(HEX[usize::from(byte >> 4)]));
        name.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    syn::Ident::new(&name, handler.span())
}
