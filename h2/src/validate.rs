use crate::hpack::OwnedHeader;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum Reason {
    UppercaseName,
    EmptyName,
    BadPseudo,
    PseudoAfterRegular,
    DuplicatePseudo,
    MissingPseudo,
    ResponsePseudoInRequest,
    RequestPseudoInResponse,
    PseudoInTrailers,
    BadConnectionHeader,
    BadTeValue,
    EmptyPath,
    BadScheme,
    BadMethod,
}

pub(super) struct Validate;

impl Validate {
    pub(super) fn request(headers: &[OwnedHeader], trailing: bool) -> Result<(), Reason> {
        let mut saw_regular = false;
        let mut has_method = false;
        let mut has_scheme = false;
        let mut has_path = false;
        let mut method_empty = true;
        let mut scheme_empty = true;
        let mut path_empty = true;

        for h in headers {
            let name = h.name.as_slice();
            let value = h.value.as_slice();
            if name.is_empty() {
                return Err(Reason::EmptyName);
            }
            if name[0] == b':' {
                if saw_regular {
                    return Err(Reason::PseudoAfterRegular);
                }
                if trailing {
                    return Err(Reason::PseudoInTrailers);
                }
                match name {
                    b":method" => {
                        if has_method {
                            return Err(Reason::DuplicatePseudo);
                        }
                        has_method = true;
                        method_empty = value.is_empty();
                    }
                    b":scheme" => {
                        if has_scheme {
                            return Err(Reason::DuplicatePseudo);
                        }
                        has_scheme = true;
                        scheme_empty = value.is_empty();
                    }
                    b":path" => {
                        if has_path {
                            return Err(Reason::DuplicatePseudo);
                        }
                        has_path = true;
                        path_empty = value.is_empty();
                    }
                    b":authority" | b":protocol" => {}
                    b":status" => return Err(Reason::ResponsePseudoInRequest),
                    _ => return Err(Reason::BadPseudo),
                }
            } else {
                Self::check_lowercase(name)?;
                saw_regular = true;
                Self::check_connection_field(name, value)?;
            }
        }

        if trailing {
            return Ok(());
        }

        if !has_method {
            return Err(Reason::MissingPseudo);
        }
        if !has_scheme {
            return Err(Reason::MissingPseudo);
        }
        if !has_path {
            return Err(Reason::MissingPseudo);
        }
        if method_empty {
            return Err(Reason::BadMethod);
        }
        if scheme_empty {
            return Err(Reason::BadScheme);
        }
        if path_empty {
            return Err(Reason::EmptyPath);
        }
        Ok(())
    }

    pub(super) fn response(headers: &[OwnedHeader], trailing: bool) -> Result<(), Reason> {
        let mut saw_regular = false;
        let mut has_status = false;

        for h in headers {
            let name = h.name.as_slice();
            let value = h.value.as_slice();
            if name.is_empty() {
                return Err(Reason::EmptyName);
            }
            if name[0] == b':' {
                if saw_regular {
                    return Err(Reason::PseudoAfterRegular);
                }
                if trailing {
                    return Err(Reason::PseudoInTrailers);
                }
                match name {
                    b":status" => {
                        if has_status {
                            return Err(Reason::DuplicatePseudo);
                        }
                        has_status = true;
                    }
                    b":method" | b":scheme" | b":path" | b":authority" | b":protocol" => {
                        return Err(Reason::RequestPseudoInResponse);
                    }
                    _ => return Err(Reason::BadPseudo),
                }
            } else {
                Self::check_lowercase(name)?;
                saw_regular = true;
                Self::check_connection_field(name, value)?;
            }
        }

        if trailing {
            return Ok(());
        }

        if !has_status {
            return Err(Reason::MissingPseudo);
        }
        Ok(())
    }

    fn check_lowercase(name: &[u8]) -> Result<(), Reason> {
        for &b in name {
            if b.is_ascii_uppercase() {
                return Err(Reason::UppercaseName);
            }
        }
        Ok(())
    }

    fn check_connection_field(name: &[u8], value: &[u8]) -> Result<(), Reason> {
        match name {
            b"connection" | b"keep-alive" | b"proxy-connection" | b"transfer-encoding"
            | b"upgrade" => Err(Reason::BadConnectionHeader),
            b"te" => {
                if value == b"trailers" {
                    Ok(())
                } else {
                    Err(Reason::BadTeValue)
                }
            }
            _ => Ok(()),
        }
    }
}
