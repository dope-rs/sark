use crate::route_compiler::Method;

impl Method {
    pub(super) fn parse(value: &str) -> Option<Self> {
        match value {
            "GET" => Some(Self::Get),
            "POST" => Some(Self::Post),
            "PUT" => Some(Self::Put),
            "PATCH" => Some(Self::Patch),
            "DELETE" => Some(Self::Delete),
            "HEAD" => Some(Self::Head),
            "OPTIONS" => Some(Self::Options),
            "WS" => Some(Self::Get),
            _ => None,
        }
    }
}

pub(super) struct Meta {
    pub(super) method: Method,
}
