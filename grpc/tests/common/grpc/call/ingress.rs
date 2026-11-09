use sark_grpc::protocol::request::Scheme;

pub const SCHEMES: [(Scheme, &[u8]); 2] = [(Scheme::Http, b"http"), (Scheme::Https, b"https")];

pub const INCOMPLETE_RESPONSES: [&[u8]; 2] =
    [&[0, 0, 0, 0, 0, 0], &[0, 0, 0, 0, 0, 0, 0, 0, 0, 2, b'x']];
