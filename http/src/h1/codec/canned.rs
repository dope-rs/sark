//! Canonical static HTTP error responses shared by execution adapters.
use crate::payload::fixed;

pub const BAD_REQUEST: fixed::Body = fixed::Body::new(
    b"HTTP/1.1 400 Bad Request\r\nServer: sark\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
);
pub const NOT_FOUND: fixed::Body = fixed::Body::new(
    b"HTTP/1.1 404 Not Found\r\nServer: sark\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
);
pub const REQUEST_TIMEOUT: fixed::Body = fixed::Body::new(
    b"HTTP/1.1 408 Request Timeout\r\nServer: sark\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
);
pub const PAYLOAD_TOO_LARGE: fixed::Body = fixed::Body::new(
    b"HTTP/1.1 413 Payload Too Large\r\nServer: sark\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
);
pub const INTERNAL_SERVER_ERROR: fixed::Body = fixed::Body::new(
    b"HTTP/1.1 500 Internal Server Error\r\nServer: sark\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
);
pub const NOT_IMPLEMENTED: fixed::Body = fixed::Body::new(
    b"HTTP/1.1 501 Not Implemented\r\nServer: sark\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
);
pub const SERVICE_UNAVAILABLE: fixed::Body = fixed::Body::new(
    b"HTTP/1.1 503 Service Unavailable\r\nServer: sark\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
);
