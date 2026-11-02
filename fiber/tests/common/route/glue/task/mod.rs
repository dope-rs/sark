pub mod generics;

use std::cell::Cell;

use dope_test::fibers::Yield;
use o3::buffer::write::{Sink, consume};
use sark_http::payload::response::{Encode, exact, status};

pub struct State<'a, 'b> {
    pub first: &'a [u8],
    pub second: &'b [u8],
    pub encoded: &'b Cell<usize>,
}

pub struct Body<'a, 'b> {
    first: &'a [u8],
    second: &'b [u8],
    encoded: &'b Cell<usize>,
}

impl exact::Body for Body<'_, '_> {
    fn encoded_len(&self) -> usize {
        self.first.len() + self.second.len()
    }
}

impl Encode for Body<'_, '_> {
    fn encode<W: Sink<Error = consume::Error>>(self, out: &mut W) -> Result<(), consume::Error> {
        out.write_slices([self.first, self.second])?;
        self.encoded.set(self.encoded.get() + 1);
        Ok(())
    }
}

#[sark::request]
pub struct ModeRequest {
    #[query("mode", default = "0")]
    mode: usize,
}

type Response<'a, 'b> = exact::Direct<(), Body<'a, 'b>>;
type Outcome<'a, 'b> = Result<Response<'a, 'b>, Response<'a, 'b>>;

struct Rejected<'a, 'b>(Body<'a, 'b>);

impl<'a, 'b> From<Rejected<'a, 'b>> for Response<'a, 'b> {
    fn from(error: Rejected<'a, 'b>) -> Self {
        Self::new(status::Code::BAD_REQUEST, (), error.0)
    }
}

pub const CASES: [(&[u8], &[u8]); 3] = [
    (
        b"GET / HTTP/1.1\r\nHost: task\r\nConnection: close\r\n\r\n",
        b"HTTP/1.1 200 OK\r\n",
    ),
    (
        b"GET /?mode=1 HTTP/1.1\r\nHost: task\r\nConnection: close\r\n\r\n",
        b"HTTP/1.1 400 Bad Request\r\n",
    ),
    (
        b"GET /?mode=2 HTTP/1.1\r\nHost: task\r\nConnection: close\r\n\r\n",
        b"HTTP/1.1 503 Service Unavailable\r\n",
    ),
];

#[sark::handler(pub)]
async fn reply<'a, 'b>(request: ModeRequest, state: &State<'a, 'b>) -> Outcome<'a, 'b> {
    let mut body = Body {
        first: state.first,
        second: state.second,
        encoded: state.encoded,
    };
    Yield::new().await;
    if request.mode == 2 {
        return Err(Response::new(status::Code::SERVICE_UNAVAILABLE, (), body));
    }
    body = match request.mode {
        1 => Err(Rejected(body)),
        _ => Ok(body),
    }?;
    Ok(Response::new(status::Code::OK, (), body))
}

sark::define_route! {
    pub App: State<'a, 'b> => {
        GET "/" => request(query) async(capacity = 2, body = discarded) reply,
    }
}
