use dope::manifold::client::session::codec::Stateful as _;
use o3::buffer::{self, hold, resident, segments, storage::Shared};
use sark_client::{
    protocol::{
        codec::{Codec, output},
        response::Event,
    },
    reply::{self, assembly::Assembly},
};
use sark_http::payload::response::Method;

use crate::common::{codec::decoder::Decoder, response::Error};

pub const HEAD: &[u8] =
    b"HTTP/1.1 201 Created\r\nTransfer-Encoding: chunked\r\nX-Head: retained\r\n\r\n";
pub const TRAILERS: &[u8] = b"0\r\nX-Tail: kept\r\n\r\n";
pub const FRAGMENTED: &[u8] = &[b'x'; output::DATA_LIMIT + 1];
pub const PADDED: &[u8] = &[b'x'; 4096];
pub const COPIED_FIELDS: usize =
    b"Transfer-Encoding: chunked\r\nX-Head: \r\nX-Tail: \r\n".len() + 2 * PADDED.len();

pub fn chunked(body: &[u8]) -> Vec<u8> {
    let mut wire = HEAD.to_vec();
    if !body.is_empty() {
        wire.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
        wire.extend_from_slice(body);
        wire.extend_from_slice(b"\r\n");
    }
    wire.extend_from_slice(TRAILERS);
    wire
}

pub fn aliased_fields() -> [Vec<u8>; 2] {
    let mut head = b"HTTP/1.1 201 Created\r\nTransfer-Encoding: chunked\r\nX-Head: ".to_vec();
    head.extend_from_slice(PADDED);
    head.extend_from_slice(b"\r\n\r\n");
    let mut trailers = b"0\r\nX-Tail: ".to_vec();
    trailers.extend_from_slice(PADDED);
    trailers.extend_from_slice(b"\r\n\r\n");
    [head, trailers]
}

pub fn events<'d>(budget: resident::Budget<'d>) -> Result<[Event<'d>; 4], Error> {
    let wire = Shared::try_copy(
        concat!(
            "HTTP/1.1 103 Early Hints\r\n\r\n",
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
            "1\r\nx\r\n0\r\nX-Tail: kept\r\n\r\n",
        )
        .as_bytes(),
    )
    .map_err(Error::Storage)?;
    let codec = Codec::default();
    let decoder = Decoder::try_new(&codec).map_err(Error::Staging)?;
    let mut state = codec.state(budget);
    let mut stored = decoder.storage(&wire, &state);
    let mut events = [const { None }; 4];
    let mut slots = events.iter_mut();
    while !stored.source().as_ref().is_empty() {
        let (output, _) = decoder
            .try_stored(&mut state, &mut stored, &budget)
            .map_err(Error::Decode)?
            .ok_or(Error::Incomplete)?;
        for event in output.into_events() {
            *slots.next().ok_or(Error::Incomplete)? = Some(event.map_err(Error::Protocol)?);
        }
    }
    let [Some(info), Some(head), Some(data), Some(trailers)] = events else {
        return Err(Error::Incomplete);
    };
    Ok([info, head, data, trailers])
}

pub fn try_sources<'a, 'd, R>(
    pool: &'a segments::Pool<resident::Shared<'d>>,
    budget: resident::Budget<'d>,
    method: Method,
    sources: &[R],
) -> Result<reply::Scoped<'a, 'd>, Error>
where
    R: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Source,
{
    let codec = Codec::default();
    let decoder = Decoder::try_new(&codec).map_err(Error::Staging)?;
    let mut state = codec.state(budget);
    let mut assembly = ControlFlow::Continue(Assembly::new(pool));
    for source in sources {
        let mut stored = decoder.storage(source, &state);
        while !stored.source().as_ref().is_empty() {
            let (output, _) = decoder
                .try_decode(&mut state, &mut stored, &budget, || Some(method))
                .map_err(Error::Decode)?
                .ok_or(Error::Incomplete)?;
            for event in output.into_events() {
                let event = event.map_err(Error::Protocol)?;
                assembly = match assembly {
                    ControlFlow::Continue(head) => head.accept(event).map_err(Error::Assembly)?,
                    ControlFlow::Break(mut body) => {
                        body.accept(event).map_err(Error::Assembly)?;
                        ControlFlow::Break(body)
                    }
                };
            }
        }
    }
    match assembly {
        ControlFlow::Continue(head) => Err(Error::Assembly(head.missing())),
        ControlFlow::Break(body) => Ok(body.finish()),
    }
}
use std::ops::ControlFlow;
