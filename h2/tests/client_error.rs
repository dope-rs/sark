pub mod common;

use common::{
    hpack::Fields,
    protocol::{
        Session,
        conn::Controls,
        content_length::{BODY, LENGTHS},
        session::{Frame, Wire},
    },
};
use sark_core::manifold::{
    client::{
        Connection as _,
        event::{Reset, Response},
        io::Read as _,
    },
    stream::{Boundary, Message as _},
};
use sark_h2::{
    ClientRole,
    conn::{Error, machine::Connection},
    frame::{
        Ack, Block,
        numbers::{ErrorCode, SettingId},
    },
};
use sark_http::field::metadata::map::record::Entry;

#[test]
fn retains_setting_failure() {
    let mut conn = Connection::<ClientRole>::try_new().expect("client connection");
    let peer = Controls::new(Ack::Required)
        .try_settings(&[(SettingId::MaxFrameSize as u16, 15_000)])
        .expect("peer settings");
    assert!(matches!(
        conn.ingest(&peer),
        Err(Error::Setting {
            id: SettingId::MaxFrameSize,
            value: 15_000,
        })
    ));
}

#[test]
fn body_length_mismatch_reaches_client_as_reset() {
    for (length, expected) in LENGTHS {
        let mut conn = Connection::<ClientRole>::try_new().expect("client connection");
        Session::new(&mut conn).try_prime().expect("peer preface");
        let id = Session::new(&mut conn).try_open().expect("request");
        let key = conn.status().stream_key(id).expect("request stream");
        let block = Fields::new(&[
            Entry::new(b":status", b"200"),
            Entry::new(b"content-length", length),
        ])
        .try_encode()
        .expect("response block");
        conn.ingest(
            &Frame::new(id.into_u32(), &block)
                .try_headers(Boundary::More, Block::End)
                .expect("response headers"),
        )
        .expect("response ingress");
        assert!(matches!(conn.poll(), Some(Response::Head { .. })));
        conn.ingest(
            &Frame::new(id.into_u32(), BODY)
                .try_padded(Boundary::End, 2)
                .expect("response DATA"),
        )
        .expect("DATA ingress");
        if expected == BODY.len() as u64 {
            assert!(matches!(
                conn.poll(),
                Some(Response::Data { message, data })
                    if message.boundary() == Boundary::End && data.contiguous() == Some(BODY)
            ));
        } else {
            assert!(matches!(
                conn.poll(),
                Some(Response::Reset { stream, reason: Reset::Other }) if stream == key
            ));
            assert_eq!(
                Wire::new(conn.output().first())
                    .try_reset()
                    .expect("reset frame"),
                Some((id, ErrorCode::ProtocolError))
            );
        }
        assert!(conn.poll().is_none());
    }
}
