pub mod common;

use common::{
    codec::Bytes,
    grpc::{
        call::calls::{Response, Settings},
        wire::Transfer,
    },
};
use sark_grpc::{
    client::{call::response, io::ingress::Session as _, receive, session::Session},
    metadata::Metadata,
    protocol::section,
};
use sark_h2::{ServerRole, client, conn::machine::Connection};

#[test]
fn only_own_pending() {
    for own in [true, false] {
        let limits = Settings::new(Some(2), Some(1), Some(1), Some(2))
            .try_build()
            .expect("client limits");
        Session::<_>::with_limits(
            client::Plan::<section::Plan>::new(limits.calls()).expect("H2 plan"),
            limits,
            |mut client| {
                let mut server = Connection::<ServerRole>::try_new().expect("H2 server");
                let metadata = Metadata::new();
                let [cancel, retained] = [(); 2].map(|()| {
                    response::Session::try_start(
                        &mut client,
                        b"/proof/Watch",
                        None,
                        &metadata,
                        &mut Bytes,
                        &b"request".to_vec(),
                    )
                    .into_result()
                    .expect("response stream")
                });
                let ids = Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_request()
                .expect("requests");
                Response::new(&mut server, ids[0])
                    .try_header()
                    .expect("first header");
                if own {
                    Response::new(&mut server, ids[0])
                        .try_message(b"discard")
                        .expect("blocked data");
                }
                Response::new(&mut server, ids[1])
                    .try_respond(b"retained")
                    .expect("other response");
                Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_response()
                .expect("responses");
                assert!(client.is_blocked());
                response::Session::cancel(&mut client, cancel);
                assert!(matches!(
                    response::Session::try_poll(&mut client, &retained).expect("other header"),
                    Some(receive::Response::Headers { .. })
                ));
                let event = response::Session::try_poll(&mut client, &retained)
                    .expect("other message")
                    .expect("message available");
                let message = match event {
                    receive::Response::Message { message } => Some(message),
                    receive::Response::Headers { .. } | receive::Response::Trailers { .. } => None,
                }
                .expect("message event");
                assert_eq!(message.payload.contiguous(), Some(b"retained".as_slice()));
                assert!(matches!(
                    response::Session::try_poll(&mut client, &retained).expect("other trailers"),
                    Some(receive::Response::Trailers { .. })
                ));
            },
        )
        .expect("client session");
    }
}
