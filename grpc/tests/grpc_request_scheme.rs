pub mod common;

use std::iter;

use common::{grpc::call::ingress::SCHEMES, headers};
use sark_grpc::{
    client::{call::unary::Session as _, config::Limits, io::Session as _, session::Session},
    details::{Diagnostic, Fault},
    metadata::Metadata,
    protocol::{request, section},
};
use sark_h2::{
    ServerRole, client,
    conn::{event::Event, machine::Connection},
};
use sark_http::field::metadata::map::Owned;

#[test]
fn rejects_unsupported_scheme() {
    let mut fields = headers::try_request([]).expect("request fields");
    fields[1] = Owned::try_new(b":scheme", b"ftp").expect("owned field");
    let error = request::Head::from_fields(&fields)
        .err()
        .expect("unsupported gRPC scheme");
    assert!(matches!(
        Diagnostic::from(&error),
        Diagnostic::SectionStored(Fault::Metadata)
    ));
}

#[test]
fn emits_scheme() {
    for (scheme, expected) in SCHEMES {
        let plan = client::Plan::<section::Plan>::new(Limits::default().calls()).expect("plan");
        Session::with(plan, |client| {
            let mut client = client.with_scheme(scheme);
            let call = client
                .start_raw(b"/svc/Unary", None, &Metadata::new(), b"")
                .expect("request");
            let mut server = Connection::<ServerRole>::try_new().expect("server");
            server
                .ingress()
                .ingest(client.outbound())
                .expect("request wire");
            let headers = iter::from_fn(|| server.ingress().poll_event())
                .find_map(|event| match event {
                    Event::Headers { head, .. } => Some(head.into_parts().0),
                    _ => None,
                })
                .expect("request headers");
            assert_eq!(
                headers
                    .iter()
                    .find(|field| field.name == b":scheme")
                    .map(|field| field.value),
                Some(expected)
            );
            request::Head::from_fields(&headers).expect("admit emitted gRPC scheme");
            client.cancel(call);
        })
        .expect("session");
    }
}
