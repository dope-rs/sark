pub mod common;

use std::{iter, pin::Pin};

use common::{
    headers::{self, Generated},
    recipe::Reply,
};
use dope::core::driver::settings::Receive;
use o3::{base::cell::region, buffer::resident};
use sark_core::{
    body,
    decode::{Decode, request::Head as _},
    manifold::stream::Boundary,
};
use sark_h2::{
    ClientRole,
    conn::{event::Event, machine::Connection},
    server::{
        self,
        service::{FromFn, Handler, Route},
        sync::{Conn, Endpoint},
    },
};
use sark_http::payload::response::statics::Direct;

#[test]
fn dispatches_named() {
    let plan = server::Plan::new(
        server::Config {
            max_request_body_bytes: 0,
            max_request_body_chunks: 0,
            max_connection_body_bytes: 0,
            max_connection_body_chunks: 0,
            max_outbound_bytes: 4096,
        },
        Receive::fixed::<2, 4096>(),
    )
    .expect("response plan");
    let capacity = plan.resident_len();
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(capacity, token.proof())
            .expect("ingress capacity");
        let handler = FromFn::new(Direct::reply).with_bound::<4096>();
        assert_eq!(size_of_val(&handler), 0);
        let endpoint = Endpoint::try_new(Route::new(handler), plan).expect("endpoint");
        let mut server = Conn::try_new(&endpoint).expect("server connection");
        server.ingress().try_bind(budget).expect("ingress budget");
        let mut client = Connection::<ClientRole>::try_new().expect("client connection");
        client
            .streams()
            .headers()
            .request(Generated(b""), Boundary::End)
            .expect("request");
        server
            .ingress()
            .ingest(client.output().first())
            .expect("request ingress");
        server.drain_events();
        client
            .ingress()
            .ingest(server.output().first())
            .expect("response ingress");
        let (data, boundary) = iter::from_fn(|| client.ingress().poll_event())
            .find_map(|event| match event {
                Event::Data { data, boundary, .. } => Some((data, boundary)),
                _ => None,
            })
            .expect("response data");
        assert_eq!(data.contiguous(), Some(b"named".as_slice()));
        assert_eq!(boundary, Boundary::End);
    });
}

#[test]
fn admitted_content_length() {
    let route = Route::new(FromFn::new(Direct::reply).with_bound::<4096>());
    for (limit, expected) in [
        (5, None),
        (
            4,
            Some(body::Error::TooLarge {
                actual: 5,
                limit: 4,
            }),
        ),
    ] {
        let head = headers::try_head(&[b"5, 5", b"05"])
            .expect("retained raw head")
            .into_parts()
            .0
            .try_request()
            .ok()
            .expect("valid request control");
        let selected = route.try_head(head).expect("admitted request length");
        assert_eq!(
            selected
                .try_start(limit, Boundary::More)
                .err()
                .map(|failure| failure.into_parts().1),
            expected,
        );
    }
}
