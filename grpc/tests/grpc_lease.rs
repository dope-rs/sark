use std::pin::Pin;

pub mod common;

use common::grpc::{
    call::calls::Response,
    replies::{METHOD, REQUEST, SHARED, SMALL},
};
use o3::{
    base::cell::region,
    buffer::{hold, resident},
};
use sark_grpc::{
    Code,
    client::{
        call::unary::Session as _,
        config::Limits,
        io::{Session as _, ingress::Session as _},
        session::Session,
    },
    message,
    metadata::Metadata,
    protocol::section,
};
use sark_h2::{INGRESS_RETENTION, client, conn};

#[test]
fn copy_requires_budget() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let domain = domain.get_mut();
        let limits = Limits::default();
        Session::<client::Plan<section::Plan>, resident::Shared<'_>>::with_limits(
            client::Plan::<section::Plan>::new(limits.calls()).expect("H2 client plan"),
            limits,
            |mut client| {
                let call = client
                    .start_raw(METHOD, None, &Metadata::new(), REQUEST)
                    .expect("request stream");
                let wire = Response::try_framed(client.outbound(), SMALL).expect("response wire");
                let framed = message::HEADER_LEN + SMALL.len();
                assert!(matches!(
                    INGRESS_RETENTION.choose(framed, || wire.len()),
                    hold::Choice::Copy
                ));
                let budget = domain
                    .try_budget(wire.len(), token.proof())
                    .expect("empty resident domain");
                client.try_bind(budget).expect("receive authority");
                let owner = resident::Exact::try_copy(&budget, &wire)
                    .expect("response owner fills the budget")
                    .into_shared();

                let error = client.ingest_owned(owner).expect_err("copy needs headroom");
                assert_eq!(error.code(), Code::ResourceExhausted);
                assert!(client.poll(&call).expect("response claim").is_none());
                assert_eq!(budget.resident(), wire.len());
                drop(client);
                assert_eq!(budget.resident(), 0);
            },
        )
        .expect("scoped-owner client");
    });
}

#[test]
fn share_preserves_lease() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let domain = domain.get_mut();
        let limits = Limits::default();
        Session::<client::Plan<section::Plan>, resident::Shared<'_>>::with_limits(
            client::Plan::<section::Plan>::new(limits.calls()).expect("H2 client plan"),
            limits,
            |mut client| {
                let call = client
                    .start_raw(METHOD, None, &Metadata::new(), REQUEST)
                    .expect("request stream");
                let wire = Response::try_framed(client.outbound(), SHARED).expect("response wire");
                let framed = message::HEADER_LEN + SHARED.len();
                assert!(matches!(
                    INGRESS_RETENTION.choose(framed, || wire.len()),
                    hold::Choice::Share
                ));
                let offset = wire
                    .windows(SHARED.len())
                    .position(|bytes| bytes == SHARED)
                    .expect("payload on wire");
                let budget = domain
                    .try_budget(wire.len(), token.proof())
                    .expect("empty resident domain");
                client.try_bind(budget).expect("receive authority");
                let owner = resident::Exact::try_copy(&budget, &wire)
                    .expect("one response owner")
                    .into_shared();
                let address = owner.as_slice().as_ptr().wrapping_add(offset);

                client.ingest_owned(owner).expect("shared response ingress");
                let result = client
                    .poll(&call)
                    .expect("response claim")
                    .expect("completed response");
                drop(client);
                let bytes = result
                    .message
                    .as_ref()
                    .expect("response message")
                    .payload
                    .contiguous()
                    .expect("one retained owner");
                assert_eq!(bytes, SHARED);
                assert_eq!(bytes.as_ptr(), address);
                assert_eq!(budget.resident(), wire.len());
                assert!(matches!(
                    resident::Exact::try_copy(&budget, SHARED),
                    Err(resident::AllocError::Budget(_))
                ));
                drop(result);
                assert_eq!(budget.resident(), 0);
            },
        )
        .expect("scoped-owner client");
    });
}

#[test]
fn copy_preserves_authority() {
    let mut domains = (resident::Domain::new(), resident::Domain::new());
    region::Token::scope_pinned(Pin::new(&mut domains), |token, domains| {
        let (domain, foreign) = domains.get_mut();
        let limits = Limits::default();
        Session::<client::Plan<section::Plan>, resident::Shared<'_>>::with_limits(
            client::Plan::<section::Plan>::new(limits.calls()).expect("H2 client plan"),
            limits,
            |mut client| {
                let call = client
                    .start_raw(METHOD, None, &Metadata::new(), REQUEST)
                    .expect("request stream");
                let wire = Response::try_framed(client.outbound(), SMALL).expect("response wire");
                let framed = message::HEADER_LEN + SMALL.len();
                assert!(matches!(
                    INGRESS_RETENTION.choose(framed, || wire.len()),
                    hold::Choice::Copy
                ));
                let capacity = wire.len() + framed;
                let budget = domain
                    .try_budget(capacity, token.proof())
                    .expect("empty resident domain");
                let foreign = foreign
                    .try_budget(capacity, token.proof())
                    .expect("empty foreign domain");
                client.try_bind(budget).expect("receive authority");
                client.try_bind(budget).expect("same authority");
                assert!(matches!(
                    client.try_bind(foreign),
                    Err(conn::Error::Authority(resident::BindError::Foreign))
                ));
                let owner = resident::Exact::try_copy(&budget, &wire)
                    .expect("response owner leaves copy headroom")
                    .into_shared();

                client.ingest_owned(owner).expect("copied response ingress");
                let result = client
                    .poll(&call)
                    .expect("response claim")
                    .expect("completed response");
                drop(client);
                assert_eq!(
                    result
                        .message
                        .as_ref()
                        .expect("response message")
                        .payload
                        .contiguous(),
                    Some(SMALL)
                );
                assert_eq!(budget.resident(), framed);
                assert_eq!(foreign.resident(), 0);
                drop(result);
                assert_eq!(budget.resident(), 0);
            },
        )
        .expect("scoped-owner client");
    });
}
