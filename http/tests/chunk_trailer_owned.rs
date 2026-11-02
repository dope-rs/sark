use std::{convert::Infallible, pin::Pin};

use o3::{
    base::cell::region,
    buffer::{resident, storage::Owned},
};
use sark_http::h1::{
    chunk,
    codec::decode::{chunks, trailer},
};

#[test]
fn growth_keeps_admission() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(16, token.proof())
            .expect("budget");
        let decoder = chunks::Decoder::with_limit(0);
        let pending = decoder
            .decode(b"0\r\nX: a")
            .expect("chunk prefix")
            .into_trailer()
            .ok()
            .expect("partial trailer");
        assert_eq!(pending.consumed().get(), 7);
        let state = pending
            .try_store(Owned::try_allocate(&budget, 4).expect("owner"))
            .ok()
            .expect("store");
        let plan = state
            .prepare(b"\r\n\r\nNEXT")
            .ok()
            .expect("admitted suffix");
        let mut checks = 0;
        let plan = plan
            .try_map(|count| {
                checks += 1;
                Ok::<_, Infallible>(count)
            })
            .ok()
            .expect("bound input");
        let plan = plan.split().ok().expect("complete trailer");
        let (mut plan, _) = plan
            .try_append(|| Ok::<_, Infallible>(()))
            .err()
            .expect("capacity")
            .into_parts();
        assert_eq!(plan.len(), 4);
        plan.try_grow(Owned::try_allocate(&budget, 8).expect("larger owner"))
            .expect("same prefix");
        let (plan, error) = plan
            .try_append(|| Err::<(), _>("dependent"))
            .err()
            .expect("dependent failure")
            .into_parts();
        assert!(matches!(error, trailer::AppendError::Ready("dependent")));
        assert_eq!(plan.len(), 4);
        let (owner, consumed, ()) = plan
            .try_append(|| Ok::<_, Infallible>(()))
            .ok()
            .expect("retained admission");
        assert_eq!(consumed.get(), 4);
        assert_eq!(checks, 1);
        assert_eq!(
            owner.fields().collect::<Vec<_>>(),
            [(b"X".as_slice(), b"a".as_slice())]
        );
        assert_eq!(owner.into_owner().as_slice(), b"X: a\r\n");
    });
}

#[test]
fn split_forbidden_name() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(32, token.proof())
            .expect("budget");
        let pending = chunks::Decoder::with_limit(0)
            .decode(b"0\r\nContent-Len")
            .expect("chunk prefix")
            .into_trailer()
            .ok()
            .expect("partial name");
        let state = pending
            .try_store(Owned::try_allocate(&budget, 32).expect("owner"))
            .ok()
            .expect("store");
        let (state, error) = state
            .prepare(b"gth: 1\r\n\r\n")
            .err()
            .expect("forbidden")
            .into_parts();
        assert!(matches!(error, chunk::Error::Forbidden { .. }));
        assert_eq!(state.len(), 11);
    });
}
