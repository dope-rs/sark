pub mod common;

use std::num::NonZero;

use common::{
    field::sections::{
        Apply as _, Authority, CompactPlan, LENGTH_CONFLICT, LENGTH_INVALID, LENGTH_RESPONSES,
        LENGTH_VALUES, LENGTH_ZERO, PATH, REQUEST, TE_VALUES,
    },
    response::{
        headers::{self, Admit as _},
        phase::{INVALID, VALID},
    },
    uri::NAMES,
};
use o3::buffer::pool;
use sark_http::{
    field::{
        Indexing, head,
        metadata::{
            block::{self, Validation},
            lower::{self, Input, Step},
            map::record::{Entry, Span},
            section::{
                rules::{self, Kind},
                state::{
                    self, Decoder, complete,
                    decode::{Completion as _, Final, Indexed, Omit, Plan, Session, Sink, Start},
                    fixed,
                },
                storage::{self, Fields, Scratch, Tag},
            },
            strings::{Cursor, Encoding},
        },
    },
    payload::response::Method,
};

#[test]
fn user_slot_bounds() {
    for slot in [storage::Slot::exact::<0>(), storage::Slot::exact::<65529>()] {
        assert_eq!(Tag::user(slot).user_slot(), Some(slot));
    }
    assert_eq!(
        storage::Slot::try_usize(65530)
            .expect_err("reserved tags reduce the slot domain")
            .actual(),
        65530,
    );
    assert_eq!(Tag::PROTOCOL.user_slot(), None);
}

#[test]
fn push_requires_safe_get() {
    assert!(Authority::is_valid(
        Kind::PushPromise,
        &Authority::request(b"GET", None)
    ));
    assert!(Authority::is_valid(
        Kind::PushPromise,
        &Authority::request(b"HEAD", Some(Entry::new(b"content-length", b"0 \t, 00")),),
    ));
    assert!(!Authority::is_valid(
        Kind::PushPromise,
        &Authority::request(b"POST", None)
    ));
    assert!(!Authority::is_valid(
        Kind::PushPromise,
        &Authority::request(b"GET", Some(Entry::new(b"content-length", b"1"))),
    ));
    assert!(!Authority::is_valid(
        Kind::PushPromise,
        &Authority::request(b"GET", Some(Entry::new(b"content-length", b"0\r"))),
    ));
    for value in [b"0 0", b"0\t0"] {
        assert!(matches!(
            Authority::validation(
                Kind::PushPromise,
                &Authority::request(b"GET", Some(Entry::new(b"content-length", value))),
            ),
            Validation::RejectPush,
        ));
    }
}

#[test]
fn push_reject_is_distinct() {
    assert!(matches!(
        Authority::validation(Kind::PushPromise, &Authority::request(b"POST", None)),
        Validation::RejectPush,
    ));
    assert!(matches!(
        Authority::validation(Kind::PushPromise, &[Entry::new(b":method", b"GET")],),
        Validation::Malformed,
    ));
    assert!(Authority::is_valid(
        Kind::Request,
        &Authority::request(b"POST", None)
    ));
}

#[test]
fn push_retains_method() {
    let lease = pool::Span::try_new(128).expect("lease");
    let mut scratch = Scratch::default();
    let mut decoder = complete::Decoder::<CompactPlan>::try_new(
        lease,
        &mut scratch,
        Kind::PushPromise,
        block::Connect::Disabled,
    )
    .expect("push section");
    for field in Authority::request(b"HEAD", Some(LENGTH_ZERO)) {
        CompactPlan::try_push(&mut decoder, field).expect("push field");
    }
    let complete = decoder.finish().expect("complete section");
    assert!(matches!(&complete, Final::Other(_)));
    let proof = complete.framing().expect("admitted framing");
    assert_eq!(proof.method(), Some(Method::Head));
    assert_eq!(proof.content_length(), Some(0));
}

#[test]
fn push_authority_rules() {
    let absent = [
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"https"),
        Entry::new(b":path", b"/asset"),
    ];
    assert!(matches!(
        Authority::validation(Kind::PushPromise, &absent),
        Validation::RejectPush,
    ));

    let mut foreign = Authority::request(b"GET", None);
    foreign[3] = Entry::new(b":authority", b"foreign.example");
    assert!(matches!(
        Authority::validation(Kind::PushPromise, &foreign),
        Validation::RejectPush,
    ));
    assert!(Authority::is_valid(Kind::Request, &foreign));

    let mut empty = Authority::request(b"GET", None);
    empty[3] = Entry::new(b":authority", b"");
    assert!(matches!(
        Authority::validation(Kind::PushPromise, &empty),
        Validation::RejectPush,
    ));

    let mut repeat = Authority::request(b"GET", None);
    repeat.push(Entry::new(b":authority", b"example.test"));
    assert!(matches!(
        Authority::validation(Kind::PushPromise, &repeat),
        Validation::Malformed,
    ));
}

#[test]
fn store_failure_resets() {
    let lease =
        (pool::Span::try_new(Tag::PREFIX_LEN)).expect("tagged-prefix test lease must allocate");
    let mut input = Input::new(storage::Writer::try_new(lease).expect("empty prefix fits"));
    let mut scratch = Scratch::default();
    let mut section = Decoder::<CompactPlan>::h3(
        Kind::Request,
        block::Connect::Disabled,
        CompactPlan::default(),
    );
    let name = Span::from_slice(b":path");
    let start = (<Decoder<CompactPlan> as Sink<CompactPlan>>::begin(
        &mut section,
        &mut input,
        block::Named::from_span(name),
        Indexing::Allowed,
    ))
    .expect("a tagged field must begin in an empty prefix-sized lease");
    assert!(matches!(start, Start::Direct(Plan)));
    let cause = match input.string(&mut scratch, Cursor::new(1, Encoding::Plain), b"x") {
        Step::Store { cause } => Some(cause),
        _ => None,
    }
    .expect("a full tagged lease must report its exact storage failure");
    let cause = match cause {
        lower::Error::SinkProgress {
            cause,
            stored,
            remaining: left,
            input,
        } => {
            assert_eq!(stored, [Tag::PREFIX_LEN; 2]);
            assert_eq!(left, 1);
            assert_eq!(input, [1, 0, 0]);
            Some(cause)
        }
        _ => None,
    }
    .expect("a full tagged lease must report its capacity failure");
    assert_eq!(cause.into_parts(), (NonZero::<usize>::MIN, 0));
    let lease = input
        .try_seal()
        .expect("rolled back input")
        .finish()
        .into_lease();
    assert_eq!(
        lease.len(),
        0,
        "storage failure must roll back the tagged prefix"
    );
}

#[test]
fn discard_reuses_scratch() {
    let mut lease =
        (pool::Span::try_new(128)).expect("compressed-section test lease must allocate");
    let mut scratch = Scratch::default();
    let mut section = Decoder::<CompactPlan>::h3(
        Kind::Request,
        block::Connect::Disabled,
        CompactPlan::default(),
    );
    for field in [
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"https"),
        Entry::new(b":authority", b"EXAMPLE.com:443"),
        Entry::new(b":path", b"/x"),
        Entry::new(b"host", b"example.com"),
    ] {
        section
            .try_lower::<Authority>(&mut lease, &mut scratch, field)
            .expect("compressed-section fixture fits storage");
    }
    assert_eq!(lease.len(), Tag::PREFIX_LEN + b"/x".len());
    assert_eq!(scratch.authority(), b"EXAMPLE.com:443");
    let sealed = Decoder::seal(&mut section, &mut lease, &mut scratch);
    let (path, state, plan) = section.finish_plan();
    assert!(sealed == Ok(state));
    assert!(matches!(state, Validation::Valid));
    assert!(plan.is_some());
    let path = (path).expect("valid compact plan must retain the path");
    assert_eq!(&lease.as_slice()[path], b"/x");
    assert_eq!(lease.len(), Tag::PREFIX_LEN + b"/x".len());
    assert!(scratch.authority().is_empty());
    let storage = storage::Finished::try_from(lease).expect("valid packed fixture");
    let frozen = <Fields as storage::Block>::from(storage);
    let mut fields = (frozen.iter_from(0)).expect("retained block must start at a field boundary");
    let (tag, _, _) = (fields.next())
        .expect("retained block must contain its path field")
        .expect("retained block must contain its path field");
    assert!((tag) == (Tag::PATH));
    assert!(fields.next().is_none());
}

#[test]
fn connect_retains_origin() {
    let mut lease =
        (pool::Span::try_new(128)).expect("compressed-section test lease must allocate");
    let mut scratch = Scratch::default();
    let mut section = Decoder::<CompactPlan>::new(Kind::Request, CompactPlan::default());
    for field in [
        Entry::new(b":method", b"CONNECT"),
        Entry::new(b":authority", b"origin.example:443"),
    ] {
        section
            .try_lower::<Authority>(&mut lease, &mut scratch, field)
            .expect("compressed-section fixture fits storage");
    }
    assert_eq!(lease.len(), 0);
    assert_eq!(scratch.authority(), b"origin.example:443");
    let sealed = Decoder::seal(&mut section, &mut lease, &mut scratch);
    let (path, state, plan) = section.finish_plan();
    assert!(sealed == Ok(state));
    assert!(path.is_none());
    assert!(matches!(state, Validation::Valid));
    assert!((plan.map(block::Plan::form)) == (Some(block::Form::Connect)));
    assert_eq!(plan.map(block::Plan::method), Some(Method::Connect));
    let origin = Tag::PREFIX_LEN..lease.len();
    assert_eq!(
        lease.as_slice().get(..Tag::PREFIX_LEN),
        Tag::AUTHORITY
            .prefix(origin.len())
            .as_ref()
            .map(<[u8; Tag::PREFIX_LEN]>::as_slice),
    );
    assert_eq!(&lease.as_slice()[origin.clone()], b"origin.example:443");
    assert_eq!(lease.len(), Tag::PREFIX_LEN + b"origin.example:443".len());
    assert!(scratch.authority().is_empty());
}

#[test]
fn retries_seal_capacity() {
    let mut lease = (pool::Span::try_new(1))
        .expect("allocate a destination too small for the staged authority");
    let mut scratch = Scratch::default();
    let mut section = Decoder::<CompactPlan>::new(Kind::Request, CompactPlan::default());
    for field in [
        Entry::new(b":method", b"CONNECT"),
        Entry::new(b":authority", b"origin.example:443"),
    ] {
        section
            .try_lower::<Authority>(&mut lease, &mut scratch, field)
            .expect("compressed-section fixture fits storage");
    }
    let cause = match Decoder::seal(&mut section, &mut lease, &mut scratch)
        .expect_err("preserve the exact capacity failure")
    {
        lower::Error::Write(cause) => Some(cause),
        _ => None,
    }
    .expect("preserve the exact capacity failure");
    let (required, room) = cause.into_parts();
    assert_eq!(
        (required.get(), room),
        (Tag::PREFIX_LEN + b"origin.example:443".len(), 1)
    );
    assert_eq!(lease.len(), 0);
    assert_eq!(scratch.authority(), b"origin.example:443");
    let mut lease =
        (pool::Span::try_new(required.get())).expect("allocate the reported destination size");
    assert!(matches!(
        Decoder::seal(&mut section, &mut lease, &mut scratch),
        Ok(Validation::Valid)
    ));
    assert_eq!(&lease.as_slice()[Tag::PREFIX_LEN..], b"origin.example:443");
    assert!(scratch.authority().is_empty());
}

#[test]
fn authority_mismatch_fails() {
    for &(left, right, equal) in NAMES {
        for (authority, host) in [(left, right), (right, left)] {
            for split in 0..=authority.len().max(host.len()) {
                let mut lease = (pool::Span::try_new(128))
                    .expect("compressed-section test lease must allocate");
                let mut scratch = Scratch::default();
                let mut section = Decoder::<CompactPlan>::h3(
                    Kind::Request,
                    block::Connect::Disabled,
                    CompactPlan::default(),
                );
                for field in [
                    Entry::new(b":method", b"GET"),
                    Entry::new(b":scheme", b"https"),
                    Entry::new(b":authority", authority),
                    Entry::new(b":path", b"/x"),
                    Entry::new(b"host", host),
                ] {
                    let name = Span::from_slice(field.name);
                    let (first, second) = field.value.split_at(split.min(field.value.len()));
                    let mut lower = lower::Lower::new(&mut lease, &mut scratch, &mut section);
                    let decision = lower.disposition(name, block::Name::classify(field.name));
                    lower
                        .try_push(decision, name, Span::new(first, second))
                        .expect("compressed-section fixture fits storage")
                        .commit::<Authority>();
                }
                let expected = if equal {
                    Validation::Valid
                } else {
                    Validation::Malformed
                };
                assert_eq!(
                    Decoder::seal(&mut section, &mut lease, &mut scratch),
                    Ok(expected)
                );
                assert_eq!(section.finish().1, expected);
            }
        }
    }
}

#[test]
fn asterisk_needs_options() {
    for version in [block::Version::Two, block::Version::Three] {
        for (kind, method, expected) in headers::ASTERISK_METHODS {
            let mut fields = headers::ASTERISK.to_vec();
            if method == b"CONNECT" {
                fields.push((head::Name::Protocol, b"websocket"));
            }
            fields.push((head::Name::Method, method));
            for _ in 0..2 {
                let gate = block::Gate::new(kind, block::Connect::Enabled, version);
                let (plan, verdict) = gate.fields(&fields);
                assert_eq!(verdict, expected);
                assert_eq!(
                    plan.map(block::Plan::form),
                    (expected == block::Validation::Valid).then_some(block::Form::Asterisk),
                );
                fields.reverse();
            }
        }
    }
}

#[test]
fn retains_response_method() {
    for version in [block::Version::Two, block::Version::Three] {
        for (method, expected) in headers::METHODS {
            let mut fields = headers::TARGET.to_vec();
            fields.push((head::Name::Method, method));
            let (plan, verdict) =
                block::Gate::new(block::Kind::Request, block::Connect::Disabled, version)
                    .fields(&fields);
            assert_eq!(verdict, Validation::Valid);
            assert_eq!(plan.map(block::Plan::method), Some(expected));
        }
    }
}

#[test]
fn rejects_invalid_names() {
    for name in [b"x bad".as_slice(), b"x:y", b"x\0y", b"x\xff", b"x)y"] {
        let proof = block::Name::classify(name);
        let verdict = block::Gate::new(
            block::Kind::Trailers,
            block::Connect::Disabled,
            block::Version::Two,
        )
        .accept_name(proof.head());
        assert_eq!(verdict, block::Validation::Malformed);
    }
}

#[test]
fn validates_discarded() {
    use rules::Validate as _;

    for value in [b"a\r\nb".as_slice(), b"a\0b", b" a", b"a\t"] {
        let mut scan = block::Value::new();
        scan.extend(&value[..1]);
        scan.extend(&value[1..]);
        let mut state = fixed::Raw::new(rules::Kind::RequestTrailers);
        state.value(scan.finish());
        assert_eq!(state.finish().1, block::Validation::Malformed);
    }
    let mut scan = block::Value::new();
    scan.extend(b"a\t");
    scan.extend(b" b");
    assert_eq!(scan.finish(), block::Validation::Valid);
}

#[test]
fn retains_huffman_tail() {
    let lease = pool::Span::try_new(storage::Tag::PREFIX_LEN + 1).expect("allocate");
    let writer = storage::Writer::<storage::Fields>::try_new(lease).expect("prefix");
    let mut input = lower::Input::new(writer);
    let mut scratch = storage::Scratch::default();
    let mut state = <fixed::Discard as Sink<rules::Discard>>::h2(
        rules::Kind::Request,
        block::Connect::Disabled,
    );
    let name = Span::from_slice(b":authority");
    <fixed::Discard as Sink<rules::Discard>>::begin(
        &mut state,
        &mut input,
        block::Named::from_span(name),
        Indexing::Allowed,
    )
    .expect("begin");
    assert!(matches!(
        input.string(&mut scratch, Cursor::new(1, Encoding::Huffman), &[0x1f]),
        lower::Step::Ready { .. }
    ));
    Omit.try_finish::<rules::RejectPush>(&mut state, &mut input, &mut scratch)
        .expect("finish");
    let lease = input.try_seal().expect("seal").finish().into_lease();
    assert_eq!(&lease.as_slice()[storage::Tag::PREFIX_LEN..], b"a");
}

#[test]
fn skips_rejected_authority() {
    let mut state = state::Decoder::new(rules::Kind::Request, CompactPlan::default());
    let name = Span::from_slice(b":authority");
    let proof = block::Name::from_span(name);
    assert!(matches!(
        state.decide(name, proof),
        rules::Decision::Scratch
    ));
    assert!(matches!(state.decide(name, proof), rules::Decision::Skip));
}

#[test]
fn validates_response_phase() {
    for version in [block::Version::Two, block::Version::Three] {
        for &(first, second, phase) in VALID {
            let response = block::Gate::response(version, Span::new(first, second));
            assert_eq!(response.validation(), block::Validation::Valid);
            assert_eq!(response.response(), Some(phase));
        }
        for status in INVALID {
            let response = block::Gate::response(version, Span::from_slice(status));
            assert_eq!(response.validation(), block::Validation::Malformed);
            assert_eq!(response.response(), None);
        }
    }
}

#[test]
fn te_request_lists() {
    for kind in [Kind::Request, Kind::PushPromise] {
        for &(value, expected) in TE_VALUES {
            let mut fields = Authority::request(b"GET", Some(Entry::new(b"te", value)));
            fields.push(Entry::new(b"te", b"trailers"));
            assert_eq!(
                Authority::validation(kind, &fields),
                expected,
                "{kind:?}: {value:?}"
            );
        }
    }
}

#[test]
fn te_request_only() {
    for kind in [
        Kind::Response,
        Kind::RequestTrailers,
        Kind::ResponseTrailers,
    ] {
        let mut fields = Vec::new();
        if kind == Kind::Response {
            fields.push(Entry::new(b":status", b"200"));
        }
        fields.push(Entry::new(b"te", b"trailers"));
        assert_eq!(
            Authority::validation(kind, &fields),
            Validation::Malformed,
            "{kind:?}"
        );
    }
}

#[test]
fn required_request_fields() {
    for (index, &field) in REQUEST.iter().take(3).enumerate() {
        let mut fields = REQUEST.to_vec();
        fields.remove(index);
        assert_eq!(
            Authority::validation(Kind::Request, &fields),
            Validation::Malformed
        );
        let mut fields = REQUEST.to_vec();
        fields.insert(index, field);
        assert_eq!(
            Authority::validation(Kind::Request, &fields),
            Validation::Malformed
        );
    }
    let mut fields = REQUEST.to_vec();
    fields[2] = Entry::new(b":path", b"");
    assert_eq!(
        Authority::validation(Kind::Request, &fields),
        Validation::Malformed
    );
}

#[test]
fn content_length_invalid() {
    for &values in LENGTH_INVALID {
        let mut fields = REQUEST.to_vec();
        fields.extend(
            values
                .iter()
                .map(|value| Entry::new(head::Name::ContentLength.wire(), value)),
        );
        assert_eq!(
            Authority::validation(Kind::Request, &fields),
            Validation::Malformed,
            "{values:?}"
        );
    }
}

#[test]
fn discarded_length_proof() {
    for &(values, expected) in LENGTH_VALUES {
        let mut scratch = Scratch::default();
        let mut decoder = CompactPlan::try_complete(&mut scratch).expect("request section");
        for value in values {
            CompactPlan::try_push(
                &mut decoder,
                Entry::new(head::Name::ContentLength.wire(), value),
            )
            .expect("content length field");
        }
        let complete = decoder.finish().expect("complete section");
        let proof = complete.framing().expect("admitted framing");
        assert_eq!(proof.content_length(), expected);
        assert_eq!(proof.status(), None);
        let (head, _, _, _) = complete.into_parts();
        let head = head.try_request().ok().expect("request");
        assert_eq!(
            head.framing().expect("admitted framing").content_length(),
            expected
        );
        assert_eq!(head.fields().as_bytes().len(), Tag::PREFIX_LEN + PATH.len());
    }
}

#[test]
fn discarded_length_required() {
    let lease = pool::Span::try_new(128).expect("lease");
    let mut session = Session::<rules::Discard>::try_new(
        lease,
        Kind::Request,
        block::Connect::Disabled,
        Scratch::default(),
    )
    .map_err(|(_, error)| error)
    .expect("discard section");
    for field in REQUEST.into_iter().chain(LENGTH_CONFLICT) {
        let name = block::Named::from_slice(field.name);
        assert!(matches!(
            session.indexed(Some(name)).expect("select field"),
            Indexed::Buffered(())
        ));
        session
            .field::<Authority>(
                name,
                block::Valued::from_span(Span::from_slice(field.value)),
                field.indexing,
            )
            .expect("validate discarded field");
    }
    let complete = session.finish().1.expect("complete section");
    assert!(complete.framing().is_none());
    assert!(matches!(&complete, Final::Rejected(_)));
    let (head, sealed, finished, _) = complete.into_parts();
    assert!(head.framing().is_none());
    assert_eq!(
        (sealed, finished),
        (Validation::Malformed, Validation::Malformed)
    );
}

#[test]
fn content_length_trailers() {
    for kind in [Kind::RequestTrailers, Kind::ResponseTrailers] {
        assert_eq!(
            Authority::validation(kind, &[LENGTH_ZERO]),
            Validation::Malformed,
            "{kind:?}"
        );
    }
}

#[test]
fn content_length_responses() {
    for &(status, code, length, expected) in LENGTH_RESPONSES {
        let lease = pool::Span::try_new(128).expect("lease");
        let mut scratch = Scratch::default();
        let mut decoder = complete::Decoder::<CompactPlan>::try_new(
            lease,
            &mut scratch,
            Kind::Response,
            block::Connect::Disabled,
        )
        .expect("response section");
        for field in [Entry::new(head::Name::Status.wire(), status), length] {
            CompactPlan::try_push(&mut decoder, field).expect("response field");
        }
        let complete = decoder.finish().expect("complete section");
        let proof = complete.framing();
        assert_eq!(proof.is_some(), expected == Validation::Valid);
        if let Some(proof) = proof {
            assert_eq!(proof.status().map(NonZero::get), Some(code));
            assert_eq!(proof.content_length(), Some(5));
        }
        let (head, _, finished, _) = complete.into_parts();
        assert_eq!(head.framing().is_some(), expected == Validation::Valid);
        assert_eq!(finished, expected, "{status:?}");
    }
}
