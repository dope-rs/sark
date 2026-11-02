use sark_http::{
    h1::{field, framing, head, head::request::Parts, switch},
    payload::response::Method,
};

#[test]
fn upgrade_selection() {
    let choice =
        (switch::Selection::select([b"h2c, websocket".as_slice(), b"example/1"], b"WebSocket"))
            .expect("test operation must succeed");
    let conn =
        (field::Connection::default().try_push(b"upgrade")).expect("test operation must succeed");

    assert!(
        framing::Outcome::admit(
            Method::Other,
            switch::Intent::Upgrade,
            101,
            framing::Message::new(None, framing::Transfer::None),
            conn,
            Some(choice),
        ) == Ok(framing::Outcome::Switch(switch::Kind::Upgrade))
    );
    assert!(
        switch::Selection::select([b"websocket".as_slice()], b"h2c")
            == Err(switch::UpgradeError::NotOffered)
    );
    assert!(
        switch::Selection::select([b"websocket".as_slice()], b"websocket, h2c")
            == Err(switch::UpgradeError::MultipleSelections)
    );
    let wire = b"GET / HTTP/1.1\r\nHost: example.test\r\nUpgrade: websocket\r\n\r\n";
    assert!(matches!(
        head::request::Parts::scan(wire, head::Limit::DEFAULT),
        Err(head::ParseError::Semantic(head::Semantic::Transition(
            switch::Error::IncompleteUpgradeRequest
        )))
    ));
}

#[test]
fn retention_length() {
    let wire = b"GET / HTTP/1.1\r\nHost: example.test\r\nConnection: upgrade\r\nUpgrade: h2c, websocket\r\nUpgrade: chat/1\r\n\r\n";
    let mut seen = Vec::new();
    let parts = (Parts::parse(wire, head::Limit::DEFAULT, |field| {
        seen.push((field.name(), field.value()));
        Ok::<(), ()>(())
    }))
    .expect("parser operation")
    .expect("complete input");
    assert_eq!(seen.len(), 4);
    assert_eq!(seen[2].1, b"h2c, websocket");
    assert_eq!(seen[3].1, b"chat/1");
    assert!((parts.transition()) == (switch::Intent::Upgrade));
    assert!(parts.upgrades().is_present());
    assert_eq!(
        parts.upgrades().compact_len(),
        b"h2c, websocket\0chat/1".len()
    );
}

#[test]
fn http10_ignores_upgrade() {
    for wire in [
        b"GET / HTTP/1.0\r\nHost: local\r\nConnection: upgrade\r\nUpgrade: websocket\r\n\r\n"
            .as_slice(),
        b"GET / HTTP/1.0\r\nHost: local\r\nUpgrade: websocket\r\n\r\n",
        b"GET / HTTP/1.0\r\nHost: local\r\nConnection: upgrade\r\n\r\n",
        b"GET / HTTP/1.0\r\nHost: local\r\nConnection: upgrade\r\nUpgrade: web socket\r\n\r\n",
    ] {
        let head = Parts::scan(wire, head::Limit::DEFAULT)
            .expect("valid HTTP/1.0 head")
            .expect("complete head");
        assert!(head.transition() == switch::Intent::None);
        assert!(!head.upgrades().is_present());
    }

    let connect = b"CONNECT local:443 HTTP/1.0\r\nHost: local\r\nConnection: upgrade\r\nUpgrade: websocket\r\n\r\n";
    let head = Parts::scan(connect, head::Limit::DEFAULT)
        .expect("valid CONNECT head")
        .expect("complete head");
    assert!(head.transition() == switch::Intent::Connect);

    let invalid =
        b"GET / HTTP/1.1\r\nHost: local\r\nConnection: upgrade\r\nUpgrade: web socket\r\n\r\n";
    assert!(matches!(
        Parts::scan(invalid, head::Limit::DEFAULT),
        Err(head::ParseError::Semantic(head::Semantic::Upgrade(
            switch::UpgradeError::InvalidProtocol
        )))
    ));

    let bad =
        b"GET / HTTP/1.0\r\nHost: local\r\nConnection: upgrade\r\nUpgrade: web\0socket\r\n\r\n";
    assert!(matches!(
        Parts::scan(bad, head::Limit::DEFAULT),
        Err(head::ParseError::Field(field::syntax::Error::Value {
            actual: 0,
            ..
        }))
    ));
}
