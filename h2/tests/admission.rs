use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::{Admission, Error, config, machine::Connection},
    frame::numbers::ErrorCode,
    stream::Id,
};
use sark_http::field::{self, Entry, Pseudo, Value};

#[test]
fn preserves_full_registry() {
    let mut conn = Connection::<ClientRole>::with_config(config::Config {
        stream_capacity: 1,
        ..config::Config::default()
    })
    .expect("one resident stream");
    let fields = [Entry::pseudo(
        Pseudo::Method,
        field::literal!(Value::try_bytes(b"GET")),
    )];
    let first = conn.streams().headers().request_key(fields, Boundary::More);
    let first = first.expect("first stream admitted");
    let rejected = conn.streams().headers().request_key(fields, Boundary::More);
    let next = Id::new(3).expect("next client wire identity");
    assert!(rejected.as_ref().is_err_and(|error| matches!(
        error,
        Error::Admission(Admission::Full { stream, used, limit })
            if *stream == next && used.get() == 1 && *limit == 1
    )));
    assert!(rejected.as_ref().is_err_and(Error::is_pressure));
    assert!(
        rejected
            .as_ref()
            .is_err_and(|error| ErrorCode::from(error) == ErrorCode::RefusedStream)
    );
    assert!(conn.status().has_stream(first.id()));
    assert!(
        conn.streams().try_cancel(first, ErrorCode::Cancel).is_ok(),
        "release the original stream"
    );
    let admitted = conn.streams().headers().request_key(fields, Boundary::More);
    let admitted = admitted.expect("capacity reusable after cancellation");
    assert!((admitted.id()) == (next));
}
