use sark_core::manifold::client::io::{self, Reject};
use sark_h2::conn::machine::Connection;

#[test]
fn prepared_owner_rollback() {
    use o3::buffer::{pool, storage};
    use sark_core::manifold::{
        client::{
            Connection as _,
            io::{Prepared as _, Write as _},
        },
        stream::Boundary,
    };
    use sark_h2::ClientRole;
    use sark_http::field::{self, Entry, Pseudo, Value};

    let mut client = Connection::<ClientRole>::try_new().expect("H2 client allocation");
    client.drain(usize::MAX);
    let stream = client
        .start(
            [
                Entry::pseudo(Pseudo::Method, field::literal!(Value::try_bytes(b"POST"))),
                Entry::pseudo(Pseudo::Scheme, field::literal!(Value::try_bytes(b"http"))),
                Entry::pseudo(
                    Pseudo::Path,
                    field::literal!(Value::try_bytes(b"/svc/Method")),
                ),
                Entry::pseudo(
                    Pseudo::Authority,
                    field::literal!(Value::try_bytes(b"localhost")),
                ),
            ],
            Boundary::More,
        )
        .expect("request headers");
    let expected = vec![b'y'; 4096];
    let body = storage::Shared::try_copy(&expected).expect("retained payload");
    let sent = client
        .send_owned(stream, b"head!", &body, 0, Boundary::End)
        .expect("retained protobuf request");
    assert!(matches!(sent, io::Outcome::Written(n) if n == expected.len() + 5));

    let before = client.outbound_len();
    for width in [1, 64 * 1024] {
        let cursors = pool::Pool::try_new(1, width).expect("one bounded prefix slot");
        let prefix = cursors.cursor().expect("the fresh pool owns its cursor");
        let prepared = client.prepare(prefix);
        let prepared = prepared
            .ok()
            .expect("queued request must retain its prefix owner");
        drop(prepared);
        assert_eq!(cursors.available(), 1);
        assert_eq!(client.outbound_len(), before);
        let prefix = cursors.cursor().expect("the cancelled prefix was returned");
        let prepared = client.prepare(prefix);
        let prepared = prepared
            .ok()
            .expect("queued request must retain its prefix owner");
        let offered = prepared.try_bind();
        let offered = offered.expect("the prefix binds its rollback owner");
        drop(offered);
        assert_eq!(cursors.available(), 1);
        assert_eq!(client.outbound_len(), before);
    }

    let mut first = [0];
    assert!(
        matches!(client.take(&mut first), Ok(io::Output::Buffered(1))),
        "one byte of header progress"
    );
    assert_eq!(client.outbound_len(), before - 1);
    client.drain(usize::MAX);
    let retained = client.outbound_len();
    let cursors = pool::Pool::try_new(1, 1).expect("one short prefix slot");
    let prefix = cursors.cursor().expect("the prefix is available");
    let rejected = client
        .prepare(prefix)
        .err()
        .expect("one byte cannot hold a DATA header");
    assert!(matches!(
        rejected,
        Reject::Failed {
            cause: io::Error::Capacity(_),
            ..
        }
    ));
    if let Reject::Failed {
        cause: io::Error::Capacity(cause),
        prefix,
    } = rejected
    {
        let (required, room) = cause.into_parts();
        assert!(required.get() >= 9);
        assert_eq!(room, 1);
        assert!(prefix.is_empty());
        drop(prefix);
    }
    assert_eq!(cursors.available(), 1);
    assert_eq!(client.outbound_len(), retained);

    let mut prefix = [0; 64 * 1024];
    let write = client.take(&mut prefix);
    assert!(
        matches!(write, Ok(io::Output::Split { .. })),
        "the retained request remains queued"
    );
    if let Ok(io::Output::Split { body, .. }) = write {
        assert_eq!(body.as_slice(), expected);
    }
    assert_eq!(client.outbound_len(), 0);
}
