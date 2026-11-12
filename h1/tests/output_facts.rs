use o3::buffer::{hold::evidence::On, storage};
use sark_h1::{
    server::task::turn,
    session,
    wire::{head, output},
};
use sark_http::{owner::evidence, payload::owned::Segments};

#[test]
fn inline_copy_tracks_body() {
    let action: output::Outcome<'static> = output::Outcome::SendCopied {
        copy: evidence::Copy {
            read: 23,
            written: 17,
            cause: evidence::Cause::Compressed,
        },
        connection: head::Connection::KeepAlive,
    };
    assert!(matches!(
        action.retention(),
        output::Retention::Response(owner) if owner.len == 17
    ));
    assert!(matches!(
        action.copy(),
        output::Copy::Copied(evidence::Copy {
            read: 23,
            written: 17,
            cause: evidence::Cause::Compressed,
        })
    ));
    let done = <On as turn::Mode>::complete(action.retention());
    assert!(matches!(
        done,
        turn::Completion::Response { owner, .. } if owner.len == 17
    ));
}

#[test]
fn borrow_copy_is_reported() {
    let body = storage::Shared::try_copy(b"response")
        .expect("eight response bytes must fit shared storage");
    let action: output::Outcome<'static> = output::Outcome::SendOwnedCopy {
        body,
        cause: evidence::Cause::Borrowed,
        connection: head::Connection::KeepAlive,
    };
    assert!(matches!(
        action.copy(),
        output::Copy::Copied(evidence::Copy {
            read: 8,
            written: 8,
            cause: evidence::Cause::Borrowed,
        })
    ));
}

#[test]
fn linked_uses_enum_state() {
    let body = storage::Shared::try_copy(b"response")
        .expect("eight response bytes must fit shared storage");
    let body = Segments::from(body);
    let original = body.segment(0).expect("response segment").as_ptr();
    let action: output::Outcome<'static> = output::Outcome::SendLinked {
        body,
        connection: head::Connection::KeepAlive,
    };
    assert!(matches!(
        action.retention(),
        output::Retention::Linked(owner) if owner.len == 8
    ));
    assert!(matches!(action.copy(), output::Copy::Uncopied));
    let response = action.into_consume::<()>(
        session::Permit::new(),
        session::Consumption::Buffered(session::Buffered::new(0)),
        head::Connection::KeepAlive,
    );
    assert!(matches!(
        response,
        session::Outcome::Complete {
            response: output::Complete::Linked { body },
            connection: head::Connection::KeepAlive,
            ..
        }
            if body.segment(0).expect("response segment").as_ptr() == original
    ));
}
