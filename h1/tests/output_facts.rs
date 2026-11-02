use o3::buffer::{hold::evidence::On, storage};
use sark_h1::{
    server::task::turn,
    wire::{head, output},
};
use sark_http::{owner::evidence, payload::owned::Segments};

#[test]
fn inline_copy_tracks_body() {
    let action: output::Outcome<'static> = output::Outcome::SendCopied {
        written: 128,
        body_bytes: 17,
        cause: evidence::Cause::Inline,
        connection: head::Connection::KeepAlive,
    };
    assert!(matches!(
        action.retention(),
        output::Retention::Response(owner) if owner.len == 17
    ));
    assert!(matches!(
        action.copy(),
        output::Copy::Copied(evidence::Copy {
            read: 17,
            written: 17,
            cause: evidence::Cause::Inline,
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
        hdr_written: 64,
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
        hdr_written: 64,
        body,
        connection: head::Connection::KeepAlive,
    };
    assert!(matches!(
        action.retention(),
        output::Retention::Linked(owner) if owner.len == 8
    ));
    assert!(matches!(
        output::Plan::split(action),
        (output::Plan::Segments {
            hdr_written: 64,
            body,
            connection: head::Connection::KeepAlive,
        }, output::Copy::Uncopied)
            if body.segment(0).is_some_and(|bytes| bytes.as_ptr() == original)
    ));
}
