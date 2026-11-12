use dope::net::link::egress::data::compose;
use o3::buffer::{hold::evidence::On, resident::bound, storage};
use sark_core::life;
use sark_h1::{
    server::{payload, task::turn},
    wire::{head, output},
};
use sark_http::{owner::evidence, payload::owned::Segments};

#[test]
fn inline_copy_tracks_body() {
    let action: output::Outcome<'static> = output::Outcome::SendCopied {
        written: 128,
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
    assert!(matches!(action.copy(), output::Copy::Uncopied));
    let response = payload::write::try_outcome::<bound::Unbounded, _, _, _>(
        action,
        &mut life::mode::Disabled,
        life::exchange::Id::new((), 0),
        |conn, _| conn,
    )
    .map_err(|(cause, _)| (cause.load.charge(), cause.maximum))
    .expect("linked response");
    let (written, body, connection, _) = response.into_parts();
    assert!(matches!(
        (written, body.map(|body| body.into_inner().into_inner()), connection),
        (64, Some(compose::Either::Left(payload::Body::Shared { owner, .. })), head::Connection::KeepAlive)
            if owner.as_ref().as_ptr() == original
    ));
}
