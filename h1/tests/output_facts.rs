use o3::buffer::{hold::evidence::On, storage};
use sark_h1::{
    server::{payload, task::turn},
    wire::output,
};
use sark_http::{owner::evidence, payload::owned::Segments};

#[test]
fn inline_copy_tracks_body() {
    let action: output::Complete<'static> = output::Complete::Copy {
        copy: evidence::Copy {
            read: 23,
            written: 17,
            cause: evidence::Cause::Compressed,
        },
    };
    assert!(matches!(
        action.retention(),
        output::Origin::Response(owner) if owner.len == 17
    ));
    assert!(matches!(
        action.copy(),
        Some(evidence::Copy {
            read: 23,
            written: 17,
            cause: evidence::Cause::Compressed,
        })
    ));
    let done = turn::Turn::<On>::published(turn::Readiness::Staged, Some(action.retention()));
    assert!(matches!(
        done,
        turn::Turn::Response { owner, linked: false, .. } if owner.len == 17
    ));
}

#[test]
fn borrow_copy_is_reported() {
    let body = storage::Shared::try_copy(b"response")
        .expect("eight response bytes must fit shared storage");
    let action: output::Complete<'static> = output::Complete::Owned {
        body,
        cause: evidence::Cause::Borrowed,
    };
    assert!(matches!(
        action.copy(),
        Some(evidence::Copy {
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
    let action: output::Complete<'static> = output::Complete::Linked { body };
    assert!(matches!(
        turn::Turn::<On>::published(turn::Readiness::NewlyReady, Some(action.retention())),
        turn::Turn::Response { owner, linked: true, .. } if owner.len == 8
    ));
    assert!(action.copy().is_none());
    assert!(matches!(
        action.into_body(()),
        Some(payload::Body::Shared { owner, .. }) if owner.as_ref().as_ptr() == original
    ));
}
