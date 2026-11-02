use std::mem;

use sark_core::manifold::stream::Boundary;
use sark_h2::{
    frame::{
        Ack,
        controls::{Priority, RstStream, Settings},
        layout,
        layout::{Data, Sharing},
        numbers::{ErrorCode, Increment, Length},
    },
    stream::Id,
};

#[test]
fn rejects_domain_bounds() {
    assert_eq!(mem::size_of::<Length>(), mem::size_of::<u32>());
    assert_eq!(mem::size_of::<Increment>(), mem::size_of::<u32>());
    assert_eq!(mem::size_of::<Option<Increment>>(), mem::size_of::<u32>());
    assert_eq!(
        Length::new(Length::MAX).map(Length::into_u32),
        Some(Length::MAX)
    );
    assert!((Length::new(Length::MAX + 1)).is_none());
    assert_eq!(Id::new(Id::MAX).map(Id::into_u32), Some(Id::MAX));
    assert!((Id::new(Id::MAX + 1)).is_none());
    assert!((Increment::new(0)).is_none());
    assert_eq!(
        Increment::new(Increment::MAX).map(Increment::get),
        Some(Increment::MAX)
    );
    assert!((Increment::new(Increment::MAX + 1)).is_none());
}

#[test]
fn rejects_invalid_streams() {
    let dep = layout::Priority {
        sharing: Sharing::Shared,
        dependency: Id::new(3).expect("fixture domain"),
        weight: 0,
    };
    assert!((Data::new(Id::CONNECTION, Boundary::More, b"")).is_none());
    assert!((Priority::new(Id::new(3).expect("fixture domain"), dep)).is_none());
    assert!((RstStream::new(Id::CONNECTION, ErrorCode::Cancel)).is_none());
    assert!((Settings::new(Ack::Complete, &[0; 6])).is_none());
}
