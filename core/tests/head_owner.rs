use std::ptr;

use dope_test::alloc::Tracker;
use o3::buffer::pool;
use sark_core::request::Head;
use sark_http::{
    field::metadata::{map, map::record::Entry, packed, section::storage},
    request::{self, Form, Layout, Target, span::Span},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn borrows_owner() {
    let mut fields = map::Fields::new();
    fields.push(b":path", b"/items").expect("path");
    fields
        .push(b":authority", b"example.test")
        .expect("authority");
    let mut lease = pool::Span::try_new(fields.as_bytes().len()).expect("packed owner");
    lease
        .try_append(fields.as_bytes())
        .expect("packed fields")
        .commit();
    let source = <packed::Decoded as storage::Block>::from(
        storage::Finished::try_from(lease).expect("valid packed fixture"),
    );
    let mut ranges = source.ranges();
    let path = Span::from_range(ranges.next().expect("path range").1).expect("path span");
    let peer = Span::from_range(ranges.next().expect("peer range").1).expect("peer span");
    let start = path.start();
    let layout = (|| {
        let target = Target::from_span(Form::Path, path)?;
        Ok::<_, request::Error>(Layout::new(target, Some(peer), Some(path)))
    })();
    let layout = layout.expect("test operation must succeed");
    let head = Head::<_>::bind(source, layout);
    let head = head.expect("test operation must succeed");

    head.scope(|head| {
        let ((path, peer, mut fields), activity) = Tracker::measure(|| {
            let view = head.borrow();
            assert!(view.form() == Form::Path);
            (view.path(), view.authority(), head.owner().iter())
        });
        assert_eq!(activity, [0; 4]);
        assert_eq!(path, b"/items");
        assert!(ptr::eq(path.as_ptr(), head.as_ref()[start..].as_ptr()));
        assert_eq!(peer, Some(b"example.test".as_slice()));
        assert!(fields.next() == Some(Entry::new(b":path", b"/items")));
        assert!(fields.next() == Some(Entry::new(b":authority", b"example.test")));
        assert!(fields.next().is_none());
    });
}
