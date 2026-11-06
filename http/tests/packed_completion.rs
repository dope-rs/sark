use o3::{
    alloc,
    buffer::{
        pool,
        storage::Shared,
        write::{Sink as _, SpareExt as _},
    },
};
use sark_http::field::metadata::{
    lower::plain,
    packed,
    packed::prefix::{self, Prefix},
    section::storage::{self, Block as _, Scope as _},
};

#[test]
fn rejects_short_prefix() {
    let mut lease = pool::Span::try_new(1).expect("allocate");
    lease.try_append(&[0]).expect("capacity").commit();
    assert!(matches!(
        packed::Decoded::try_from(lease),
        Err(storage::Fault::Packed {
            offset: 0,
            cause: prefix::Error::Prefix {
                required: Prefix::LEN,
                available: 1
            }
        })
    ));
}

#[test]
fn rejects_short_payload() {
    let mut lease = pool::Span::try_new(Prefix::LEN).expect("allocate");
    lease
        .try_append(&[1, 0, 0, 0, 1, 0, 0, 0, 0])
        .expect("capacity")
        .commit();
    assert!(matches!(
        packed::Decoded::try_from(lease),
        Err(storage::Fault::Packed {
            offset: 0,
            cause: prefix::Error::Payload { .. }
        })
    ));
}

#[test]
fn commits_complete_records() {
    let lease = pool::Span::try_new(32).expect("allocate");
    let mut writer = storage::Writer::<packed::Decoded>::try_new(lease).expect("prefix");
    let rejected = writer.try_write::<plain::Error>(|output| {
        let foreign = Box::leak(
            alloc::BoxExt::try_new(pool::Span::try_new(32).expect("foreign field storage"))
                .expect("foreign field owner"),
        );
        let spare = Box::leak(alloc::BoxExt::try_new(foreign.spare()).expect("foreign spare"));
        *output = spare.bounded();
        output.write_slice(b"nv").map_err(plain::Error::Write)?;
        Ok(1)
    });
    assert!(matches!(rejected, Err(plain::Error::Layout([0, 1, 0]))));
    let rejected = writer.try_write::<plain::Error>(|output| {
        output.write_slice(b"x").map_err(plain::Error::Write)?;
        Ok(2)
    });
    assert!(matches!(rejected, Err(plain::Error::Layout([0, 2, 1]))));
    writer
        .try_write::<plain::Error>(|output| {
            output.write_slice(b"nv").map_err(plain::Error::Write)?;
            Ok(1)
        })
        .expect("complete field");
    let decoded = <packed::Decoded as storage::Block>::from(writer.finish());
    let mut fields = decoded.iter();
    let field = fields.next().expect("committed field");
    assert_eq!(
        (field.name, field.value),
        (b"n".as_slice(), b"v".as_slice())
    );
    assert!(fields.next().is_none());
}

#[test]
fn retains_prefixed_owner_through_consumption_and_freeze() {
    type Kept = packed::Decoded<pool::Span, 26>;
    let base = pool::Layout::new(1, 34).expect("header layout");
    let layout = storage::try_layout::<Kept>(base, 34).expect("field-list capacity");
    let pool = pool::Pool::try_from(layout).expect("header slot");
    for freeze in 0..4 {
        let lease = pool.acquire().expect("exclusive header slot");
        let address = lease.as_ref().as_ptr();
        let mut writer = storage::Writer::<Kept>::try_new(lease).expect("seal prefix");
        writer
            .try_write::<plain::Error>(|output| {
                output.write_slice(b"nv").map_err(plain::Error::Write)?;
                Ok(1)
            })
            .expect("field");
        let decoded = <Kept as storage::Block>::from(writer.finish());
        let (field, range) = decoded.ranges().next().expect("retained field");
        assert_eq!(range, 36..37);
        assert!(std::ptr::eq(field.value, &decoded.as_bytes()[range]));
        assert_eq!(decoded.get(b"n"), Some(b"v".as_slice()));
        let shared: Shared = match freeze {
            0 => {
                let mut lease = decoded.into_lease();
                assert_eq!(lease.as_ref().as_ptr(), address);
                lease.as_mut_slice()[36] = b'w';
                lease.freeze().into_shared()
            }
            1 => decoded.into(),
            2 => decoded.into_bytes().into(),
            _ => decoded.scope(|head| {
                let owner: o3::buffer::bytes::scope::Owner<'_, Shared> = head.into();
                owner.into_inner()
            }),
        };
        assert_eq!(shared.as_ref().as_ptr(), address);
        assert_eq!(shared.as_ref()[36], if freeze == 0 { b'w' } else { b'v' });
        assert!(pool.acquire().is_none());
        drop(shared);
        assert_eq!(pool.available(), 1);
    }
}
