use o3::{
    alloc,
    buffer::{
        pool,
        write::{Sink as _, SpareExt as _},
    },
};
use sark_http::field::metadata::{
    lower::plain,
    packed,
    packed::prefix::{self, Prefix},
    section::storage,
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
