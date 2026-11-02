use sark_http::h1::codec::numbers::{Error, Wire as _};

#[test]
fn writes_exact_region() {
    for value in [0usize, 9, 10, 99, 100, usize::MAX] {
        let mut out = [0xa5; usize::BITS as usize];
        let end = value.try_write(&mut out, 3).expect("decimal fits").get();
        assert_eq!(&out[3..end], value.to_string().as_bytes());
        assert!(out[..3].iter().chain(&out[end..]).all(|byte| *byte == 0xa5));
    }
}

#[test]
fn rejects_short_region() {
    let mut out = [0xa5; 3];
    assert!(matches!(
        123usize.try_write(&mut out, 1),
        Err(Error::Capacity { required, actual: 3 }) if required.get() == 4
    ));
    assert_eq!(out, [0xa5; 3]);
}

#[test]
fn rejects_offset_overflow() {
    let mut out = [0xa5; 3];
    assert!(matches!(
        0usize.try_write(&mut out, usize::MAX),
        Err(Error::Overflow { current: usize::MAX, added }) if added.get() == 1
    ));
    assert_eq!(out, [0xa5; 3]);
}
