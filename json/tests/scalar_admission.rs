use sark_json::{Error, input::Input};

#[test]
fn scalar_boundaries() {
    let input = b"18446744073709551615,true,false]";
    let mut index = 0;
    assert_eq!(input.parse_u64(&mut index).ok(), Some(u64::MAX));
    assert_eq!(index, 20);
    index += 1;
    assert_eq!(input.parse_bool(&mut index).ok(), Some(true));
    assert_eq!(index, 25);
    index += 1;
    assert_eq!(input.parse_bool(&mut index).ok(), Some(false));
    assert_eq!(index, 31);
}

#[test]
fn scalar_rejection() {
    for input in [
        b" 18446744073709551616000000000000000000000,".as_slice(),
        b" -123456789012345678901234567890,",
        b" 1.23456789012345678901234567890,",
        b" 1e123456789012345678901234567890,",
    ] {
        let mut index = 1;
        assert!(matches!(input.parse_u64(&mut index), Err(Error::Invalid)));
        assert_eq!(index, 1);
        assert!(matches!(input.parse_bool(&mut index), Err(Error::Invalid)));
        assert_eq!(index, 1);
        assert!(input.skip_value(&mut index).is_ok());
        assert_eq!(index, input.len() - 1);
    }
    let mut index = 1;
    assert!(matches!(
        b" truex,".as_slice().parse_bool(&mut index),
        Err(Error::Invalid)
    ));
    assert_eq!(index, 1);
}
