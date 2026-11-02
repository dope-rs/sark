use sark_json::{Error, input::Input};

#[test]
fn skip_numbers() {
    for (input, consumed) in [
        ("123,", 3),
        ("-42]", 3),
        ("1.5,2.5", 3),
        ("3.14159}", 7),
        ("1e10,", 4),
        ("1E-10]", 5),
        ("6.022e+23,", 9),
        ("-1.5e-3,", 7),
        ("0.01e+02,", 8),
    ] {
        let mut index = 0;
        input.as_bytes().skip_value(&mut index).expect("number");
        assert_eq!(index, consumed, "{input}");
    }
}

#[test]
fn reject_invalid_numbers() {
    for input in ["01,", "-01,", "1.", "1e", "1e+", "-"] {
        let mut index = 0;
        std::assert_matches!(input.as_bytes().skip_value(&mut index), Err(Error::Invalid));
        assert_eq!(index, 0, "{input}");
    }
}

#[test]
fn rejects_unsigned_zero() {
    let mut index = 1;
    std::assert_matches!(
        b" 01,".as_slice().parse_u64(&mut index),
        Err(Error::Invalid)
    );
    assert_eq!(index, 1);
}
