use o3::buffer::write;
use sark_json::writing::{TryEncode, TrySize, Writer};

#[test]
fn decimal_boundaries() {
    let values = (0..=19)
        .flat_map(|exponent| {
            let value = 10u64.pow(exponent);
            [value - 1, value, value + 1]
        })
        .chain([u64::MAX]);
    for value in values {
        let output = value.try_encode().expect("encode");
        assert_eq!(output, value.to_string().as_bytes());
    }
}

#[test]
fn signed_minimum() {
    let mut output = Vec::new();
    assert!(Writer::new(&mut output).put_i64(i64::MIN).is_continue());
    assert_eq!(output, i64::MIN.to_string().as_bytes());
    assert_eq!(i64::MIN.try_size().expect("size"), output.len());
}

#[test]
fn usize_maximum() {
    let output = usize::MAX.try_encode().expect("encode");
    assert_eq!(output, usize::MAX.to_string().as_bytes());
}

#[test]
fn decimal_failure() {
    let mut output = [0; 9];
    let mut sink = write::Slice::new(&mut output);
    let failure = 9_876_543_210u64
        .try_write(Writer::new(&mut sink))
        .break_value()
        .expect("reject the complete decimal write");
    let (required, room) = failure.into_error().into_parts();
    assert_eq!((required.get(), room), (10, 9));
}
