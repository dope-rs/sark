pub mod common;

use common::encode::Quoted;
use o3::buffer::write;
use sark_json::writing::{TrySize, Writer};

#[test]
fn every_scalar_lane() {
    for value in ('\0'..='\u{7f}').chain([
        '\u{80}',
        '\u{7ff}',
        '\u{800}',
        '\u{ffff}',
        '\u{10000}',
        '\u{10ffff}',
    ]) {
        for offset in 0..24 {
            let quoted = Quoted::try_new(value, offset).expect("scalar lane");
            let mut output = Vec::new();
            assert!(
                Writer::new(&mut output)
                    .put_str(&quoted.input)
                    .is_continue()
            );
            assert_eq!(output, quoted.output);
            assert_eq!(quoted.input.try_size().expect("size"), output.len());
        }
    }
}

#[test]
fn preserves_prefix() {
    let mut output = [0; 6];
    let mut sink = write::Slice::new(&mut output);
    let error = Writer::new(&mut sink)
        .put_str("abcd\n")
        .break_value()
        .expect("the escape exceeds the remaining destination")
        .into_error();
    let (required, room) = error.into_parts();
    assert_eq!((required.get(), room), (2, 1));
    assert_eq!(&output[..5], b"\"abcd");
}
