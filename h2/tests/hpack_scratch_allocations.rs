pub mod common;

use common::hpack::{
    GET, INDEXED_AUTHORITY, INDEXED_FIELDS, INDEXED_NAMES, MISSING_INDEXED_NAME, bound::Literal,
};
use dope_test::alloc::Tracker;
use o3::alloc;
use sark_h2::hpack::{
    decode::{Block, Decoder, Error, limit::Admission},
    encode::{self, Encoder, Mode},
};
use sark_http::field::metadata::map::record::Entry;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn encoder_alloc_error() {
    let mut encoder = Encoder::try_new(0).expect("encoder");
    let mut output = Vec::new();
    let headers = [Entry::new(b":method", b"GET")];
    let result = Tracker::fail_after(0, || encoder.try_encode(headers, &mut output));
    assert_eq!(
        result,
        Err(encode::Error::Allocation(alloc::Error::Exhausted))
    );
    assert!(output.is_empty());
    encoder.try_encode(headers, &mut output).expect("retry");
    assert_eq!(output, [0x82]);
}

#[test]
fn encoder_alloc_rollback() {
    let fields = [
        Entry::new(b"c", b"3"),
        Entry::new(b"d", b"4"),
        Entry::new(b":method", b"GET"),
    ];
    for (capacity, consumed) in [(1, 0), (10, 2)] {
        let mut encoder = Encoder::try_new(4096).expect("encoder");
        let mut clean = Encoder::try_new(4096).expect("clean encoder");
        for encoder in [&mut encoder, &mut clean] {
            encoder.set_mode(Mode::Plain);
            encoder.set_max_size(64);
            encoder.set_max_size(128);
        }
        let mut output = Vec::with_capacity(capacity);
        output.push(0xaa);
        let mut visited = 0;
        let headers = fields.into_iter().inspect(|_| visited += 1);
        let result = Tracker::fail_after(0, || encoder.try_encode(headers, &mut output));
        assert_eq!(
            result,
            Err(encode::Error::Allocation(alloc::Error::Exhausted))
        );
        assert_eq!(output, [0xaa]);
        assert_eq!(visited, consumed);
        let mut expected = vec![0xaa];
        clean
            .try_encode(fields, &mut expected)
            .expect("clean block");
        encoder.try_encode(fields, &mut output).expect("retry");
        assert_eq!(output, expected);
    }
}

#[test]
fn plain_borrows() {
    let field = GET[3];
    let wire = Literal::new(field.name, field.value).encode_plain(false);
    let decoder = Decoder::try_new(0).expect("decoder");
    let (result, count) = Tracker::count(|| {
        decoder.decode_limit(&wire, |name, value| {
            assert_eq!(name.parts().0.as_ptr(), wire[2..].as_ptr());
            assert_eq!(
                value.parts().0.as_ptr(),
                wire[3 + field.name.len()..].as_ptr()
            );
        })
    });
    let (_, admission) = result.expect("plain field");
    assert_eq!(admission, Admission::Admitted);
    assert_eq!(count, 0);
}

#[test]
fn huffman_allocation_error() {
    let field = GET[3];
    let wire = Literal::new(field.name, field.value)
        .try_encode(false)
        .expect("literal");
    let decoder = Decoder::try_new(0).expect("decoder");
    let result = Tracker::fail_after(0, || decoder.decode_limit(&wire, |_, _| {}));
    assert!(matches!(
        result,
        Err(Error::Allocation(alloc::Error::Exhausted))
    ));
}

#[test]
fn fragment_alloc_error() {
    let field = GET[3];
    let wire = Literal::new(field.name, field.value).encode_plain(false);
    let block = Block::from(Decoder::try_new(0).expect("decoder"));
    let result = Tracker::fail_after(0, || block.decode(&wire[..3], &mut |_, _| {}));
    assert!(matches!(
        result,
        Err(Error::Allocation(alloc::Error::Exhausted))
    ));
}

#[test]
fn indexed_name_uses_owner() {
    for chunk in [INDEXED_NAMES.len(), 1] {
        let block = Block::from(Decoder::try_new(4096).expect("decoder"));
        let ((result, fields), count) = Tracker::count(|| {
            let mut block = block;
            let mut fields = 0;
            for bytes in INDEXED_NAMES.chunks(chunk) {
                block = block
                    .decode(bytes, &mut |name, value| {
                        let field = INDEXED_FIELDS[fields];
                        assert!(name.is_equal(field.name));
                        assert!(value.is_equal(field.value));
                        fields += 1;
                    })
                    .expect("indexed field");
            }
            (block.finish(), fields)
        });
        result.expect("complete block");
        assert_eq!(fields, INDEXED_FIELDS.len());
        assert_eq!(count, 0);
    }
}

#[test]
fn index_alloc_error() {
    for capacity in [0, 34] {
        for chunk in [INDEXED_AUTHORITY.len(), 1] {
            let block = Block::from(Decoder::try_new(capacity).expect("decoder"));
            let result = Tracker::fail_after(0, || {
                block.decode(&INDEXED_AUTHORITY[..chunk], &mut |_, _| {})
            });
            assert!(matches!(
                result,
                Err(Error::Allocation(alloc::Error::Exhausted))
            ));
        }
    }
}

#[test]
fn rejects_missing_name() {
    for capacity in [0, 4096] {
        for chunk in [MISSING_INDEXED_NAME.len(), 1] {
            let block = Block::from(Decoder::try_new(capacity).expect("decoder"));
            let result = Tracker::fail_after(0, || {
                block.decode(&MISSING_INDEXED_NAME[..chunk], &mut |_, _| {})
            });
            assert!(matches!(result, Err(Error::BadIndex)));
        }
    }
}
