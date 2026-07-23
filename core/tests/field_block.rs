use sark_core::http::{Field, OwnedFieldBlock, PooledFieldBlock};

#[test]
fn owned_and_packed_storage_expose_the_same_fields() {
    let fields = [
        Field::new(b":method", b"POST"),
        Field::new(b"content-type", b"application/grpc"),
        Field::new(b"x-request-id", b"42"),
    ];
    let packed = PooledFieldBlock::from_fields(&fields);
    let mut owned = OwnedFieldBlock::new();
    for field in fields {
        owned.push(field.name, field.value);
    }

    assert_eq!(packed, owned);
    assert_eq!(packed.get(b"x-request-id"), Some(b"42".as_slice()));
}

#[test]
fn packed_blocks_append_without_repacking() {
    let mut headers = PooledFieldBlock::from_fields(&[Field::new(b":status", b"200")]);
    let trailers = PooledFieldBlock::from_fields(&[Field::new(b"grpc-status", b"0")]);
    headers.append(trailers).expect("second segment");

    let fields: Vec<_> = headers.iter().collect();
    assert_eq!(
        fields,
        [
            Field::new(b":status", b"200"),
            Field::new(b"grpc-status", b"0"),
        ]
    );
}
