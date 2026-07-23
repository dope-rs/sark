use sark_core::http::{Field, OwnedFieldBlock, PooledFieldBlock, VecFieldBlock};

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

#[test]
fn generated_parts_are_written_directly_and_rollback_on_error() {
    let mut fields = VecFieldBlock::new();
    fields
        .try_push_parts::<()>(
            |name| {
                name.extend_from_slice(b"x-generated");
                Ok(())
            },
            |value, _| {
                value.extend_from_slice(b"direct");
                Ok(())
            },
        )
        .unwrap();

    let error = fields.try_push_parts(
        |name| {
            name.extend_from_slice(b"x-broken");
            Ok(())
        },
        |value, _| {
            value.extend_from_slice(b"partial");
            Err("decode failed")
        },
    );

    assert_eq!(error, Err("decode failed"));
    assert_eq!(
        fields.iter().collect::<Vec<_>>(),
        [Field::new(b"x-generated", b"direct")]
    );
    assert_eq!(
        fields
            .iter_with_value_ranges()
            .map(|(field, range)| (field, &fields.as_bytes()[range]))
            .collect::<Vec<_>>(),
        [(Field::new(b"x-generated", b"direct"), b"direct".as_slice())]
    );
}
