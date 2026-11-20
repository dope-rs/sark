use sark_h2::hpack::decode::{Decoder, limit::Admission};

fn section(
    decoder: Decoder,
    (example, wire): (&str, &[u8]),
    fields: &[(&[u8], &[u8])],
    table: (usize, usize),
) -> Decoder {
    let mut remaining = fields.iter().enumerate();
    let mut mismatch = None;
    let (decoder, admission) = decoder
        .decode_limit(wire, |name, value| match remaining.next() {
            Some((_, &(n, v))) if name.is_equal(n) && value.is_equal(v) => {}
            field => {
                mismatch.get_or_insert(field.map_or(fields.len(), |(index, _)| index));
            }
        })
        .expect(example);
    assert_eq!(admission, Admission::Admitted, "{example}");
    assert_eq!(mismatch, None, "{example} first unexpected field index");
    assert_eq!(remaining.len(), 0, "{example} missing fields");
    assert_eq!((decoder.dyn_len(), decoder.dyn_size()), table, "{example}");
    decoder
}

pub fn requests([first, second, third]: [(&str, &[u8]); 3]) {
    let base: [(&[u8], &[u8]); 4] = [
        (b":method", b"GET"),
        (b":scheme", b"http"),
        (b":path", b"/"),
        (b":authority", b"www.example.com"),
    ];
    let [method, scheme, path, authority] = base;
    let decoder = Decoder::try_new(4096).expect("HPACK decoder capacity");
    let decoder = section(decoder, first, &base, (1, 57));
    let (name, value) = decoder.dyn_get(0).expect(first.0);
    assert!(name.is_equal(authority.0), "{} dynamic name", first.0);
    assert!(value.is_equal(authority.1), "{} dynamic value", first.0);
    let decoder = section(
        decoder,
        second,
        &[
            method,
            scheme,
            path,
            authority,
            (b"cache-control", b"no-cache"),
        ],
        (2, 110),
    );
    section(
        decoder,
        third,
        &[
            method,
            (b":scheme", b"https"),
            (b":path", b"/index.html"),
            authority,
            (b"custom-key", b"custom-value"),
        ],
        (3, 164),
    );
}

pub fn responses([first, second, third]: [(&str, &[u8]); 3]) {
    let base: [(&[u8], &[u8]); 4] = [
        (b":status", b"302"),
        (b"cache-control", b"private"),
        (b"date", b"Mon, 21 Oct 2013 20:13:21 GMT"),
        (b"location", b"https://www.example.com"),
    ];
    let [_, cache, date, location] = base;
    let decoder = Decoder::try_new(256).expect("HPACK decoder capacity");
    let decoder = section(decoder, first, &base, (4, 222));
    let decoder = section(
        decoder,
        second,
        &[(b":status", b"307"), cache, date, location],
        (4, 222),
    );
    section(
        decoder,
        third,
        &[
            (b":status", b"200"),
            cache,
            (b"date", b"Mon, 21 Oct 2013 20:13:22 GMT"),
            location,
            (b"content-encoding", b"gzip"),
            (
                b"set-cookie",
                b"foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1",
            ),
        ],
        (3, 215),
    );
}
