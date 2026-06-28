use sark_core::http::compress::Brotli;

fn sample_json() -> String {
    let mut json = String::from("[");
    for i in 0..25 {
        if i > 0 {
            json.push(',');
        }
        json.push_str(&format!(
            r#"{{"id":{i},"name":"item-{i}","score":{},"active":true,"tags":["alpha","beta","gamma"]}}"#,
            i * 7 % 100
        ));
    }
    json.push(']');
    json
}

/// The thread-local encoder must compress, stay deterministic across calls
/// (the reused buffer is cleared, not appended), and produce valid brotli that
/// decodes back to the input.
#[test]
fn brotli_round_trips_through_thread_local() {
    let body = sample_json();
    let raw = body.as_bytes();

    let first = Brotli::with_thread_local(|b| b.encode(raw).to_vec());
    let second = Brotli::with_thread_local(|b| b.encode(raw).to_vec());
    assert_eq!(first, second, "buffer reuse must be deterministic");
    assert!(
        first.len() < raw.len(),
        "must compress: {} >= {}",
        first.len(),
        raw.len()
    );

    let mut decoded = Vec::new();
    let mut reader = first.as_slice();
    brotli::BrotliDecompress(&mut reader, &mut decoded).expect("decode");
    assert_eq!(decoded, raw, "round-trip mismatch");
}
