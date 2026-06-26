use sark_core::http::head::header_lines;

fn collect(wire: &[u8]) -> Vec<(String, String)> {
    header_lines(wire)
        .map(|(n, v)| {
            (
                String::from_utf8_lossy(n).into_owned(),
                String::from_utf8_lossy(v).into_owned(),
            )
        })
        .collect()
}

#[test]
fn parses_crlf_and_trims_ows() {
    assert_eq!(
        collect(b"Host: x\r\nUpgrade:   websocket  \r\n\r\n"),
        vec![
            ("Host".into(), "x".into()),
            ("Upgrade".into(), "websocket".into()),
        ]
    );
}

#[test]
fn tolerates_bare_lf() {
    assert_eq!(
        collect(b"a: 1\nb: 2\n\n"),
        vec![("a".into(), "1".into()), ("b".into(), "2".into())]
    );
}

#[test]
fn stops_at_blank_line_and_ignores_body() {
    assert_eq!(
        collect(b"a: 1\r\n\r\nPOSTish body: not-a-header\r\n"),
        vec![("a".into(), "1".into())]
    );
}

#[test]
fn skips_lines_without_colon_and_keeps_empty_value() {
    assert_eq!(
        collect(b"garbage line\r\nx:\r\ny: a:b\r\n\r\n"),
        vec![("x".into(), "".into()), ("y".into(), "a:b".into())]
    );
}
