use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use o3::buffer::{Bytes, Retained};
use sark::service::RouteRequestImpl;
use sark_core::http::codec::RequestLine;

#[sark_gen::request(ordered)]
struct TwoQuery {
    #[query("nonce", default = "none")]
    nonce: Bytes<Retained>,
    #[query("lane", default = "none")]
    lane: Bytes<Retained>,
}

#[sark_gen::request(ordered)]
struct ThreeQuery {
    #[query("expand", default = "none")]
    expand: Bytes<Retained>,
    #[query("nonce", default = "none")]
    nonce: Bytes<Retained>,
    #[query("lane", default = "none")]
    lane: Bytes<Retained>,
}

fn fixtures() -> Vec<(&'static str, &'static [u8])> {
    vec![
        (
            "json",
            b"GET /json HTTP/1.1\r\nHost: x\r\nAccept: */*\r\n\r\n",
        ),
        ("db", b"GET /db HTTP/1.1\r\nHost: x\r\n\r\n"),
        (
            "user_param",
            b"GET /user/12345 HTTP/1.1\r\nHost: x\r\n\r\n",
        ),
        (
            "fat",
            b"GET /json HTTP/1.1\r\nHost: example.com\r\nAccept: */*\r\nAccept-Encoding: gzip\r\nUser-Agent: bench/1.0\r\nConnection: keep-alive\r\nContent-Type: application/json\r\nCache-Control: no-cache\r\nX-Request-Id: abcdef\r\nX-Forwarded-For: 10.0.0.1\r\nReferer: http://x/y\r\nCookie: a=b\r\nPragma: no-cache\r\n\r\n",
        ),
    ]
}

fn parse_line(buf: &[u8]) -> Option<(usize, usize, usize)> {
    let head = RequestLine::parse(buf).ok().flatten()?;
    Some((head.method.len(), head.target.len(), head.headers_start))
}

fn bench_head(c: &mut Criterion) {
    let mut group = c.benchmark_group("head_parse");
    for (name, buf) in fixtures() {
        group.bench_function(name, |b| {
            b.iter(|| black_box(parse_line(black_box(buf))));
        });
    }
    group.finish();

    let mut query = c.benchmark_group("query_parse");
    let two = b"nonce=74828&lane=6";
    query.bench_function("ordered_two", |b| {
        b.iter(|| {
            let mut headers = TwoQueryHeadersRaw::default();
            <TwoQuery as RouteRequestImpl>::parse_query_raw(
                &mut headers,
                black_box(two),
                0..two.len(),
            )
            .unwrap();
            black_box(headers);
        });
    });
    let three = b"expand=items&nonce=74828&lane=6";
    query.bench_function("ordered_three", |b| {
        b.iter(|| {
            let mut headers = ThreeQueryHeadersRaw::default();
            <ThreeQuery as RouteRequestImpl>::parse_query_raw(
                &mut headers,
                black_box(three),
                0..three.len(),
            )
            .unwrap();
            black_box(headers);
        });
    });
    query.finish();
}

criterion_group!(benches, bench_head);
criterion_main!(benches);
