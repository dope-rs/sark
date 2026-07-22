use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use sark::dispatch::Ctx;
use sark::framer::FusedHead;
use sark::service::Key;
use sark_core::http::codec::ParsedRequestHead;

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

fn baseline_pipeline(buf: &[u8]) -> Option<(Key, usize, usize)> {
    let head = ParsedRequestHead::parse(buf)?;
    let ctx = Ctx::parse(buf, &head);
    Some((
        ctx.method_key,
        ctx.slice_path.bytes().len(),
        head.headers_start,
    ))
}

fn fused_pipeline(buf: &[u8]) -> Option<(Key, usize, usize)> {
    let fused = FusedHead::parse(buf)?;
    let ctx = Ctx::parse_with_key(buf, &fused.head, fused.method_key);
    Some((
        ctx.method_key,
        ctx.slice_path.bytes().len(),
        fused.head.headers_start,
    ))
}

fn bench_head(c: &mut Criterion) {
    let mut group = c.benchmark_group("head_parse");
    for (name, buf) in fixtures() {
        group.bench_function(format!("baseline/{name}"), |b| {
            b.iter(|| black_box(baseline_pipeline(black_box(buf))));
        });
        group.bench_function(format!("fused/{name}"), |b| {
            b.iter(|| black_box(fused_pipeline(black_box(buf))));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_head);
criterion_main!(benches);
