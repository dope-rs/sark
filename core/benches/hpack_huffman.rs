use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use sark_core::http::{HpackHuffmanEncoded, HpackHuffmanSource};

const COMMON: &[u8] = b"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
const LARGE: &[u8] = concat!(
    ":methodGET:path/index.html:schemehttps:authoritywww.example.com",
    "accepttext/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-encodinggzip, deflate, braccept-languageen-US,en;q=0.9",
    "cache-controlmax-age=0content-typeapplication/jsoncontent-length12345",
    "user-agentMozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    ":methodPOST:path/api/v1/messages:schemehttps:authorityapi.example.com",
    "authorizationBearer abcdefghijklmnopqrstuvwxyz0123456789",
    "content-typeapplication/grpc+protogrpc-encodinggzipte trailers",
    "x-request-id01234567-89ab-cdef-0123-456789abcdef",
    "traceparent00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
    ":status200serverSarkdateThu, 31 Jul 2026 00:00:00 GMT",
    "content-typeapplication/octet-streamcache-controlno-store",
    "varyaccept-encodingetag\"0123456789abcdef\"",
    "strict-transport-securitymax-age=31536000; includeSubDomains",
    "permissions-policycamera=(), microphone=(), geolocation=()",
    "content-security-policydefault-src 'none'; frame-ancestors 'none'",
)
.as_bytes();

fn encoded(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    HpackHuffmanSource::new(input).encode(&mut out);
    out
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hpack_huffman_decode");
    for (name, raw) in [("common", COMMON), ("large", LARGE)] {
        let encoded = encoded(raw);
        group.throughput(Throughput::Bytes(raw.len() as u64));
        group.bench_function(name, |b| {
            b.iter(|| {
                let mut decoded = 0usize;
                HpackHuffmanEncoded::new(black_box(&encoded))
                    .decode_with(|byte| {
                        black_box(byte);
                        decoded += 1;
                        Ok::<_, core::convert::Infallible>(())
                    })
                    .expect("benchmark fixture must decode");
                black_box(decoded)
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
