pub mod common;

use common::hpack::rfc;

#[test]
fn request_plain() {
    rfc::requests([
        (
            "RFC 7541 C.3.1 request decodes",
            b"\x82\x86\x84\x41\x0fwww.example.com",
        ),
        (
            "RFC 7541 C.3.2 request decodes",
            b"\x82\x86\x84\xbe\x58\x08no-cache",
        ),
        (
            "RFC 7541 C.3.3 request decodes",
            b"\x82\x87\x85\xbf\x40\x0acustom-key\x0ccustom-value",
        ),
    ]);
}

#[test]
fn request_huffman() {
    rfc::requests([
        (
            "RFC 7541 C.4.1 request decodes",
            b"\x82\x86\x84\x41\x8c\xf1\xe3\xc2\xe5\xf2\x3a\x6b\xa0\xab\x90\xf4\xff",
        ),
        (
            "RFC 7541 C.4.2 request decodes",
            b"\x82\x86\x84\xbe\x58\x86\xa8\xeb\x10\x64\x9c\xbf",
        ),
        (
            "RFC 7541 C.4.3 request decodes",
            b"\x82\x87\x85\xbf\x40\x88\x25\xa8\x49\xe9\x5b\xa9\x7d\x7f\x89\x25\xa8\
              \x49\xe9\x5b\xb8\xe8\xb4\xbf",
        ),
    ]);
}

#[test]
fn response_plain() {
    rfc::responses([
        (
            "RFC 7541 C.5.1 response decodes",
            b"\x48\x03302\x58\x07private\x61\x1dMon, 21 Oct 2013 20:13:21 GMT\x6e\
              \x17https://www.example.com",
        ),
        (
            "RFC 7541 C.5.2 response decodes",
            b"\x48\x03307\xc1\xc0\xbf",
        ),
        (
            "RFC 7541 C.5.3 response decodes",
            b"\x88\xc1\x61\x1dMon, 21 Oct 2013 20:13:22 GMT\xc0\x5a\x04gzip\x77\
              \x38foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1",
        ),
    ]);
}

#[test]
fn response_huffman() {
    rfc::responses([
        (
            "RFC 7541 C.6.1 response decodes",
            b"\x48\x82\x64\x02\x58\x85\xae\xc3\x77\x1a\x4b\x61\x96\xd0\x7a\xbe\x94\
              \x10\x54\xd4\x44\xa8\x20\x05\x95\x04\x0b\x81\x66\xe0\x82\xa6\x2d\x1b\
              \xff\x6e\x91\x9d\x29\xad\x17\x18\x63\xc7\x8f\x0b\x97\xc8\xe9\xae\x82\
              \xae\x43\xd3",
        ),
        (
            "RFC 7541 C.6.2 response decodes",
            b"\x48\x83\x64\x0e\xff\xc1\xc0\xbf",
        ),
        (
            "RFC 7541 C.6.3 response decodes",
            b"\x88\xc1\x61\x96\xd0\x7a\xbe\x94\x10\x54\xd4\x44\xa8\x20\x05\x95\x04\
              \x0b\x81\x66\xe0\x84\xa6\x2d\x1b\xff\xc0\x5a\x83\x9b\xd9\xab\x77\xad\
              \x94\xe7\x82\x1d\xd7\xf2\xe6\xc7\xb3\x35\xdf\xdf\xcd\x5b\x39\x60\xd5\
              \xaf\x27\x08\x7f\x36\x72\xc1\xab\x27\x0f\xb5\x29\x1f\x95\x87\x31\x60\
              \x65\xc0\x03\xed\x4e\xe5\xb1\x06\x3d\x50\x07",
        ),
    ]);
}
