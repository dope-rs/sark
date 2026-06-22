use sark_h2::hpack::{Decoder, DecoderError, Encoder, Header};

type Pair = (Vec<u8>, Vec<u8>);

struct Helper;

impl Helper {
    fn collect(dec: &mut Decoder, buf: &[u8]) -> Result<Vec<Pair>, DecoderError> {
        let mut out: Vec<Pair> = Vec::new();
        dec.decode(buf, |n, v| out.push((n.to_vec(), v.to_vec())))?;
        Ok(out)
    }
}

mod c2_examples {
    use super::*;

    #[test]
    fn c2_1_literal_with_indexing() {
        let bytes: &[u8] = &[
            0x40, 0x0a, b'c', b'u', b's', b't', b'o', b'm', b'-', b'k', b'e', b'y', 0x0d, b'c',
            b'u', b's', b't', b'o', b'm', b'-', b'h', b'e', b'a', b'd', b'e', b'r',
        ];
        let mut dec = Decoder::new(4096);
        let got = Helper::collect(&mut dec, bytes).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, b"custom-key");
        assert_eq!(got[0].1, b"custom-header");
        assert_eq!(dec.dyn_size(), 55);
        assert_eq!(dec.dyn_len(), 1);
        let (n, v) = dec.dyn_get(0).unwrap();
        assert_eq!(n, b"custom-key");
        assert_eq!(v, b"custom-header");
    }

    #[test]
    fn c2_2_literal_without_indexing() {
        let bytes: &[u8] = &[
            0x04, 0x0c, b'/', b's', b'a', b'm', b'p', b'l', b'e', b'/', b'p', b'a', b't', b'h',
        ];
        let mut dec = Decoder::new(4096);
        let got = Helper::collect(&mut dec, bytes).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, b":path");
        assert_eq!(got[0].1, b"/sample/path");
        assert_eq!(dec.dyn_size(), 0);
        assert_eq!(dec.dyn_len(), 0);
    }

    #[test]
    fn c2_3_literal_never_indexed() {
        let bytes: &[u8] = &[
            0x10, 0x08, b'p', b'a', b's', b's', b'w', b'o', b'r', b'd', 0x06, b's', b'e', b'c',
            b'r', b'e', b't',
        ];
        let mut dec = Decoder::new(4096);
        let got = Helper::collect(&mut dec, bytes).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, b"password");
        assert_eq!(got[0].1, b"secret");
        assert_eq!(dec.dyn_size(), 0);
    }

    #[test]
    fn c2_4_indexed_header() {
        let bytes: &[u8] = &[0x82];
        let mut dec = Decoder::new(4096);
        let got = Helper::collect(&mut dec, bytes).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, b":method");
        assert_eq!(got[0].1, b"GET");
    }
}

mod c3_request_seq_no_huffman {
    use super::*;

    #[test]
    fn c3_full_three_requests() {
        let mut dec = Decoder::new(4096);

        let r1: &[u8] = &[
            0x82, 0x86, 0x84, 0x41, 0x0f, b'w', b'w', b'w', b'.', b'e', b'x', b'a', b'm', b'p',
            b'l', b'e', b'.', b'c', b'o', b'm',
        ];
        let got = Helper::collect(&mut dec, r1).unwrap();
        assert_eq!(
            got,
            vec![
                (b":method".to_vec(), b"GET".to_vec()),
                (b":scheme".to_vec(), b"http".to_vec()),
                (b":path".to_vec(), b"/".to_vec()),
                (b":authority".to_vec(), b"www.example.com".to_vec()),
            ]
        );
        assert_eq!(dec.dyn_len(), 1);
        assert_eq!(dec.dyn_size(), 57);
        let (n, v) = dec.dyn_get(0).unwrap();
        assert_eq!(n, b":authority");
        assert_eq!(v, b"www.example.com");

        let r2: &[u8] = &[
            0x82, 0x86, 0x84, 0xbe, 0x58, 0x08, b'n', b'o', b'-', b'c', b'a', b'c', b'h', b'e',
        ];
        let got = Helper::collect(&mut dec, r2).unwrap();
        assert_eq!(
            got,
            vec![
                (b":method".to_vec(), b"GET".to_vec()),
                (b":scheme".to_vec(), b"http".to_vec()),
                (b":path".to_vec(), b"/".to_vec()),
                (b":authority".to_vec(), b"www.example.com".to_vec()),
                (b"cache-control".to_vec(), b"no-cache".to_vec()),
            ]
        );
        assert_eq!(dec.dyn_len(), 2);
        assert_eq!(dec.dyn_size(), 110);

        let r3: &[u8] = &[
            0x82, 0x87, 0x85, 0xbf, 0x40, 0x0a, b'c', b'u', b's', b't', b'o', b'm', b'-', b'k',
            b'e', b'y', 0x0c, b'c', b'u', b's', b't', b'o', b'm', b'-', b'v', b'a', b'l', b'u',
            b'e',
        ];
        let got = Helper::collect(&mut dec, r3).unwrap();
        assert_eq!(
            got,
            vec![
                (b":method".to_vec(), b"GET".to_vec()),
                (b":scheme".to_vec(), b"https".to_vec()),
                (b":path".to_vec(), b"/index.html".to_vec()),
                (b":authority".to_vec(), b"www.example.com".to_vec()),
                (b"custom-key".to_vec(), b"custom-value".to_vec()),
            ]
        );
        assert_eq!(dec.dyn_len(), 3);
        assert_eq!(dec.dyn_size(), 164);
    }
}

mod c4_request_seq_huffman {
    use super::*;

    #[test]
    fn c4_full_three_requests() {
        let mut dec = Decoder::new(4096);

        let r1: &[u8] = &[
            0x82, 0x86, 0x84, 0x41, 0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab,
            0x90, 0xf4, 0xff,
        ];
        let got = Helper::collect(&mut dec, r1).unwrap();
        assert_eq!(
            got,
            vec![
                (b":method".to_vec(), b"GET".to_vec()),
                (b":scheme".to_vec(), b"http".to_vec()),
                (b":path".to_vec(), b"/".to_vec()),
                (b":authority".to_vec(), b"www.example.com".to_vec()),
            ]
        );
        assert_eq!(dec.dyn_size(), 57);

        let r2: &[u8] = &[
            0x82, 0x86, 0x84, 0xbe, 0x58, 0x86, 0xa8, 0xeb, 0x10, 0x64, 0x9c, 0xbf,
        ];
        let got = Helper::collect(&mut dec, r2).unwrap();
        assert_eq!(
            got,
            vec![
                (b":method".to_vec(), b"GET".to_vec()),
                (b":scheme".to_vec(), b"http".to_vec()),
                (b":path".to_vec(), b"/".to_vec()),
                (b":authority".to_vec(), b"www.example.com".to_vec()),
                (b"cache-control".to_vec(), b"no-cache".to_vec()),
            ]
        );
        assert_eq!(dec.dyn_size(), 110);

        let r3: &[u8] = &[
            0x82, 0x87, 0x85, 0xbf, 0x40, 0x88, 0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xa9, 0x7d, 0x7f,
            0x89, 0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xb8, 0xe8, 0xb4, 0xbf,
        ];
        let got = Helper::collect(&mut dec, r3).unwrap();
        assert_eq!(
            got,
            vec![
                (b":method".to_vec(), b"GET".to_vec()),
                (b":scheme".to_vec(), b"https".to_vec()),
                (b":path".to_vec(), b"/index.html".to_vec()),
                (b":authority".to_vec(), b"www.example.com".to_vec()),
                (b"custom-key".to_vec(), b"custom-value".to_vec()),
            ]
        );
        assert_eq!(dec.dyn_size(), 164);
    }
}

mod c5_response_seq_no_huffman {
    use super::*;

    #[test]
    fn c5_eviction_size_256() {
        let mut dec = Decoder::new(256);

        let r1: &[u8] = &[
            0x48, 0x03, b'3', b'0', b'2', 0x58, 0x07, b'p', b'r', b'i', b'v', b'a', b't', b'e',
            0x61, 0x1d, b'M', b'o', b'n', b',', b' ', b'2', b'1', b' ', b'O', b'c', b't', b' ',
            b'2', b'0', b'1', b'3', b' ', b'2', b'0', b':', b'1', b'3', b':', b'2', b'1', b' ',
            b'G', b'M', b'T', 0x6e, 0x17, b'h', b't', b't', b'p', b's', b':', b'/', b'/', b'w',
            b'w', b'w', b'.', b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c', b'o', b'm',
        ];
        let got = Helper::collect(&mut dec, r1).unwrap();
        assert_eq!(
            got,
            vec![
                (b":status".to_vec(), b"302".to_vec()),
                (b"cache-control".to_vec(), b"private".to_vec()),
                (b"date".to_vec(), b"Mon, 21 Oct 2013 20:13:21 GMT".to_vec()),
                (b"location".to_vec(), b"https://www.example.com".to_vec()),
            ]
        );
        assert_eq!(dec.dyn_size(), 222);
        assert_eq!(dec.dyn_len(), 4);

        let r2: &[u8] = &[0x48, 0x03, b'3', b'0', b'7', 0xc1, 0xc0, 0xbf];
        let got = Helper::collect(&mut dec, r2).unwrap();
        assert_eq!(
            got,
            vec![
                (b":status".to_vec(), b"307".to_vec()),
                (b"cache-control".to_vec(), b"private".to_vec()),
                (b"date".to_vec(), b"Mon, 21 Oct 2013 20:13:21 GMT".to_vec()),
                (b"location".to_vec(), b"https://www.example.com".to_vec()),
            ]
        );
        assert_eq!(dec.dyn_size(), 222);
        assert_eq!(dec.dyn_len(), 4);

        let r3: &[u8] = &[
            0x88, 0xc1, 0x61, 0x1d, b'M', b'o', b'n', b',', b' ', b'2', b'1', b' ', b'O', b'c',
            b't', b' ', b'2', b'0', b'1', b'3', b' ', b'2', b'0', b':', b'1', b'3', b':', b'2',
            b'2', b' ', b'G', b'M', b'T', 0xc0, 0x5a, 0x04, b'g', b'z', b'i', b'p', 0x77, 0x38,
            b'f', b'o', b'o', b'=', b'A', b'S', b'D', b'J', b'K', b'H', b'Q', b'K', b'B', b'Z',
            b'X', b'O', b'Q', b'W', b'E', b'O', b'P', b'I', b'U', b'A', b'X', b'Q', b'W', b'E',
            b'O', b'I', b'U', b';', b' ', b'm', b'a', b'x', b'-', b'a', b'g', b'e', b'=', b'3',
            b'6', b'0', b'0', b';', b' ', b'v', b'e', b'r', b's', b'i', b'o', b'n', b'=', b'1',
        ];
        let got = Helper::collect(&mut dec, r3).unwrap();
        assert_eq!(
            got,
            vec![
                (b":status".to_vec(), b"200".to_vec()),
                (b"cache-control".to_vec(), b"private".to_vec()),
                (b"date".to_vec(), b"Mon, 21 Oct 2013 20:13:22 GMT".to_vec()),
                (b"location".to_vec(), b"https://www.example.com".to_vec()),
                (b"content-encoding".to_vec(), b"gzip".to_vec()),
                (
                    b"set-cookie".to_vec(),
                    b"foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1".to_vec()
                ),
            ]
        );
        assert_eq!(dec.dyn_size(), 215);
        assert_eq!(dec.dyn_len(), 3);
    }
}

mod c6_response_seq_huffman {
    use super::*;

    #[test]
    fn c6_eviction_size_256_huffman() {
        let mut dec = Decoder::new(256);

        let r1: &[u8] = &[
            0x48, 0x82, 0x64, 0x02, 0x58, 0x85, 0xae, 0xc3, 0x77, 0x1a, 0x4b, 0x61, 0x96, 0xd0,
            0x7a, 0xbe, 0x94, 0x10, 0x54, 0xd4, 0x44, 0xa8, 0x20, 0x05, 0x95, 0x04, 0x0b, 0x81,
            0x66, 0xe0, 0x82, 0xa6, 0x2d, 0x1b, 0xff, 0x6e, 0x91, 0x9d, 0x29, 0xad, 0x17, 0x18,
            0x63, 0xc7, 0x8f, 0x0b, 0x97, 0xc8, 0xe9, 0xae, 0x82, 0xae, 0x43, 0xd3,
        ];
        let got = Helper::collect(&mut dec, r1).unwrap();
        assert_eq!(
            got,
            vec![
                (b":status".to_vec(), b"302".to_vec()),
                (b"cache-control".to_vec(), b"private".to_vec()),
                (b"date".to_vec(), b"Mon, 21 Oct 2013 20:13:21 GMT".to_vec()),
                (b"location".to_vec(), b"https://www.example.com".to_vec()),
            ]
        );
        assert_eq!(dec.dyn_size(), 222);

        let r2: &[u8] = &[0x48, 0x83, 0x64, 0x0e, 0xff, 0xc1, 0xc0, 0xbf];
        let got = Helper::collect(&mut dec, r2).unwrap();
        assert_eq!(
            got,
            vec![
                (b":status".to_vec(), b"307".to_vec()),
                (b"cache-control".to_vec(), b"private".to_vec()),
                (b"date".to_vec(), b"Mon, 21 Oct 2013 20:13:21 GMT".to_vec()),
                (b"location".to_vec(), b"https://www.example.com".to_vec()),
            ]
        );
        assert_eq!(dec.dyn_size(), 222);

        let r3: &[u8] = &[
            0x88, 0xc1, 0x61, 0x96, 0xd0, 0x7a, 0xbe, 0x94, 0x10, 0x54, 0xd4, 0x44, 0xa8, 0x20,
            0x05, 0x95, 0x04, 0x0b, 0x81, 0x66, 0xe0, 0x84, 0xa6, 0x2d, 0x1b, 0xff, 0xc0, 0x5a,
            0x83, 0x9b, 0xd9, 0xab, 0x77, 0xad, 0x94, 0xe7, 0x82, 0x1d, 0xd7, 0xf2, 0xe6, 0xc7,
            0xb3, 0x35, 0xdf, 0xdf, 0xcd, 0x5b, 0x39, 0x60, 0xd5, 0xaf, 0x27, 0x08, 0x7f, 0x36,
            0x72, 0xc1, 0xab, 0x27, 0x0f, 0xb5, 0x29, 0x1f, 0x95, 0x87, 0x31, 0x60, 0x65, 0xc0,
            0x03, 0xed, 0x4e, 0xe5, 0xb1, 0x06, 0x3d, 0x50, 0x07,
        ];
        let got = Helper::collect(&mut dec, r3).unwrap();
        assert_eq!(
            got,
            vec![
                (b":status".to_vec(), b"200".to_vec()),
                (b"cache-control".to_vec(), b"private".to_vec()),
                (b"date".to_vec(), b"Mon, 21 Oct 2013 20:13:22 GMT".to_vec()),
                (b"location".to_vec(), b"https://www.example.com".to_vec()),
                (b"content-encoding".to_vec(), b"gzip".to_vec()),
                (
                    b"set-cookie".to_vec(),
                    b"foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1".to_vec()
                ),
            ]
        );
        assert_eq!(dec.dyn_size(), 215);
    }
}

mod roundtrip {
    use super::*;

    #[test]
    fn enc_dec_simple() {
        let mut enc = Encoder::new(4096);
        let mut dec = Decoder::new(4096);
        let headers = [
            Header {
                name: b":method",
                value: b"POST",
            },
            Header {
                name: b":path",
                value: b"/api/v1/x",
            },
            Header {
                name: b":scheme",
                value: b"https",
            },
            Header {
                name: b":authority",
                value: b"example.com",
            },
            Header {
                name: b"content-type",
                value: b"application/json",
            },
            Header {
                name: b"accept",
                value: b"text/plain",
            },
        ];
        let mut buf = Vec::new();
        enc.encode(headers.iter().copied(), &mut buf);
        let got = Helper::collect(&mut dec, &buf).unwrap();
        let expected: Vec<(Vec<u8>, Vec<u8>)> = headers
            .iter()
            .map(|h| (h.name.to_vec(), h.value.to_vec()))
            .collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn enc_dec_no_huffman() {
        let mut enc = Encoder::new(4096);
        enc.set_huffman(false);
        let mut dec = Decoder::new(4096);
        let headers = [Header {
            name: b"x-custom",
            value: b"hello world",
        }];
        let mut buf = Vec::new();
        enc.encode(headers.iter().copied(), &mut buf);
        let got = Helper::collect(&mut dec, &buf).unwrap();
        assert_eq!(got, vec![(b"x-custom".to_vec(), b"hello world".to_vec())]);
    }

    #[test]
    fn enc_dec_many_rounds_builds_dyn() {
        let mut enc = Encoder::new(4096);
        let mut dec = Decoder::new(4096);
        for _ in 0..5 {
            let headers = [
                Header {
                    name: b":method",
                    value: b"GET",
                },
                Header {
                    name: b"user-agent",
                    value: b"sark/1.0",
                },
            ];
            let mut buf = Vec::new();
            enc.encode(headers.iter().copied(), &mut buf);
            let got = Helper::collect(&mut dec, &buf).unwrap();
            assert_eq!(got.len(), 2);
            assert_eq!(got[0].0, b":method");
            assert_eq!(got[1].0, b"user-agent");
            assert_eq!(got[1].1, b"sark/1.0");
        }
        assert!(dec.dyn_len() >= 1);
    }
}

mod eviction {
    use super::*;

    #[test]
    fn eviction_basic() {
        let mut dec = Decoder::new(80);
        let bytes: &[u8] = &[
            0x40, 0x03, b'a', b'a', b'a', 0x03, b'A', b'A', b'A', 0x40, 0x03, b'b', b'b', b'b',
            0x03, b'B', b'B', b'B',
        ];
        let got = Helper::collect(&mut dec, bytes).unwrap();
        assert_eq!(
            got,
            vec![
                (b"aaa".to_vec(), b"AAA".to_vec()),
                (b"bbb".to_vec(), b"BBB".to_vec()),
            ]
        );
        assert_eq!(dec.dyn_len(), 2);

        let more: &[u8] = &[0x40, 0x03, b'c', b'c', b'c', 0x03, b'C', b'C', b'C'];
        let got = Helper::collect(&mut dec, more).unwrap();
        assert_eq!(got, vec![(b"ccc".to_vec(), b"CCC".to_vec())]);
        assert_eq!(dec.dyn_len(), 2);
        let (n0, _) = dec.dyn_get(0).unwrap();
        let (n1, _) = dec.dyn_get(1).unwrap();
        assert_eq!(n0, b"ccc");
        assert_eq!(n1, b"bbb");
    }

    #[test]
    fn dyn_size_update_clears_table() {
        let mut dec = Decoder::new(4096);
        let bytes: &[u8] = &[0x40, 0x03, b'a', b'a', b'a', 0x03, b'A', b'A', b'A'];
        Helper::collect(&mut dec, bytes).unwrap();
        assert_eq!(dec.dyn_len(), 1);
        let upd: &[u8] = &[0x20];
        Helper::collect(&mut dec, upd).unwrap();
        assert_eq!(dec.dyn_len(), 0);
        assert_eq!(dec.dyn_max(), 0);
    }
}

mod errors {
    use super::*;

    #[test]
    fn bad_index_zero() {
        let mut dec = Decoder::new(4096);
        let bytes: &[u8] = &[0x80];
        let err = Helper::collect(&mut dec, bytes).unwrap_err();
        assert_eq!(err, DecoderError::BadIndex);
    }

    #[test]
    fn bad_index_out_of_range() {
        let mut dec = Decoder::new(4096);
        let bytes: &[u8] = &[0xff, 0x7f];
        let err = Helper::collect(&mut dec, bytes).unwrap_err();
        assert_eq!(err, DecoderError::BadIndex);
    }

    #[test]
    fn bad_dyn_size_update_too_large() {
        let mut dec = Decoder::new(256);
        let bytes: &[u8] = &[0x3f, 0xe1, 0x1f];
        let err = Helper::collect(&mut dec, bytes).unwrap_err();
        assert_eq!(err, DecoderError::BadDynSizeUpdate);
    }

    #[test]
    fn need_more_truncated_string() {
        let mut dec = Decoder::new(4096);
        let bytes: &[u8] = &[0x40, 0x0a, b'c', b'u', b's', b't', b'o', b'm'];
        let err = Helper::collect(&mut dec, bytes).unwrap_err();
        assert_eq!(err, DecoderError::NeedMore);
    }

    #[test]
    fn integer_overflow_detected() {
        let mut dec = Decoder::new(4096);
        let mut bytes = vec![0xff; 13];
        bytes.push(0x7f);
        let err = Helper::collect(&mut dec, &bytes).unwrap_err();
        assert_eq!(err, DecoderError::BadInteger);
    }
}

mod size_setting {
    use super::*;

    #[test]
    fn encoder_set_max_emits_update() {
        let mut enc = Encoder::new(4096);
        enc.set_max_size(1024);
        let mut buf = Vec::new();
        enc.encode(std::iter::empty::<Header>(), &mut buf);
        assert_eq!(buf[0] & 0xe0, 0x20);
    }
}

mod header_list_bound {
    use super::*;

    #[test]
    fn running_total_rejects_hpack_bomb() {
        let mut enc = Encoder::new(4096);
        let big_value = vec![b'a'; 4000];
        let header = Header {
            name: b"x-bomb",
            value: &big_value,
        };
        let mut block = Vec::new();
        enc.encode_one(header, &mut block);
        let indexed_ref: u8 = 0x80 | 62;
        for _ in 0..100 {
            block.push(indexed_ref);
        }

        let mut dec = Decoder::new(4096);
        dec.set_max_header_list_size(Some(8192));
        let mut emitted = 0usize;
        let over = dec.decode_bounded(&block, |_, _| emitted += 1).unwrap();
        assert!(over);
        assert!(emitted < 101);
    }

    #[test]
    fn within_limit_decodes() {
        let mut enc = Encoder::new(4096);
        let mut block = Vec::new();
        enc.encode_one(
            Header {
                name: b"a",
                value: b"b",
            },
            &mut block,
        );
        let mut dec = Decoder::new(4096);
        dec.set_max_header_list_size(Some(8192));
        let mut emitted = 0usize;
        let over = dec.decode_bounded(&block, |_, _| emitted += 1).unwrap();
        assert!(!over);
        assert_eq!(emitted, 1);
    }
}
