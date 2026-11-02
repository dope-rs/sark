pub mod common;

use std::{borrow::Cow, mem};

use common::{
    field::storage::Auth,
    response::headers::{OPAQUE, PRIVATE, STANDARD},
};
use dope_test::alloc::Tracker;
use sark_http::{
    field::{
        self, Lower, Name, Value,
        head::{self, headers::Known},
        metadata::block::{self, Scan},
        storage::Static,
    },
    payload::response::headers,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn accepts_rfc_token_names() {
    for name in ["content-type", "x-request_id", "x!#$%&'*+-.^_`|~"] {
        assert!(field::Error::name(name.as_bytes()) == Ok(()));
    }
}

#[test]
fn rejects_invalid_names() {
    assert!(field::Error::name(b"") == Err(field::Error::Empty));
    assert!(
        field::Error::name(b"x bad")
            == Err(field::Error::InvalidByte {
                index: 1,
                byte: b' '
            })
    );
    assert!(
        field::Error::name(b"x:bad")
            == Err(field::Error::InvalidByte {
                index: 1,
                byte: b':'
            })
    );
}

#[test]
fn rejects_managed_names() {
    for name in [
        "date",
        "Server",
        "CONTENT-LENGTH",
        "Connection",
        "Transfer-Encoding",
        "Keep-Alive",
        "Proxy-Connection",
        "Upgrade",
        "TE",
    ] {
        let name = Name::new(Cow::Borrowed(name)).expect("field name");
        let name = field::Lower::try_name(name).expect("lowercase field name");
        assert!(matches!(
            headers::Name::new(name),
            Err(headers::Error::Managed)
        ));
    }
}

#[test]
fn validates_value_octets() {
    for byte in 0u8..=u8::MAX {
        let expected = byte == b'\t' || matches!(byte, 0x20..=0x7e | 0x80..=0xff);
        assert_eq!(
            field::Error::value(&[byte]).is_ok(),
            expected,
            "byte {byte:#04x}"
        );
    }
}

#[test]
fn preserves_header_owners() {
    let name = String::from("X-Trace");
    let name_ptr = name.as_ptr();
    let name = (Name::new(name)).expect("test operation must succeed");
    assert_eq!(name.as_str(), "X-Trace");
    assert_eq!(name.as_str().as_bytes().as_ptr(), name_ptr);

    let value = String::from("café\tready");
    let pointer = value.as_ptr();
    let value = (Value::new(value)).expect("test operation must succeed");
    assert_eq!(value.as_str(), "café\tready");
    assert_eq!(value.as_str().as_bytes().as_ptr(), pointer);

    let bytes = OPAQUE.to_vec();
    let pointer = bytes.as_ptr();
    let value = Value::new(bytes).expect("opaque header owner");
    assert_eq!(value.as_bytes(), OPAQUE);
    assert_eq!(value.as_bytes().as_ptr(), pointer);

    assert_eq!(mem::size_of::<Name<String>>(), mem::size_of::<String>());
    assert_eq!(mem::size_of::<Value<String>>(), mem::size_of::<String>());
    assert!(
        Name::new("bad name")
            == Err(field::Error::InvalidByte {
                index: 3,
                byte: b' '
            })
    );
    assert!(Value::new("nul\0value") == Err(field::Error::InvalidByte { index: 3, byte: 0 }));
}

#[test]
fn shares_request_names() {
    for known in head::Name::H1 {
        let wire = known.wire();
        assert!((head::Name::from_h1(wire)) == (Some(known)));

        let mut upper = wire.to_vec();
        upper.make_ascii_uppercase();
        assert!((head::Name::from_h1(&upper)) == (Some(known)));

        let semantic = Known::from_name(wire).expect("semantic H1 name");
        upper.push(b':');
        assert!(Known::from_prefix(&upper) == Some((semantic, wire.len())));
        for end in 0..upper.len() {
            assert!(Known::from_prefix(&upper[..end]).is_none());
        }
    }

    assert_eq!(
        head::Name::from_h1(b"kEeP-aLiVe"),
        Some(head::Name::KeepAlive)
    );
    for name in [b"".as_slice(), b"hoss", b"te", b"proxy-connection"] {
        assert_eq!(head::Name::from_h1(name), None);
    }

    for (wire, expected) in [
        (b":method".as_slice(), head::Name::Method),
        (b":path".as_slice(), head::Name::Path),
        (b"content-length".as_slice(), head::Name::ContentLength),
        (b"host".as_slice(), head::Name::Host),
        (b"te".as_slice(), head::Name::Te),
        (b"x-route".as_slice(), head::Name::Regular),
    ] {
        assert!((block::Name::classify(wire).head()) == (expected));
    }

    for name in [
        b"connection".as_slice(),
        b"keep-alive".as_slice(),
        b"proxy-connection".as_slice(),
        b"transfer-encoding".as_slice(),
        b"upgrade".as_slice(),
        b"X-UPPER".as_slice(),
    ] {
        assert!((block::Name::classify(name).head()) == (head::Name::Invalid));
    }
}

#[test]
fn mandates_h1_admission() {
    for known in [
        head::Name::Host,
        head::Name::Connection,
        head::Name::ContentLength,
        head::Name::TransferEncoding,
        head::Name::Expect,
    ] {
        assert!(
            known.is_required(),
            "{:?} changes protocol admission and cannot be route-selective",
            known.wire(),
        );
    }

    assert!(
        !head::Name::AcceptEncoding.is_required(),
        "representation negotiation may remain route-selective",
    );
}

#[test]
fn resumes_name_proof() {
    for name in [
        b"".as_slice(),
        b":method".as_slice(),
        b":protocol".as_slice(),
        b":unknown".as_slice(),
        b"host".as_slice(),
        b"content-length".as_slice(),
        b"content-lengt".as_slice(),
        b"connection".as_slice(),
        b"x-ordinary-header".as_slice(),
        b"x-late-Upper".as_slice(),
    ] {
        let expected = block::Name::classify(name);
        for split in 0..=name.len() {
            let mut scan = Scan::new();
            for &byte in &name[..split] {
                scan.push(byte);
            }
            let mut resumed = scan;
            for &byte in &name[split..] {
                resumed.push(byte);
            }
            assert!(
                (resumed.finish()) == (expected),
                "name={name:?}, split={split}"
            );
        }
    }
}

#[test]
fn round_trips_name_proof() {
    use head::Name;

    for known in [
        Name::Invalid,
        Name::Method,
        Name::Scheme,
        Name::Path,
        Name::Status,
        Name::Authority,
        Name::Protocol,
        Name::Host,
        Name::Connection,
        Name::ContentLength,
        Name::TransferEncoding,
        Name::Expect,
        Name::AcceptEncoding,
        Name::Te,
        Name::KeepAlive,
        Name::ProxyConnection,
        Name::Upgrade,
        Name::Regular,
    ] {
        let proof = block::Name::from_head(known);
        assert!((block::Name::from_compact(proof.compact())) == (Some(proof)));
    }
    assert!(block::Name::from_compact(u8::MAX).is_none());
}

#[test]
fn standard_names_borrow() {
    for standard in STANDARD {
        let upper = standard.to_ascii_uppercase();
        for bytes in [standard.as_bytes(), upper.as_bytes()] {
            let (result, usage) = Tracker::usage(|| field::Name::try_copy(bytes));
            let name = result.expect("standard header name");
            assert_eq!(name.as_str(), standard);
            assert_eq!(usage, (0, 0));
        }
    }
    let (result, count) = Tracker::count(|| field::Name::try_copy(PRIVATE.as_bytes()));
    assert_eq!(count, 1);
    assert_eq!(result.expect("private header name").as_str(), "x-private");
}

#[test]
fn field_copy_failures() {
    use field::storage::Error;

    assert!(matches!(
        Tracker::fail_after(0, || Name::try_copy(PRIVATE.as_bytes())),
        Err(Error::Allocation(o3::alloc::Exhausted))
    ));
    assert!(matches!(
        Tracker::fail_after(0, || Value::try_copy(OPAQUE)),
        Err(Error::Allocation(o3::alloc::Exhausted))
    ));
    let name = Name::new(Cow::Borrowed("X-Private")).expect("admitted name");
    assert!(matches!(
        Tracker::fail_after(0, || Lower::try_name(name)),
        Err(o3::alloc::Exhausted)
    ));
    assert!(matches!(
        Tracker::fail_after(0, || Name::try_copy(b"")),
        Err(Error::Syntax(field::Error::Empty))
    ));
}

#[test]
fn constant_zst() {
    let name = Name::new(Static::<Auth>::new()).expect("constant field name");
    assert_eq!(name.as_str(), "Authorization");
    assert_eq!(std::mem::size_of_val(&name), 0);
}
