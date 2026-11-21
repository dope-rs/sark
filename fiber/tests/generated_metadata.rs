pub mod common;

use std::net::{Shutdown, TcpStream};

use common::{
    route::{select::SelectiveDispatch, skip::SkipDispatch},
    server::{Plan, peer::Endpoint as _},
};

#[test]
fn selects_requested_fields() {
    Plan::serve::<SelectiveDispatch>(|addr| {
        TcpStream::connect(addr)
            .expect("connect early-close peer")
            .shutdown(Shutdown::Both)
            .expect("close early peer");
        assert_eq!(
            addr.try_exchange(b"/minimal?ignored=yes")
                .expect("minimal response")
                .get(9..12),
            Some(b"200".as_slice()),
        );
        assert!(
            addr.try_exchange(b"/items/new?tag=rust")
                .expect("literal response")
                .ends_with(b"minimal"),
            "the strict literal route must win over its parameter fallback",
        );
        for (path, body) in [
            (b"/items/4".as_slice(), b"routed".as_slice()),
            (b"/items/4?tag=rust", b"routed"),
            (b"/items/42suffix", b"minimal"),
            (b"/items/42suffix?tag=rust", b"minimal"),
            (b"/items/42suffix.", b"routed"),
            (b"/items/42suffix.gz", b"full"),
            (b"/items/42suffix.gx?tag=rust", b"routed"),
        ] {
            assert!(
                addr.try_exchange(path)
                    .expect("prefix response")
                    .ends_with(body),
                "wrong route for {path:?}",
            );
        }
        for (path, status) in [
            (b"/items/4/".as_slice(), b"404"),
            (b"/items/4/2suffix", b"404"),
            (b"/items/42?tag=rust", b"418"),
            (b"/public/items/42?tag=rust", b"418"),
            (b"/number/1", b"200"),
            (b"/number/x", b"400"),
        ] {
            assert_eq!(
                addr.try_exchange(path).expect("route response").get(9..12),
                Some(status.as_slice()),
            );
        }

        let unused = b"/minimal?ignored=\x01";
        assert_eq!(
            addr.try_exchange(unused)
                .expect("invalid selective response")
                .get(9..12),
            Some(b"400".as_slice()),
            "selective extraction must not bypass protocol-level target validation",
        );

        let full_bad = b"/full?ignored=\x01";
        assert_eq!(
            addr.try_exchange(full_bad)
                .expect("invalid full response")
                .get(9..12),
            Some(b"400".as_slice()),
        );
    });
}

#[test]
fn skip_shapes_headers() {
    Plan::serve::<SkipDispatch>(|bind| {
        let lean = String::from_utf8(bind.try_get("/lean").expect("lean response")).expect("utf8");
        assert!(lean.starts_with("HTTP/1.1 200 "), "lean status: {lean:?}");
        assert!(
            lean.contains("Content-Length: 2\r\n"),
            "lean content-length: {lean:?}"
        );
        assert!(
            lean.ends_with("\r\n\r\nok"),
            "lean body must arrive: {lean:?}"
        );
        assert!(!lean.contains("Server:"), "lean must omit Server: {lean:?}");
        assert!(!lean.contains("Date:"), "lean must omit Date: {lean:?}");
        assert!(
            lean.contains("content-type: text/plain\r\n"),
            "lean keeps content-type: {lean:?}"
        );

        let full = String::from_utf8(bind.try_get("/full").expect("full response")).expect("utf8");
        assert!(
            full.contains("Server: sark\r\n"),
            "full keeps Server: {full:?}"
        );
        assert!(full.contains("Date: "), "full keeps Date: {full:?}");
        assert!(
            !full.contains("Mon, 01 Jan 2000"),
            "full must emit the live Date: {full:?}"
        );
        assert!(
            full.ends_with("\r\n\r\nok"),
            "full body must arrive: {full:?}"
        );

        let no_srv = String::from_utf8(bind.try_get("/no-server").expect("no-server response"))
            .expect("utf8");
        assert!(
            !no_srv.contains("Server:"),
            "no-server must omit Server: {no_srv:?}"
        );
        assert!(
            no_srv.contains("Date: "),
            "no-server keeps Date: {no_srv:?}"
        );
        assert!(
            !no_srv.contains("Mon, 01 Jan 2000"),
            "no-server must emit the live Date: {no_srv:?}"
        );
        assert!(
            no_srv.ends_with("\r\n\r\nok"),
            "no-server body must arrive: {no_srv:?}"
        );

        let no_date =
            String::from_utf8(bind.try_get("/no-date").expect("no-date response")).expect("utf8");
        assert!(
            no_date.contains("Server: sark\r\n"),
            "no-date keeps Server: {no_date:?}"
        );
        assert!(
            !no_date.contains("Date:"),
            "no-date must omit Date: {no_date:?}"
        );
        assert!(
            no_date.ends_with("\r\n\r\nok"),
            "no-date body must arrive: {no_date:?}"
        );
    });
}
