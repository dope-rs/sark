use sark_http::preface::{CLIENT_PREFACE, Prefix, Read};

#[test]
fn carries_only_the_admitted_prefix() {
    for split in 0..CLIENT_PREFACE.len() {
        let (count, Read::Pending(prefix)) = Prefix::default()
            .read(&CLIENT_PREFACE[..split])
            .expect("prefix")
        else {
            panic!("incomplete prefix");
        };
        assert_eq!(count, split);
        assert_eq!(prefix.as_ref(), &CLIENT_PREFACE[..split]);
        let mut suffix = CLIENT_PREFACE[split..].to_vec();
        suffix.push(0);
        let (count, Read::Complete(_)) = prefix.read(&suffix).expect("complete") else {
            panic!("complete preface");
        };
        assert_eq!(
            count,
            suffix.len() - 1,
            "next protocol input stays unconsumed"
        );
    }
}

#[test]
fn mismatch_retains_prior_input_and_exact_failure() {
    let (_, Read::Pending(prefix)) = Prefix::default().read(b"P").expect("prefix") else {
        panic!("partial preface");
    };
    let (prefix, offset, expected, actual) = prefix.read(b"RX").expect_err("mismatch").into_parts();
    assert_eq!(
        prefix.as_ref(),
        b"P",
        "the current input remains with its caller"
    );
    assert_eq!((offset, expected, actual), (2, b'I', b'X'));
}
