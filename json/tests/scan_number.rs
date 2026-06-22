use sark_json::Scan;

fn skip(input: &str) -> usize {
    let bytes = input.as_bytes();
    let mut idx = 0usize;
    Scan::skip_value(bytes, &mut idx).expect("skip_value should accept valid number");
    idx
}

#[test]
fn skip_integer() {
    assert_eq!(skip("123,"), 3);
}

#[test]
fn skip_negative_integer() {
    assert_eq!(skip("-42]"), 3);
}

#[test]
fn skip_fraction() {
    assert_eq!(skip("1.5,"), 3);
}

#[test]
fn skip_fraction_multi_digit() {
    assert_eq!(skip("3.14159}"), 7);
}

#[test]
fn skip_exponent_plain() {
    assert_eq!(skip("1e10,"), 4);
}

#[test]
fn skip_exponent_signed() {
    assert_eq!(skip("1E-10]"), 5);
    assert_eq!(skip("6.022e+23,"), 9);
}

#[test]
fn skip_negative_fraction_exponent() {
    let input = "-1.5e-3,";
    assert_eq!(skip(input), input.len() - 1);
}

#[test]
fn skip_stops_at_value_boundary() {
    let input = b"1.5,2.5";
    let mut idx = 0usize;
    Scan::skip_value(input, &mut idx).unwrap();
    assert_eq!(idx, 3);
    assert_eq!(input[idx], b',');
}

#[test]
fn reject_lonely_dot() {
    let input = b"1.";
    let mut idx = 0usize;
    assert!(Scan::skip_value(input, &mut idx).is_err());
}

#[test]
fn reject_lonely_exponent() {
    let input = b"1e";
    let mut idx = 0usize;
    assert!(Scan::skip_value(input, &mut idx).is_err());
}

#[test]
fn reject_exponent_sign_only() {
    let input = b"1e+";
    let mut idx = 0usize;
    assert!(Scan::skip_value(input, &mut idx).is_err());
}

#[test]
fn reject_bare_minus() {
    let input = b"-";
    let mut idx = 0usize;
    assert!(Scan::skip_value(input, &mut idx).is_err());
}
