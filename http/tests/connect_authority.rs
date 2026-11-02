pub mod common;

use common::{
    connect::{AUTHORITIES, HOST, METHOD, PATH, PROTOCOL, SCHEME},
    response::headers::Admit as _,
};
use sark_http::{
    field::{
        head::Name,
        metadata::{
            block::{Connect, Form, Gate, Kind, Plan, Validation, Version},
            map::record::Span,
        },
    },
    uri::Authority,
};

#[test]
fn requires_nonempty_port() {
    for version in [Version::Two, Version::Three] {
        for &(value, expected) in AUTHORITIES {
            for split in 0..=value.len() {
                let (first, second) = value.split_at(split);
                let mut fields = [
                    (Name::Method, Span::from_slice(METHOD)),
                    (Name::Authority, Span::new(first, second)),
                ];
                for _ in 0..2 {
                    let mut gate = Gate::new(Kind::Request, Connect::Disabled, version);
                    for (name, value) in fields {
                        let check = gate.accept_name(name);
                        assert_eq!(check, Validation::Valid);
                        assert_eq!(gate.accept_value(name, value), Validation::Valid);
                    }
                    let (plan, check) = gate.finish().into_parts();
                    assert_eq!(check, expected);
                    assert_eq!(
                        plan.map(Plan::form),
                        (expected == Validation::Valid).then_some(Form::Connect),
                    );
                    fields.reverse();
                }
            }
        }
    }
}

#[test]
fn preserves_other_forms() {
    for &(value, _) in AUTHORITIES {
        Authority::parse(value).expect("generic authority syntax");
        for version in [Version::Two, Version::Three] {
            let mut fields = [
                (Name::Method, METHOD),
                (Name::Authority, value),
                (Name::Scheme, SCHEME),
                (Name::Path, PATH),
                (Name::Protocol, PROTOCOL),
            ];
            for _ in 0..2 {
                let (plan, check) =
                    Gate::new(Kind::Request, Connect::Enabled, version).fields(&fields);
                assert_eq!(check, Validation::Valid);
                assert_eq!(plan.map(Plan::form), Some(Form::ExtendedConnect));
                fields.reverse();
            }
        }
    }
}

#[test]
fn host_port_does_not_admit() {
    for version in [Version::Two, Version::Three] {
        for &(value, expected) in AUTHORITIES {
            if expected == Validation::Valid {
                continue;
            }
            let (plan, check) = Gate::new(Kind::Request, Connect::Disabled, version).fields(&[
                (Name::Authority, value),
                (Name::Method, METHOD),
                (Name::Host, HOST),
            ]);
            assert_eq!(check, Validation::Malformed);
            assert!(plan.is_none());
        }
    }
}
