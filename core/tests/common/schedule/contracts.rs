pub const PROOF: &str = r#"
type Brand<'id> = region::Proof<region::Identity<'id>>;
type Slot<'a, 'id> = schedule::Occupied<'a, key::Handle, usize, Block, Brand<'id>>;
type Choice<'id> = schedule::Decision<'id, key::Handle>;
type Runtime = schedule::Runtime<key::Handle, usize, Block>;
type Typed = schedule::typed::Runtime<key::Handle, (), usize, bool, Block>;
"#;

pub const CONTRACTS: &[(&str, &str, &str)] = &[
    (
        r#"
fn contract(domain: &mut Typed, key: key::Handle) {
    if let Some(schedule::typed::Entry::Idle(entry)) = domain.entry(key) {
        entry.transition(|value| (Some(schedule::typed::Next::Data(usize::from(value), None)), ()));
    }
}
"#,
        "",
        "",
    ),
    (
        r#"
fn contract(domain: &mut Typed, key: key::Handle) {
    domain.get_mut(key);
}
"#,
        "error[E0599]: no method named `get_mut`",
        "domain.get_mut(key)",
    ),
    (
        r#"
fn contract(domain: &mut Typed, key: key::Handle) {
    domain.try_vacant(key);
}
"#,
        "error[E0596]: cannot borrow data in dereference",
        "domain.try_vacant(key)",
    ),
    (
        r#"
fn contract(domain: &mut Typed, key: key::Handle) {
    if let Some(schedule::typed::Entry::Data(mut entry)) = domain.entry(key) {
        let _: &mut bool = entry.get_mut();
    }
}
"#,
        "error[E0308]: mismatched types",
        "entry.get_mut()",
    ),
    (
        r#"
fn contract(domain: &mut Typed, key: key::Handle) {
    if let Some(schedule::typed::Entry::Idle(entry)) = domain.entry(key) {
        entry.activate(None);
    }
}
"#,
        "error[E0599]: no method named `activate`",
        "entry.activate(None)",
    ),
    (
        r#"
fn contract<'id>(driver: &mut Driver, entry: Slot<'_, 'id>) -> Choice<'id> {
    let (entry, ()) = entry.change(|value| (value, ()));
    entry.owned(driver, |_, _, _| schedule::Control::Complete)
}
"#,
        "",
        "",
    ),
    (
        r#"
fn contract<'id>(prior: &mut Option<Choice<'static>>, _: Slot<'_, 'id>) -> Choice<'id> {
    prior.take().expect("cached")
}
"#,
        "lifetime may not live long enough",
        "prior.take()",
    ),
    (
        r#"
fn contract<'id>(driver: &mut Driver, _: Slot<'_, 'id>, other: Runtime, key: key::Handle) -> Choice<'id> {
    let other: &'static mut Runtime = Box::leak(Box::new(other));
    other.entry(key).expect("foreign").borrowed(driver, |_, _, _| Ok(schedule::ControlTurn::idle()))
}
"#,
        "error[E0599]: no method named `borrowed`",
        "other.entry(key)",
    ),
    (
        r#"
fn contract<'id>(driver: &mut Driver, _: Slot<'_, 'id>, mut other: Runtime, key: key::Handle) -> Choice<'id> {
    region::Token::scope(|_| {
        other.entry(key).expect("foreign").owned(driver, |_, _, _| schedule::Control::Complete)
    })
}
"#,
        "error[E0599]: no method named `owned`",
        "other.entry(key)",
    ),
    (
        r#"
fn contract<'id>(prior: Choice<'id>) -> Choice<'id> {
    region::Token::scope(|_| {
        let _bind = Choice::bind;
        prior
    })
}
"#,
        "error[E0624]: associated function `bind` is private",
        "let _bind =",
    ),
    (
        r#"
fn contract<'id>(entry: Slot<'_, 'id>) -> Brand<'id> {
    entry.scope
}
"#,
        "error[E0616]: field `scope`",
        "entry.scope",
    ),
    (
        r#"
fn contract<'id>(driver: &mut Driver, entry: Slot<'_, 'id>) -> Choice<'id> {
    let _new = schedule::Occupied::<key::Handle, usize, Block, Brand<'id>>::new;
    entry.borrowed(driver, |_, _, _| Ok(schedule::ControlTurn::idle()))
}
"#,
        "error[E0624]: associated function `new` is private",
        "let _new =",
    ),
];
