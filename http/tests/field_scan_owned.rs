use sark_http::h1::field::scan::{Progress, State};

#[test]
fn copy_retains_progress() {
    let state = State::try_new(b"X: a\r\nPar".to_vec(), 0, 0, 2, 32).expect("source");
    let mut names = Vec::new();
    let progress = state
        .advance(|field| {
            names.push(field.name().to_vec());
            Ok::<_, ()>(())
        })
        .expect("first field");
    let Progress::More(state) = progress else {
        assert!(matches!(progress, Progress::More(_)), "partial name");
        return;
    };
    let mut copied = state.try_copy(Vec::new()).expect("copy");
    copied.try_extend(b"tial: b\r\n\r\nNEXT").expect("append");
    let progress = copied
        .advance(|field| {
            names.push(field.name().to_vec());
            Ok::<_, ()>(())
        })
        .expect("remaining field");
    let Progress::Complete(complete) = progress else {
        assert!(matches!(progress, Progress::Complete(_)), "complete fields");
        return;
    };
    assert_eq!(names, [b"X".as_slice(), b"Partial"]);
    let (source, fields, wire) = complete.into_parts();
    assert_eq!(&source[fields..wire.get() as usize], b"\r\n");
    assert_eq!(&source[wire.get() as usize..], b"NEXT");
}

#[test]
fn seeded_field_count() {
    for limit in [0, 1, u32::MAX as usize] {
        let state =
            State::try_new(b"X: a\r\nY: b\r\n\r\n".to_vec(), 6, limit, limit, 32).expect("seed");
        let error = state
            .advance(|_| Ok::<_, ()>(()))
            .err()
            .expect("field limit")
            .1;
        assert!(matches!(
            error,
            sark_http::h1::field::scan::Error::Syntax(sark_http::h1::field::syntax::Error::TooMany)
        ));
    }
    let limit = u32::MAX as usize;
    let state = State::try_new(b"X: a\r\n\r\n".to_vec(), 0, limit - 1, limit, 32).expect("seed");
    let mut fields = 0;
    let progress = state
        .advance(|_| {
            fields += 1;
            Ok::<_, ()>(())
        })
        .expect("last field");
    assert!(matches!(progress, Progress::Complete(_)));
    assert_eq!(fields, 1);
}
