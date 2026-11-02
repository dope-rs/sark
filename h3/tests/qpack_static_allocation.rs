use dope_test::alloc::Tracker;
use sark_h3::qpack::encoder::Encoder;
use sark_http::field::Entry;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn skips_static_journal() {
    let (encoder, allocs) = Tracker::usage(Encoder::new);
    assert_eq!(allocs, (0, 0));
    for mut encoder in [
        encoder,
        Encoder::try_bounded(0, 1).expect("bounded encoder"),
    ] {
        let mut output = Vec::with_capacity(2);
        let (result, allocs) =
            Tracker::usage(|| encoder.encode(0, [] as [Entry<'_>; 0], &mut output));
        result.expect("static section");
        assert_eq!(allocs, (0, 0));
        assert_eq!(output, [0, 0]);
    }
}
