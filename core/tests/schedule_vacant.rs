pub mod common;
use std::mem;

use common::schedule::vacant::{Blocked, Bounded as _, Handle as _};
use dope_test::alloc::Tracker;
use o3::depot::arena::slab::key::{Generation, Handle, Parts};
use sark_core::manifold::schedule::{self, Activation, Runtime};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

fn fail<T, E>(result: Result<T, E>) -> E {
    (result).err().expect("test operation must fail")
}

fn some<T>(value: Option<T>) -> T {
    value.expect("test value must be present")
}

#[test]
fn vacancy_is_zero_alloc() {
    let mut run = Runtime::<Handle, usize, Blocked>::try_new().expect("bounded runtime fixture");
    let (_, allocs) = Tracker::count(|| {
        some(run.vacant(Handle::key(0))).insert(7, None);
    });

    assert_eq!(allocs, 0);
    assert_eq!(run.get(Handle::key(0)), Some(&7));
    assert!(run.vacant(Handle::key(0)).is_none());
    assert!(run.vacant(Handle::key(1)).is_none());
}

#[test]
fn owner_survives_full() -> Result<(), &'static str> {
    let mut run: Runtime<Handle, Box<usize>, Blocked> =
        Runtime::try_new().expect("test operation must succeed");
    assert!(matches!(
        run.try_insert(Handle::key(0), Box::new(7), Activation::Keep),
        Ok(())
    ));
    let value = Box::new(11);
    let addr = (&*value) as *const usize;

    let (failure, allocs) =
        Tracker::count(|| fail(run.try_insert(Handle::key(1), value, Activation::Keep)));

    assert_eq!(allocs, 0);
    let schedule::Insert::Full {
        stream,
        value,
        used,
        limit,
    } = failure
    else {
        return Err("bounded insertion must report saturation");
    };
    assert!(stream == Handle::key(1));
    assert_eq!((&*value) as *const usize, addr);
    assert_eq!(used, 1);
    assert_eq!(limit, 1);
    Ok(())
}

#[test]
fn owner_survives_occupied() -> Result<(), &'static str> {
    let mut run: Runtime<Handle, Box<usize>, Blocked> =
        Runtime::try_new().expect("test operation must succeed");
    assert!(matches!(
        run.try_insert(Handle::key(0), Box::new(7), Activation::Keep),
        Ok(())
    ));
    let value = Box::new(13);
    let addr = (&*value) as *const usize;
    let version = Generation::MIN
        .checked_add(1)
        .expect("next generation is representable");
    let request = Handle::from_parts(Parts::new(0, version));

    let (failure, allocs) =
        Tracker::count(|| fail(run.try_insert(request, value, Activation::Keep)));

    assert_eq!(allocs, 0);
    let schedule::Insert::Occupied {
        stream,
        stored,
        value,
    } = failure
    else {
        return Err("duplicate insertion must report its occupied key");
    };
    assert!(stream == request);
    assert!(stored == Handle::key(0));
    assert_eq!((&*value) as *const usize, addr);
    Ok(())
}

#[test]
fn owner_survives_excluded() -> Result<(), &'static str> {
    let mut run: Runtime<Handle, Box<usize>, Blocked> =
        Runtime::try_new().expect("test operation must succeed");
    assert!(matches!(
        run.try_insert(Handle::key(0), Box::new(7), Activation::Keep),
        Ok(())
    ));
    let value = Box::new(17);
    let addr = (&*value) as *const usize;

    let (failure, allocs) =
        Tracker::count(|| fail(run.try_insert(Handle::key(4), value, Activation::Keep)));

    assert_eq!(allocs, 0);
    let schedule::Insert::Excluded {
        stream,
        value,
        index,
        capacity,
    } = failure
    else {
        return Err("out-of-domain insertion must report its index");
    };
    assert!(stream == Handle::key(4));
    assert_eq!((&*value) as *const usize, addr);
    assert_eq!(index, 4);
    assert_eq!(capacity, 4);
    Ok(())
}

#[test]
fn probe_keeps_empty() {
    let mut run = Runtime::<Handle, usize, Blocked>::try_new().expect("bounded runtime fixture");
    assert!(run.vacant(Handle::key(4)).is_none());
    assert!(run.vacant(Handle::key(0)).is_some());
    assert!(!run.contains(Handle::key(0)));
    assert_eq!(run.len(), 0);
}

#[test]
fn input_is_preserved() {
    let mut run = Runtime::<Handle, usize, Blocked>::try_new().expect("bounded runtime fixture");
    let absent = Box::new(11);
    let addr = (&*absent) as *const usize;
    let (result, allocs) = Tracker::count(|| {
        run.try_apply(Handle::key(0), absent, |value, input| {
            Ok::<_, ((), Box<usize>)>((Activation::Keep, (*value, input)))
        })
    });
    assert_eq!(allocs, 0);
    let (_, input) = fail(result);
    assert_eq!((&*input) as *const usize, addr);

    some(run.vacant(Handle::key(0))).insert(7, None);
    let input = Box::new(13_usize);
    let addr = (&*input) as *const usize;
    let (result, allocs) = Tracker::count(|| {
        run.try_apply(Handle::key(0), input, |value, input| {
            assert_eq!(*value, 7);
            Err::<(Activation<Blocked>, ()), _>(((), input))
        })
    });
    assert_eq!(allocs, 0);
    let (_, input) = fail(result);
    assert_eq!((&*input) as *const usize, addr);
    assert_eq!(run.get(Handle::key(0)), Some(&7));
}

#[test]
fn moves_wait_lane() {
    let mut run = Runtime::<Handle, usize, Blocked>::try_new().expect("bounded runtime fixture");
    let stream = Handle::key(0);
    (run.try_insert(stream, 7, Activation::control(Some(Blocked::Connection))))
        .as_ref()
        .expect("test operation must succeed");

    (run.transaction(stream, |_| {
        Ok::<_, ()>((Activation::control(Some(Blocked::Stream)), ()))
    }))
    .as_ref()
    .expect("test operation must succeed");

    assert_eq!(run.wake(schedule::Wake::Connection), 0);
    assert_eq!(run.wake(schedule::Wake::Stream(stream)), 1);
}

#[test]
fn insertion_schedules_data() {
    let mut run = Runtime::<Handle, usize, Blocked>::try_new().expect("bounded runtime fixture");
    let stream = Handle::key(0);
    let (result, allocs) = Tracker::count(|| run.try_insert(stream, 7, Activation::data(None)));
    (result).as_ref().expect("test operation must succeed");
    assert_eq!(allocs, 0);
    assert!(run.has_runnable());
    assert_eq!(run.remove(stream), Some(7));
    assert!(!run.has_runnable());
}

#[test]
fn reject_before_build() {
    let mut run = Runtime::<Handle, usize, Blocked>::try_new().expect("bounded runtime fixture");
    assert!(matches!(run.try_store(Handle::key(0), || 7), Ok(())));
    let mut made = 0;
    for index in [0, 1, 4] {
        let error = run
            .try_store(Handle::key(index), || {
                made += 1;
                11
            })
            .expect_err("reject the unavailable storage position");
        assert!(match error {
            schedule::Insert::Occupied {
                stream,
                stored,
                value: (),
            } => index == 0 && stream == Handle::key(0) && stored == Handle::key(0),
            schedule::Insert::Full {
                stream,
                value: (),
                used,
                limit,
            } => index == 1 && stream == Handle::key(1) && (used, limit) == (1, 1),
            schedule::Insert::Excluded {
                stream,
                value: (),
                index: actual,
                capacity,
            } => index == 4 && stream == Handle::key(4) && (actual, capacity) == (4, 4),
        });
    }
    assert_eq!(made, 0);
    assert_eq!(run.get(Handle::key(0)), Some(&7));
}

#[test]
fn store_idle_without_alloc() {
    let mut run = Runtime::<Handle, usize, Blocked>::try_new().expect("bounded runtime fixture");
    let mut made = 0;
    let (result, allocs) = Tracker::count(|| {
        run.try_store(Handle::key(0), || {
            made += 1;
            7
        })
    });
    assert!(matches!(result, Ok(())));
    assert_eq!(made, 1);
    assert_eq!(allocs, 0);
    assert!(!run.has_runnable());
    assert!(!run.has_work());
    assert_eq!(run.remove(Handle::key(0)), Some(7));
}

const _: () = assert!(
    mem::size_of::<schedule::Vacant<'static, Handle, usize, Blocked>>()
        == 3 * mem::size_of::<usize>()
);
