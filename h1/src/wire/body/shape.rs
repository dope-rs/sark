#[derive(Clone, Copy)]
#[repr(u8)]
pub(super) enum Shape {
    Flat,
    Segmented,
}

const _: () = assert!(size_of::<Shape>() == size_of::<bool>());
const _: () = assert!(align_of::<Shape>() == align_of::<bool>());
