#[derive(Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum Policy {
    #[default]
    Strict,
    Lenient,
}

const _: () = assert!(size_of::<Policy>() == 1);
