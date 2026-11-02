use o3::buffer::hold;

const SHARED_MIN_BYTES: usize = 4 * 1024;
pub(super) const MAX_RESIDENT_AMPLIFICATION: usize = 2;

/// One retention contract for every value entering the response resident domain.
pub(super) const RESPONSE: hold::Policy =
    hold::Policy::new(SHARED_MIN_BYTES, MAX_RESIDENT_AMPLIFICATION);
