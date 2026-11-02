use std::arch::aarch64;

use dope_core::dope_sys::thread;

use crate::payload::crc32::Backend;

/// Constructed only after the CPU admits IEEE CRC instructions.
#[derive(Clone, Debug)]
pub(super) struct Aarch64(());

impl Backend for Aarch64 {
    fn admit() -> Option<Self> {
        match <dope_core::Backend as thread::Thread>::try_crc32() {
            Ok(true) => Some(Self(())),
            Ok(false) | Err(_) => None,
        }
    }

    fn update(&self, value: u32, bytes: &[u8]) -> u32 {
        // SAFETY: this retained admission was created after detecting CRC support.
        unsafe { update(value, bytes) }
    }
}

#[target_feature(enable = "crc")]
unsafe fn update(mut value: u32, bytes: &[u8]) -> u32 {
    let (blocks, tail) = bytes.as_chunks::<64>();
    for block in blocks {
        for word in block.as_chunks::<8>().0 {
            value = aarch64::__crc32d(value, u64::from_le_bytes(*word));
        }
    }
    let (words, tail) = tail.as_chunks::<8>();
    for word in words {
        value = aarch64::__crc32d(value, u64::from_le_bytes(*word));
    }
    for byte in tail {
        value = aarch64::__crc32b(value, *byte);
    }
    value
}
