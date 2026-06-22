pub struct Mask;

cfg_select! {
    target_arch = "aarch64" => {
        impl Mask {
            pub fn unmask_inline(buf: &mut [u8], mask: [u8; 4]) {
                // SAFETY: aarch64 baseline always includes the neon target feature.
                unsafe { unmask_neon(buf, mask) }
            }
        }

        #[target_feature(enable = "neon")]
        fn unmask_neon(buf: &mut [u8], mask: [u8; 4]) {
            use core::arch::aarch64::{vld1q_u8, vst1q_u8, veorq_u8};
            let key_arr: [u8; 16] = [
                mask[0], mask[1], mask[2], mask[3],
                mask[0], mask[1], mask[2], mask[3],
                mask[0], mask[1], mask[2], mask[3],
                mask[0], mask[1], mask[2], mask[3],
            ];
            // SAFETY: key_arr is a live 16-byte stack array.
            let key = unsafe { vld1q_u8(key_arr.as_ptr()) };
            let len = buf.len();
            let p = buf.as_mut_ptr();
            let mut i = 0;
            while i + 16 <= len {
                // SAFETY: i + 16 <= len bounds the 16-byte load/store inside buf.
                let v = unsafe { vld1q_u8(p.add(i)) };
                // SAFETY: same 16-byte window as the load above.
                unsafe { vst1q_u8(p.add(i), veorq_u8(v, key)) };
                i += 16;
            }
            unmask_scalar(&mut buf[i..], mask);
        }
    }
    _ => {
        impl Mask {
            pub fn unmask_inline(buf: &mut [u8], mask: [u8; 4]) {
                unmask_scalar(buf, mask)
            }
        }
    }
}

fn unmask_scalar(buf: &mut [u8], mask: [u8; 4]) {
    let mask_u32 = u32::from_ne_bytes(mask);
    let mut chunks = buf.chunks_exact_mut(4);
    for chunk in &mut chunks {
        let val = u32::from_ne_bytes(chunk.try_into().unwrap());
        chunk.copy_from_slice(&(val ^ mask_u32).to_ne_bytes());
    }
    for (i, byte) in chunks.into_remainder().iter_mut().enumerate() {
        *byte ^= mask[i & 3];
    }
}
