#[derive(Clone, Copy)]
#[repr(u8)]
pub(super) enum End {
    More,
    Last,
}

const _: () = assert!(core::mem::size_of::<End>() == core::mem::size_of::<bool>());
const _: () = assert!(core::mem::align_of::<End>() == core::mem::align_of::<bool>());

#[derive(Clone, Copy)]
pub(super) struct Part {
    value: usize,
}

impl Part {
    pub(super) fn try_new(len: usize, end: End) -> Option<Self> {
        let value = len.checked_mul(2)? | end as usize;
        Some(Self { value })
    }

    pub(super) fn encoded_len(self) -> usize {
        let mut value = self.value;
        let mut len = 1;
        while value >= 0x80 {
            value >>= 7;
            len += 1;
        }
        len
    }

    pub(super) fn write(self, output: &mut [u8]) -> Option<usize> {
        let mut value = self.value;
        let mut index = 0;
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            *output.get_mut(index)? = byte | u8::from(value != 0) << 7;
            index += 1;
            if value == 0 {
                return Some(index);
            }
        }
    }
}
