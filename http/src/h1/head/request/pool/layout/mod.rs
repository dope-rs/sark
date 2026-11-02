use crate::h1::switch;

mod mode;
use mode::Mode;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::h1::head::request) enum Source {
    Compact,
    Head,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(in crate::h1::head::request) struct Layout {
    bytes: [u8; 7],
    mode: Mode,
}

impl Layout {
    pub(in crate::h1::head::request) const BYTES: usize = size_of::<Self>();
    pub(in crate::h1::head::request) const MAX: usize = (1 << 13) - 1;

    pub(in crate::h1::head::request) fn new(
        words: [u16; 4],
        source: Source,
        intent: switch::Intent,
    ) -> Self {
        let bits = u64::from(words[0])
            | u64::from(words[1]) << 13
            | u64::from(words[2]) << 26
            | u64::from(words[3]) << 39;
        let bytes = bits.to_le_bytes();
        Self {
            bytes: [
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
            ],
            mode: Mode::new(source, intent),
        }
    }

    pub(in crate::h1::head::request) fn encode(self) -> [u8; Self::BYTES] {
        let [a, b, c, d, e, f, g] = self.bytes;
        [a, b, c, d, e, f, g, self.mode as u8]
    }

    pub(in crate::h1::head::request) fn method_end(self) -> usize {
        self.bits(0)
    }
    pub(in crate::h1::head::request) fn target_start(self) -> usize {
        self.bits(13)
    }
    pub(in crate::h1::head::request) fn target_end(self) -> usize {
        self.bits(26)
    }
    pub(in crate::h1::head::request) fn head_end(self) -> usize {
        self.bits(39)
    }

    pub(in crate::h1::head::request) const fn source(self) -> Source {
        self.mode.source()
    }
    pub(in crate::h1::head::request) const fn transition(self) -> switch::Intent {
        self.mode.transition()
    }

    fn bits(self, shift: u32) -> usize {
        let [a, b, c, d, e, f, g] = self.bytes;
        ((u64::from_le_bytes([a, b, c, d, e, f, g, 0]) >> shift) as usize) & Self::MAX
    }
}

const _: () = assert!(size_of::<Layout>() == size_of::<u64>());
