use core::convert::Infallible;

#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
/// Bytes before HPACK/QPACK Huffman encoding.
pub struct HpackHuffmanSource<'a>(&'a [u8]);

#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
/// Bytes carrying an HPACK/QPACK Huffman bit stream.
pub struct HpackHuffmanEncoded<'a>(&'a [u8]);

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
/// An invalid Huffman code, EOS symbol, or padding suffix.
pub struct HpackHuffmanError;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
/// Separates malformed input from a failure in the decoded-byte sink.
pub enum HpackHuffmanDecodeError<E> {
    InvalidEncoding(HpackHuffmanError),
    Sink(E),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Incremental HPACK/QPACK Huffman decoder state.
pub struct HpackHuffmanDecoder {
    state: u8,
    maybe_eos: bool,
}

/// RFC 7541 Appendix B canonical codes, including EOS at index 256.
pub const CODES: [(u32, u8); 257] = [
    (0x1ff8, 13),
    (0x7fffd8, 23),
    (0xfffffe2, 28),
    (0xfffffe3, 28),
    (0xfffffe4, 28),
    (0xfffffe5, 28),
    (0xfffffe6, 28),
    (0xfffffe7, 28),
    (0xfffffe8, 28),
    (0xffffea, 24),
    (0x3ffffffc, 30),
    (0xfffffe9, 28),
    (0xfffffea, 28),
    (0x3ffffffd, 30),
    (0xfffffeb, 28),
    (0xfffffec, 28),
    (0xfffffed, 28),
    (0xfffffee, 28),
    (0xfffffef, 28),
    (0xffffff0, 28),
    (0xffffff1, 28),
    (0xffffff2, 28),
    (0x3ffffffe, 30),
    (0xffffff3, 28),
    (0xffffff4, 28),
    (0xffffff5, 28),
    (0xffffff6, 28),
    (0xffffff7, 28),
    (0xffffff8, 28),
    (0xffffff9, 28),
    (0xffffffa, 28),
    (0xffffffb, 28),
    (0x14, 6),
    (0x3f8, 10),
    (0x3f9, 10),
    (0xffa, 12),
    (0x1ff9, 13),
    (0x15, 6),
    (0xf8, 8),
    (0x7fa, 11),
    (0x3fa, 10),
    (0x3fb, 10),
    (0xf9, 8),
    (0x7fb, 11),
    (0xfa, 8),
    (0x16, 6),
    (0x17, 6),
    (0x18, 6),
    (0x0, 5),
    (0x1, 5),
    (0x2, 5),
    (0x19, 6),
    (0x1a, 6),
    (0x1b, 6),
    (0x1c, 6),
    (0x1d, 6),
    (0x1e, 6),
    (0x1f, 6),
    (0x5c, 7),
    (0xfb, 8),
    (0x7ffc, 15),
    (0x20, 6),
    (0xffb, 12),
    (0x3fc, 10),
    (0x1ffa, 13),
    (0x21, 6),
    (0x5d, 7),
    (0x5e, 7),
    (0x5f, 7),
    (0x60, 7),
    (0x61, 7),
    (0x62, 7),
    (0x63, 7),
    (0x64, 7),
    (0x65, 7),
    (0x66, 7),
    (0x67, 7),
    (0x68, 7),
    (0x69, 7),
    (0x6a, 7),
    (0x6b, 7),
    (0x6c, 7),
    (0x6d, 7),
    (0x6e, 7),
    (0x6f, 7),
    (0x70, 7),
    (0x71, 7),
    (0x72, 7),
    (0xfc, 8),
    (0x73, 7),
    (0xfd, 8),
    (0x1ffb, 13),
    (0x7fff0, 19),
    (0x1ffc, 13),
    (0x3ffc, 14),
    (0x22, 6),
    (0x7ffd, 15),
    (0x3, 5),
    (0x23, 6),
    (0x4, 5),
    (0x24, 6),
    (0x5, 5),
    (0x25, 6),
    (0x26, 6),
    (0x27, 6),
    (0x6, 5),
    (0x74, 7),
    (0x75, 7),
    (0x28, 6),
    (0x29, 6),
    (0x2a, 6),
    (0x7, 5),
    (0x2b, 6),
    (0x76, 7),
    (0x2c, 6),
    (0x8, 5),
    (0x9, 5),
    (0x2d, 6),
    (0x77, 7),
    (0x78, 7),
    (0x79, 7),
    (0x7a, 7),
    (0x7b, 7),
    (0x7ffe, 15),
    (0x7fc, 11),
    (0x3ffd, 14),
    (0x1ffd, 13),
    (0xffffffc, 28),
    (0xfffe6, 20),
    (0x3fffd2, 22),
    (0xfffe7, 20),
    (0xfffe8, 20),
    (0x3fffd3, 22),
    (0x3fffd4, 22),
    (0x3fffd5, 22),
    (0x7fffd9, 23),
    (0x3fffd6, 22),
    (0x7fffda, 23),
    (0x7fffdb, 23),
    (0x7fffdc, 23),
    (0x7fffdd, 23),
    (0x7fffde, 23),
    (0xffffeb, 24),
    (0x7fffdf, 23),
    (0xffffec, 24),
    (0xffffed, 24),
    (0x3fffd7, 22),
    (0x7fffe0, 23),
    (0xffffee, 24),
    (0x7fffe1, 23),
    (0x7fffe2, 23),
    (0x7fffe3, 23),
    (0x7fffe4, 23),
    (0x1fffdc, 21),
    (0x3fffd8, 22),
    (0x7fffe5, 23),
    (0x3fffd9, 22),
    (0x7fffe6, 23),
    (0x7fffe7, 23),
    (0xffffef, 24),
    (0x3fffda, 22),
    (0x1fffdd, 21),
    (0xfffe9, 20),
    (0x3fffdb, 22),
    (0x3fffdc, 22),
    (0x7fffe8, 23),
    (0x7fffe9, 23),
    (0x1fffde, 21),
    (0x7fffea, 23),
    (0x3fffdd, 22),
    (0x3fffde, 22),
    (0xfffff0, 24),
    (0x1fffdf, 21),
    (0x3fffdf, 22),
    (0x7fffeb, 23),
    (0x7fffec, 23),
    (0x1fffe0, 21),
    (0x1fffe1, 21),
    (0x3fffe0, 22),
    (0x1fffe2, 21),
    (0x7fffed, 23),
    (0x3fffe1, 22),
    (0x7fffee, 23),
    (0x7fffef, 23),
    (0xfffea, 20),
    (0x3fffe2, 22),
    (0x3fffe3, 22),
    (0x3fffe4, 22),
    (0x7ffff0, 23),
    (0x3fffe5, 22),
    (0x3fffe6, 22),
    (0x7ffff1, 23),
    (0x3ffffe0, 26),
    (0x3ffffe1, 26),
    (0xfffeb, 20),
    (0x7fff1, 19),
    (0x3fffe7, 22),
    (0x7ffff2, 23),
    (0x3fffe8, 22),
    (0x1ffffec, 25),
    (0x3ffffe2, 26),
    (0x3ffffe3, 26),
    (0x3ffffe4, 26),
    (0x7ffffde, 27),
    (0x7ffffdf, 27),
    (0x3ffffe5, 26),
    (0xfffff1, 24),
    (0x1ffffed, 25),
    (0x7fff2, 19),
    (0x1fffe3, 21),
    (0x3ffffe6, 26),
    (0x7ffffe0, 27),
    (0x7ffffe1, 27),
    (0x3ffffe7, 26),
    (0x7ffffe2, 27),
    (0xfffff2, 24),
    (0x1fffe4, 21),
    (0x1fffe5, 21),
    (0x3ffffe8, 26),
    (0x3ffffe9, 26),
    (0xffffffd, 28),
    (0x7ffffe3, 27),
    (0x7ffffe4, 27),
    (0x7ffffe5, 27),
    (0xfffec, 20),
    (0xfffff3, 24),
    (0xfffed, 20),
    (0x1fffe6, 21),
    (0x3fffe9, 22),
    (0x1fffe7, 21),
    (0x1fffe8, 21),
    (0x7ffff3, 23),
    (0x3fffea, 22),
    (0x3fffeb, 22),
    (0x1ffffee, 25),
    (0x1ffffef, 25),
    (0xfffff4, 24),
    (0xfffff5, 24),
    (0x3ffffea, 26),
    (0x7ffff4, 23),
    (0x3ffffeb, 26),
    (0x7ffffe6, 27),
    (0x3ffffec, 26),
    (0x3ffffed, 26),
    (0x7ffffe7, 27),
    (0x7ffffe8, 27),
    (0x7ffffe9, 27),
    (0x7ffffea, 27),
    (0x7ffffeb, 27),
    (0xffffffe, 28),
    (0x7ffffec, 27),
    (0x7ffffed, 27),
    (0x7ffffee, 27),
    (0x7ffffef, 27),
    (0x7fffff0, 27),
    (0x3ffffee, 26),
    (0x3fffffff, 30),
];

const DECODED: u8 = 1;
const ERROR: u8 = 2;
const MAYBE_EOS: u8 = 4;
const NO_NODE: u16 = u16::MAX;
const NO_SYMBOL: u16 = u16::MAX;
const NODE_COUNT: usize = CODES.len() * 2 - 1;
const STATE_COUNT: usize = CODES.len() - 1;

#[repr(C)]
#[derive(Copy, Clone)]
struct DecodeEntry {
    next_state: u8,
    byte: u8,
    flags: u8,
}

impl DecodeEntry {
    const EMPTY: Self = Self {
        next_state: 0,
        byte: 0,
        flags: 0,
    };
}

#[derive(Copy, Clone)]
struct TreeNode {
    children: [u16; 2],
    symbol: u16,
}

impl TreeNode {
    const EMPTY: Self = Self {
        children: [NO_NODE; 2],
        symbol: NO_SYMBOL,
    };
}

const DECODE_TABLE: [[DecodeEntry; 16]; STATE_COUNT] = build_decode_table();

const fn build_decode_table() -> [[DecodeEntry; 16]; STATE_COUNT] {
    let mut nodes = [TreeNode::EMPTY; NODE_COUNT];
    let mut node_count = 1usize;
    let mut symbol = 0usize;
    while symbol < CODES.len() {
        let (code, code_len) = CODES[symbol];
        let mut node = 0usize;
        let mut bit = code_len as usize;
        while bit != 0 {
            bit -= 1;
            let direction = ((code >> bit) & 1) as usize;
            let child = nodes[node].children[direction];
            if child == NO_NODE {
                nodes[node].children[direction] = node_count as u16;
                node = node_count;
                node_count += 1;
            } else {
                node = child as usize;
            }
        }
        nodes[node].symbol = symbol as u16;
        symbol += 1;
    }

    let mut node_by_state = [0u16; STATE_COUNT];
    let mut state_by_node = [0u8; NODE_COUNT];
    let mut state_count = 0usize;
    let mut node = 0usize;
    while node < node_count {
        if nodes[node].symbol == NO_SYMBOL {
            node_by_state[state_count] = node as u16;
            state_by_node[node] = state_count as u8;
            state_count += 1;
        }
        node += 1;
    }

    let mut eos_prefix = [false; NODE_COUNT];
    eos_prefix[0] = true;
    node = 0;
    let mut eos_bits = 0;
    while eos_bits < 7 {
        node = nodes[node].children[1] as usize;
        eos_prefix[node] = true;
        eos_bits += 1;
    }

    let mut table = [[DecodeEntry::EMPTY; 16]; STATE_COUNT];
    let mut state = 0usize;
    while state < state_count {
        let mut nibble = 0usize;
        while nibble < 16 {
            node = node_by_state[state] as usize;
            let mut byte = 0;
            let mut flags = 0;
            let mut remaining = 4usize;
            while remaining != 0 {
                remaining -= 1;
                let direction = (nibble >> remaining) & 1;
                node = nodes[node].children[direction] as usize;
                let decoded = nodes[node].symbol;
                if decoded != NO_SYMBOL {
                    if decoded == 256 {
                        flags |= ERROR;
                        node = 0;
                        break;
                    }
                    flags |= DECODED;
                    byte = decoded as u8;
                    node = 0;
                }
            }
            if eos_prefix[node] {
                flags |= MAYBE_EOS;
            }
            table[state][nibble] = DecodeEntry {
                next_state: state_by_node[node],
                byte,
                flags,
            };
            nibble += 1;
        }
        state += 1;
    }
    table
}

impl<'a> HpackHuffmanSource<'a> {
    pub const fn new(input: &'a [u8]) -> Self {
        Self(input)
    }

    pub fn encoded_len(self) -> usize {
        let mut bits: u64 = 0;
        for &b in self.0 {
            bits += CODES[b as usize].1 as u64;
        }
        bits.div_ceil(8) as usize
    }

    pub fn encode(self, out: &mut Vec<u8>) {
        let mut buffer: u64 = 0;
        let mut bits_in: u32 = 0;
        for &b in self.0 {
            let (code, len) = CODES[b as usize];
            let len = len as u32;
            buffer = (buffer << len) | (code as u64);
            bits_in += len;
            while bits_in >= 8 {
                bits_in -= 8;
                out.push((buffer >> bits_in) as u8);
            }
        }
        if bits_in > 0 {
            let pad_bits = 8 - bits_in;
            buffer = (buffer << pad_bits) | ((1u64 << pad_bits) - 1);
            out.push(buffer as u8);
        }
    }
}

impl<'a> HpackHuffmanEncoded<'a> {
    pub const fn new(input: &'a [u8]) -> Self {
        Self(input)
    }

    pub fn decode(self, out: &mut Vec<u8>) -> Result<(), HpackHuffmanError> {
        match self.decode_with(|byte| {
            out.push(byte);
            Ok::<_, Infallible>(())
        }) {
            Ok(()) => Ok(()),
            Err(HpackHuffmanDecodeError::InvalidEncoding(error)) => Err(error),
            Err(HpackHuffmanDecodeError::Sink(never)) => match never {},
        }
    }

    pub fn decode_with<E>(
        self,
        emit: impl FnMut(u8) -> Result<(), E>,
    ) -> Result<(), HpackHuffmanDecodeError<E>> {
        let mut decoder = HpackHuffmanDecoder::new();
        decoder.feed(self.0, emit)?;
        decoder
            .finish()
            .map_err(HpackHuffmanDecodeError::InvalidEncoding)
    }
}

impl HpackHuffmanDecoder {
    pub const fn new() -> Self {
        Self {
            state: 0,
            maybe_eos: true,
        }
    }

    pub fn feed<E>(
        &mut self,
        input: &[u8],
        mut emit: impl FnMut(u8) -> Result<(), E>,
    ) -> Result<(), HpackHuffmanDecodeError<E>> {
        let mut state = self.state as usize;
        let mut maybe_eos = self.maybe_eos;
        for &byte in input {
            let high = DECODE_TABLE[state][(byte >> 4) as usize];
            if high.flags & ERROR != 0 {
                return Err(HpackHuffmanDecodeError::InvalidEncoding(HpackHuffmanError));
            }
            if high.flags & DECODED != 0 {
                emit(high.byte).map_err(HpackHuffmanDecodeError::Sink)?;
            }
            state = high.next_state as usize;

            let low = DECODE_TABLE[state][(byte & 0x0f) as usize];
            if low.flags & ERROR != 0 {
                return Err(HpackHuffmanDecodeError::InvalidEncoding(HpackHuffmanError));
            }
            if low.flags & DECODED != 0 {
                emit(low.byte).map_err(HpackHuffmanDecodeError::Sink)?;
            }
            state = low.next_state as usize;
            maybe_eos = low.flags & MAYBE_EOS != 0;
        }
        self.state = state as u8;
        self.maybe_eos = maybe_eos;
        Ok(())
    }

    pub fn finish(self) -> Result<(), HpackHuffmanError> {
        if self.state == 0 || self.maybe_eos {
            Ok(())
        } else {
            Err(HpackHuffmanError)
        }
    }
}

impl Default for HpackHuffmanDecoder {
    fn default() -> Self {
        Self::new()
    }
}
