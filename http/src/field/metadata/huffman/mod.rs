pub mod decoder;
pub mod source;

/// RFC 7541 Appendix B canonical codes, including EOS at index 256.
pub const CODES: [(u32, u8); 257] = {
    const LENGTHS: [u8; 257] = [
        13, 23, 28, 28, 28, 28, 28, 28, 28, 24, 30, 28, 28, 30, 28, 28, 28, 28, 28, 28, 28, 28, 30,
        28, 28, 28, 28, 28, 28, 28, 28, 28, 6, 10, 10, 12, 13, 6, 8, 11, 10, 10, 8, 11, 8, 6, 6, 6,
        5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 7, 8, 15, 6, 12, 10, 13, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 8, 7, 8, 13, 19, 13, 14, 6, 15, 5, 6, 5, 6, 5, 6, 6, 6, 5,
        7, 7, 6, 6, 6, 5, 6, 7, 6, 5, 5, 6, 7, 7, 7, 7, 7, 15, 11, 14, 13, 28, 20, 22, 20, 20, 22,
        22, 22, 23, 22, 23, 23, 23, 23, 23, 24, 23, 24, 24, 22, 23, 24, 23, 23, 23, 23, 21, 22, 23,
        22, 23, 23, 24, 22, 21, 20, 22, 22, 23, 23, 21, 23, 22, 22, 24, 21, 22, 23, 23, 21, 21, 22,
        21, 23, 22, 23, 23, 20, 22, 22, 22, 23, 22, 22, 23, 26, 26, 20, 19, 22, 23, 22, 25, 26, 26,
        26, 27, 27, 26, 24, 25, 19, 21, 26, 27, 27, 26, 27, 24, 21, 21, 26, 26, 28, 27, 27, 27, 20,
        24, 20, 21, 22, 21, 21, 23, 22, 22, 25, 25, 24, 24, 26, 23, 26, 27, 26, 26, 27, 27, 27, 27,
        27, 28, 27, 27, 27, 27, 27, 26, 30,
    ];
    let mut next = [0u32; 31];
    let mut symbol = 0;
    while symbol < LENGTHS.len() {
        next[LENGTHS[symbol] as usize] += 1;
        symbol += 1;
    }

    let mut first = 0;
    let mut width = 1;
    while width < next.len() {
        let count = next[width];
        next[width] = first;
        first = (first + count) << 1;
        width += 1;
    }

    let mut codes = [(0, 0); LENGTHS.len()];
    symbol = 0;
    while symbol < LENGTHS.len() {
        let width = LENGTHS[symbol];
        let code = &mut next[width as usize];
        codes[symbol] = (*code, width);
        *code += 1;
        symbol += 1;
    }
    codes
};

const DECODED: u8 = 1;
const ERROR: u8 = 2;
const MAYBE_EOS: u8 = 4;
const BYTE_DECODE_MASK: u8 = 3;
const BYTE_ERROR: u8 = 4;
const BYTE_MAYBE_EOS: u8 = 8;
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

#[repr(C)]
#[derive(Copy, Clone)]
struct ByteEntry {
    next_state: u8,
    first: u8,
    second: u8,
    flags: u8,
}

impl ByteEntry {
    const EMPTY: Self = Self {
        next_state: 0,
        first: 0,
        second: 0,
        flags: 0,
    };
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
static BYTE_TABLE: [[ByteEntry; 256]; STATE_COUNT] = build_byte_table();

const fn build_byte_table() -> [[ByteEntry; 256]; STATE_COUNT] {
    let mut table = [[ByteEntry::EMPTY; 256]; STATE_COUNT];
    let mut state = 0usize;
    while state < STATE_COUNT {
        let mut byte = 0usize;
        while byte < 256 {
            let high = DECODE_TABLE[state][byte >> 4];
            if high.flags & ERROR != 0 {
                table[state][byte].flags = BYTE_ERROR;
                byte += 1;
                continue;
            }
            let low = DECODE_TABLE[high.next_state as usize][byte & 0x0f];
            let mut entry = ByteEntry {
                next_state: low.next_state,
                first: 0,
                second: 0,
                flags: 0,
            };
            if high.flags & DECODED != 0 {
                entry.first = high.byte;
                entry.flags = 1;
            }
            if low.flags & DECODED != 0 {
                if entry.flags == 0 {
                    entry.first = low.byte;
                } else {
                    entry.second = low.byte;
                }
                entry.flags += 1;
            }
            if low.flags & ERROR != 0 {
                entry.flags |= BYTE_ERROR;
            }
            if low.flags & MAYBE_EOS != 0 {
                entry.flags |= BYTE_MAYBE_EOS;
            }
            table[state][byte] = entry;
            byte += 1;
        }
        state += 1;
    }
    table
}

const fn build_decode_table() -> [[DecodeEntry; 16]; STATE_COUNT] {
    let mut nodes = [TreeNode::EMPTY; NODE_COUNT];
    let mut next = 1usize;
    let mut symbol = 0usize;
    while symbol < CODES.len() {
        let (code, code_len) = CODES[symbol];
        let mut node = 0usize;
        let mut bit = code_len as usize;
        while bit != 0 {
            bit -= 1;
            let side = ((code >> bit) & 1) as usize;
            let child = nodes[node].children[side];
            if child == NO_NODE {
                nodes[node].children[side] = next as u16;
                node = next;
                next += 1;
            } else {
                node = child as usize;
            }
        }
        nodes[node].symbol = symbol as u16;
        symbol += 1;
    }

    let mut by_state = [0u16; STATE_COUNT];
    let mut by_node = [0u8; NODE_COUNT];
    let mut states = 0usize;
    let mut node = 0usize;
    while node < next {
        if nodes[node].symbol == NO_SYMBOL {
            by_state[states] = node as u16;
            by_node[node] = states as u8;
            states += 1;
        }
        node += 1;
    }

    let mut eos = [false; NODE_COUNT];
    eos[0] = true;
    node = 0;
    let mut eos_bits = 0;
    while eos_bits < 7 {
        node = nodes[node].children[1] as usize;
        eos[node] = true;
        eos_bits += 1;
    }

    let mut table = [[DecodeEntry::EMPTY; 16]; STATE_COUNT];
    let mut state = 0usize;
    while state < states {
        let mut nibble = 0usize;
        while nibble < 16 {
            node = by_state[state] as usize;
            let mut byte = 0;
            let mut flags = 0;
            let mut bits = 4usize;
            while bits != 0 {
                bits -= 1;
                let side = (nibble >> bits) & 1;
                node = nodes[node].children[side] as usize;
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
            if eos[node] {
                flags |= MAYBE_EOS;
            }
            table[state][nibble] = DecodeEntry {
                next_state: by_node[node],
                byte,
                flags,
            };
            nibble += 1;
        }
        state += 1;
    }
    table
}
