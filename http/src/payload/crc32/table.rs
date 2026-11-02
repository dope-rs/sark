const TABLE: [[u32; 256]; 16] = {
    let mut table = [[0; 256]; 16];
    let mut byte = 0;
    while byte < 256 {
        let mut slice = 0;
        let mut value = byte as u32;
        while slice < 16 {
            value = super::shift(value, 8);
            table[slice][byte] = value;
            slice += 1;
        }
        byte += 1;
    }
    table
};

pub(super) fn update(mut value: u32, bytes: &[u8]) -> u32 {
    let (groups, tail) = bytes.as_chunks::<64>();
    for group in groups {
        for bytes in group.as_chunks::<16>().0 {
            value = block(value, bytes);
        }
    }
    let (blocks, tail) = tail.as_chunks::<16>();
    for bytes in blocks {
        value = block(value, bytes);
    }
    for byte in tail {
        value = (value >> 8) ^ TABLE[0][usize::from(value as u8 ^ *byte)];
    }
    value
}

fn block(value: u32, bytes: &[u8; 16]) -> u32 {
    let prefix = value.to_le_bytes();
    let mut value = TABLE[15][usize::from(bytes[0] ^ prefix[0])]
        ^ TABLE[14][usize::from(bytes[1] ^ prefix[1])]
        ^ TABLE[13][usize::from(bytes[2] ^ prefix[2])]
        ^ TABLE[12][usize::from(bytes[3] ^ prefix[3])];
    for (position, byte) in bytes[4..].iter().enumerate() {
        value ^= TABLE[11 - position][usize::from(*byte)];
    }
    value
}
