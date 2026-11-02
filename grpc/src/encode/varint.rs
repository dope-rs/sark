pub(super) struct Varint(Bytes);

enum Bytes {
    One([u8; 1]),
    Two([u8; 2]),
    Three([u8; 3]),
    Four([u8; 4]),
    Five([u8; 5]),
    Six([u8; 6]),
    Seven([u8; 7]),
    Eight([u8; 8]),
    Nine([u8; 9]),
    Ten([u8; 10]),
}

impl Varint {
    pub(super) const fn new(value: u64) -> Self {
        let bytes = match value {
            0..=127 => Bytes::One([value as u8]),
            128..=16383 => Bytes::Two([(value as u8) | 0x80, (value >> 7) as u8]),
            16384..=2097151 => Bytes::Three([
                (value as u8) | 0x80,
                ((value >> 7) as u8) | 0x80,
                (value >> 14) as u8,
            ]),
            2097152..=268435455 => Bytes::Four([
                (value as u8) | 0x80,
                ((value >> 7) as u8) | 0x80,
                ((value >> 14) as u8) | 0x80,
                (value >> 21) as u8,
            ]),
            268435456..=34359738367 => Bytes::Five([
                (value as u8) | 0x80,
                ((value >> 7) as u8) | 0x80,
                ((value >> 14) as u8) | 0x80,
                ((value >> 21) as u8) | 0x80,
                (value >> 28) as u8,
            ]),
            34359738368..=4398046511103 => Bytes::Six([
                (value as u8) | 0x80,
                ((value >> 7) as u8) | 0x80,
                ((value >> 14) as u8) | 0x80,
                ((value >> 21) as u8) | 0x80,
                ((value >> 28) as u8) | 0x80,
                (value >> 35) as u8,
            ]),
            4398046511104..=562949953421311 => Bytes::Seven([
                (value as u8) | 0x80,
                ((value >> 7) as u8) | 0x80,
                ((value >> 14) as u8) | 0x80,
                ((value >> 21) as u8) | 0x80,
                ((value >> 28) as u8) | 0x80,
                ((value >> 35) as u8) | 0x80,
                (value >> 42) as u8,
            ]),
            562949953421312..=72057594037927935 => Bytes::Eight([
                (value as u8) | 0x80,
                ((value >> 7) as u8) | 0x80,
                ((value >> 14) as u8) | 0x80,
                ((value >> 21) as u8) | 0x80,
                ((value >> 28) as u8) | 0x80,
                ((value >> 35) as u8) | 0x80,
                ((value >> 42) as u8) | 0x80,
                (value >> 49) as u8,
            ]),
            72057594037927936..=9223372036854775807 => Bytes::Nine([
                (value as u8) | 0x80,
                ((value >> 7) as u8) | 0x80,
                ((value >> 14) as u8) | 0x80,
                ((value >> 21) as u8) | 0x80,
                ((value >> 28) as u8) | 0x80,
                ((value >> 35) as u8) | 0x80,
                ((value >> 42) as u8) | 0x80,
                ((value >> 49) as u8) | 0x80,
                (value >> 56) as u8,
            ]),
            _ => Bytes::Ten([
                (value as u8) | 0x80,
                ((value >> 7) as u8) | 0x80,
                ((value >> 14) as u8) | 0x80,
                ((value >> 21) as u8) | 0x80,
                ((value >> 28) as u8) | 0x80,
                ((value >> 35) as u8) | 0x80,
                ((value >> 42) as u8) | 0x80,
                ((value >> 49) as u8) | 0x80,
                ((value >> 56) as u8) | 0x80,
                (value >> 63) as u8,
            ]),
        };
        Self(bytes)
    }

    pub(super) fn as_slice(&self) -> &[u8] {
        match &self.0 {
            Bytes::One(bytes) => bytes,
            Bytes::Two(bytes) => bytes,
            Bytes::Three(bytes) => bytes,
            Bytes::Four(bytes) => bytes,
            Bytes::Five(bytes) => bytes,
            Bytes::Six(bytes) => bytes,
            Bytes::Seven(bytes) => bytes,
            Bytes::Eight(bytes) => bytes,
            Bytes::Nine(bytes) => bytes,
            Bytes::Ten(bytes) => bytes,
        }
    }
}

const _: () = {
    assert!(size_of::<Varint>() == 11);
    assert!(size_of::<Option<Varint>>() == size_of::<Varint>());
};
