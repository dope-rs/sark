use crate::field::base64::decode::{Progress, View};

const INPUT: usize = 16;
const OUTPUT: usize = 12;

pub(super) fn decode(view: &mut View<'_>) -> Progress {
    let mut blocks = view.shape.groups() / 4;
    let mut read = view.origin;
    let mut written = view.destination;
    while blocks != 0 {
        let lanes = view.read::<INPUT>(read);
        let values = values(lanes);
        view.write(written, pack(values));
        read += INPUT;
        written += OUTPUT;
        blocks -= 1;
    }
    Progress::new(read, written)
}

fn values(input: [u8; INPUT]) -> [u8; INPUT] {
    // View has already admitted every symbol in this complete block.
    input.map(|byte| {
        byte.wrapping_add(16)
            .wrapping_add(3 * u8::from(byte == b'+'))
            .wrapping_sub(12 * u8::from(byte >= b'0'))
            .wrapping_sub(69 * u8::from(byte >= b'A'))
            .wrapping_sub(6 * u8::from(byte >= b'a'))
    })
}

fn pack(values: [u8; INPUT]) -> [u8; OUTPUT] {
    let mut output = [0; OUTPUT];
    for (group, output) in values
        .as_chunks::<4>()
        .0
        .iter()
        .zip(output.as_chunks_mut::<3>().0)
    {
        let [a, b, c, d] = *group;
        *output = [(a << 2) | (b >> 4), (b << 4) | (c >> 2), (c << 6) | d];
    }
    output
}
