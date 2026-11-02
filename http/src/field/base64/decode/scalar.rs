use crate::field::base64::{
    TABLE,
    decode::View,
    shape::{Shape, Tail},
};

const VALUES: [u8; 256] = {
    let mut values = [u8::MAX; 256];
    let mut index = 0;
    while index != TABLE.len() {
        values[TABLE[index] as usize] = index as u8;
        index += 1;
    }
    values
};

#[repr(transparent)]
#[derive(Clone, Copy)]
struct Symbol(u8);

pub(super) fn forward(view: &mut View<'_>, mut read: usize, mut written: usize, shape: Shape) {
    let consumed = read - view.origin;
    let mut groups = (shape.symbols() - consumed) / 4;
    while groups != 0 {
        full(view, read, written);
        read += 4;
        written += 3;
        groups -= 1;
    }
    tail(view, read, written, shape.tail());
}

pub(super) fn backward(view: &mut View<'_>, shape: Shape) {
    let groups = shape.groups();
    let read = view.origin + groups * 4;
    let written = view.destination + groups * 3;
    tail(view, read, written, shape.tail());
    let mut groups = groups;
    while groups != 0 {
        groups -= 1;
        full(
            view,
            view.origin + groups * 4,
            view.destination + groups * 3,
        );
    }
}

fn full(view: &mut View<'_>, read: usize, written: usize) {
    let [Symbol(a), Symbol(b), Symbol(c), Symbol(d)] = view.read::<4>(read).map(symbol);
    view.write(
        written,
        [(a << 2) | (b >> 4), (b << 4) | (c >> 2), (c << 6) | d],
    );
}

fn tail(view: &mut View<'_>, read: usize, written: usize, tail: Tail) {
    match tail {
        Tail::None => {}
        Tail::Two => {
            let [Symbol(a), Symbol(b)] = view.read::<2>(read).map(symbol);
            view.write(written, [(a << 2) | (b >> 4)]);
        }
        Tail::Three => {
            let [Symbol(a), Symbol(b), Symbol(c)] = view.read::<3>(read).map(symbol);
            view.write(written, [(a << 2) | (b >> 4), (b << 4) | (c >> 2)]);
        }
    }
}

fn symbol(byte: u8) -> Symbol {
    Symbol(VALUES[usize::from(byte)])
}

const _: () = assert!(core::mem::size_of::<Symbol>() == core::mem::size_of::<u8>());
