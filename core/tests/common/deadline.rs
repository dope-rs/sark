use o3::depot::arena::{
    hash::runtime::Domain,
    slab::key::{Generation as Epoch, Handle, Parts},
};

pub enum Generation {}

pub enum Range {}

pub fn key<T>(index: u32) -> Handle<Domain<T>> {
    Handle::from_parts(Parts::new(index, Epoch::MIN))
}
