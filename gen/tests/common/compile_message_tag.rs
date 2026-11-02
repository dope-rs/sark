#[derive(sark_gen::Message)]
struct Invalid {
    #[prost(bytes = "vec", tag = "0")]
    data: Vec<u8>,
}
fn main() {}