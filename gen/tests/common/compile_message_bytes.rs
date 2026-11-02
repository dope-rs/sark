#[derive(sark_gen::Message)]
struct Unproved {
    #[prost(uint32, tag = "1")]
    data: u32,
}
fn main() {}