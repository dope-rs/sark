#[sark_json::json]
struct Generic<const N: usize> {
    #[field(raw)]
    token: sark_json::Token<N>,
}
fn main() {}
