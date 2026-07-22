use sark::fs::ServeDir;

fn main() {
    let state = dope::hash::Seed::new([1, 2]).state();
    let serve = ServeDir::new(".", state);
    let _second_owner = serve.clone();
}
