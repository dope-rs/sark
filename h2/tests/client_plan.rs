use dope::core::random;
use sark_core::manifold::client::domain::Plan as _;
use sark_h2::{
    ClientRole, client,
    conn::{
        config::{self, Config, capacity::Encoder},
        machine::Connection,
    },
};

#[test]
fn retains_custom_capacity() {
    let config = config::Validated::new(Config {
        hpack_encoder: Encoder {
            table: 1024,
            ..Default::default()
        },
        ..Default::default()
    })
    .expect("validated client configuration");
    let plan = client::Plan::from(config);
    let seed = random::Seed::acquire().expect("seed");
    let conn: Connection<ClientRole> = plan.open_seeded(&seed).expect("client connection");
    assert_eq!(conn.settings().table_cap(), 1024);
}
