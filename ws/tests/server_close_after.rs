use std::{
    cell::Cell,
    io::{Read as _, Write as _},
    net::TcpStream,
    time::Duration,
};

use dope::{
    core::driver::life::recovery,
    runtime::{executor::session::Completion, topology::ops::Run},
};
use dope_test::harness::server::Pair;

pub mod common;

use common::{
    echo::{Closing, Echo},
    peer::Peer as _,
    protocol::Payload as _,
    server::{self, Build},
};

#[test]
fn reply_precedes_eof() {
    for (request, reply) in [(1, 32), (128 * 1024, 32), (128 * 1024, 128 * 1024)] {
        Pair::bind()
            .expect("listener")
            .check(
                move |context, bound| {
                    let calls = Cell::new(0);
                    let failure = Cell::new(None);
                    let payload = vec![0xa7; reply];
                    let sink = Closing {
                        echo: Echo {
                            calls: &calls,
                            repeat: Some(&payload),
                            activity: &|_| {},
                            failed: &|error| failure.set(Some(error)),
                        },
                    };
                    let server = server::try_new(Echo::config(bound.local_addr()), sink, bound)?;
                    server.enter(
                        context,
                        Run(&mut Completion::new()),
                        recovery::Policy::new(|_| {}, common::recover),
                        |result| assert!(matches!(result, Ok(Ok(_)))),
                    );
                    assert_eq!(calls.get(), 1);
                    let failure = failure.take();
                    assert!(
                        failure.is_none(),
                        "unexpected protocol failure: {failure:?}"
                    );
                    Ok::<_, Build>(())
                },
                |bind| {
                    let (mut socket, mut buffered) = TcpStream::try_connect(bind).expect("upgrade");
                    socket
                        .set_write_timeout(Some(Duration::from_secs(5)))
                        .expect("request deadline");
                    socket
                        .write_all(&vec![0x5a; request].try_masked(0x2, true).expect("request"))
                        .expect("request write");
                    assert_eq!(
                        socket.try_next(&mut buffered).expect("final response"),
                        (0x2, vec![0xa7; reply])
                    );
                    assert!(buffered.is_empty());
                    assert_eq!(socket.read(&mut [0; 1]).expect("server EOF"), 0);
                },
                |result| result.expect("server layout"),
            )
            .expect("server execution");
    }
}
