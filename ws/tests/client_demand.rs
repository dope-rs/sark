pub mod common;

use std::{
    convert::Infallible,
    io::{self, Read as _},
    net::TcpListener,
    pin::Pin,
};

use common::{
    app::{self, borrowed::Borrowed},
    client::topology,
    peer::Peer as _,
};
use dope::core::driver::life::recovery;
use dope_fiber::{abi::poll_fn, context};
use dope_test::{
    fibers::Wait,
    harness::{
        observe::Trace,
        scenario::run::{Entry, Task as _},
        signals::Gate,
    },
};
use sark_ws::client::{self, channel, mode};

#[test]
fn selected_stops_redial() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let address = listener.local_addr().expect("listener address");
    let peer = Entry(move || {
        let (first, _) = listener.accept().expect("first connection");
        let head = app::Upgrade::new(&first).try_head().expect("first head");
        let (mut second, _) = listener.accept().expect("second connection");
        let next = app::Upgrade::new(&second).try_head().expect("second head");
        app::Upgrade::new(&first)
            .try_respond(&app::Head::new(&head).try_accept().expect("first key"))
            .expect("first upgrade");
        let (_, opcode, data) = first.try_client(&mut Vec::new()).expect("selected send");
        assert_eq!((opcode, data.as_slice()), (1, b"selected".as_slice()));
        app::Upgrade::new(&second)
            .try_respond(&app::Head::new(&next).try_accept().expect("second key"))
            .expect("second upgrade");
        assert_eq!(second.read(&mut [0]).expect("second retired"), 0);
        let (_, opcode, data) = first.try_client(&mut Vec::new()).expect("retained send");
        assert_eq!((opcode, data.as_slice()), (1, b"retained".as_slice()));
        listener.set_nonblocking(true).expect("inspect surplus");
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
        );
    });

    peer.with(|| {
        let events = Trace::new();
        let closed = Gate::new();
        let config = client::Config::new("localhost", "/").expect("configuration");
        topology::Plan::new(address, config, 2, 1)
            .enter(
                |domain| {
                    domain.run(
                        channel::App::<_, mode::Egress>::new(Borrowed {
                            events: &events,
                            closed: &closed,
                            message: &|_| {},
                        }),
                        |mut app| {
                            let handle = app.client();
                            let mut send = handle.try_driver().expect("exclusive sender");
                            let opening = poll_fn(|cx: Pin<&mut context::Context<'_, '_>>| {
                                send.poll_active(cx.as_ref().completion())
                            });
                            app.drive_within(opening)
                                .expect("opening driver")
                                .expect("opening wake")
                                .expect("selected connection");
                            app.drive(handle.send_text("selected"))
                                .expect("send driver")
                                .expect("selected send");
                            assert_eq!(
                                app.try_until(&closed, 1).expect("second retirement"),
                                Wait::Reached
                            );
                            app.drive(handle.send_text("retained"))
                                .expect("send driver")
                                .expect("selected send");
                            assert_eq!(
                                app.try_until(&closed, 2).expect("peer completed send"),
                                Wait::Reached
                            );
                        },
                    )
                },
                recovery::Policy::new(
                    |_| {},
                    |failure: recovery::Recovery<'_, '_>| -> Infallible {
                        panic!("{:?}", failure.cause())
                    },
                ),
                |result| result.expect("client execution"),
            )
            .as_ref()
            .map_err(|error| error.kind())
            .expect("client setup");
    })
    .expect("peer");
}
