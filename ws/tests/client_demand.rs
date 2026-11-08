pub mod common;

use std::{
    convert::Infallible,
    io::{self, Read as _},
    net::TcpListener,
    task::Poll,
};

use common::{
    app::{self, borrowed::Borrowed},
    client::topology,
    peer::Peer as _,
};
use dope::core::driver::life::recovery;
use dope_fiber::abi::poll_fn;
use dope_test::{
    fibers::Wait,
    harness::{
        observe::Trace,
        scenario::run::{Entry, Task as _},
        signals::Gate,
    },
};
use o3::base::cell::local::Access;
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
        let mut buf = Vec::new();
        for expected in [b"filler".as_slice(), b"filler", b"selected"] {
            let (_, opcode, data) = first.try_client(&mut buf).expect("selected send");
            assert_eq!((opcode, data.as_slice()), (1, expected));
        }
        app::Upgrade::new(&second)
            .try_respond(&app::Head::new(&next).try_accept().expect("second key"))
            .expect("second upgrade");
        assert_eq!(second.read(&mut [0]).expect("second retired"), 0);
        let (_, opcode, data) = first.try_client(&mut buf).expect("retained send");
        assert_eq!((opcode, data.as_slice()), (1, b"retained".as_slice()));
        listener.set_nonblocking(true).expect("inspect surplus");
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
        );
    });

    peer.with(|| {
        let events = Trace::new();
        let closed = Gate::new();
        let config = client::Config::new("localhost", "/")
            .expect("configuration")
            .message(8)
            .outbound(8);
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
                            app.drive_within(poll_fn(|cx| {
                                send.poll_active(cx.as_ref().completion())
                            }))
                            .expect("opening driver")
                            .expect("opening wake")
                            .expect("selected connection");
                            let mut sent = 0;
                            let mut parked = false;
                            app.drive_within(poll_fn(|mut cx| {
                                while sent < 3 {
                                    let wake = cx.as_ref().completion();
                                    let data = if sent == 2 { "selected" } else { "filler" };
                                    match send.poll_text(
                                        Access::new(cx.as_mut().region_token()),
                                        data,
                                        wake,
                                    ) {
                                        Poll::Pending => {
                                            assert_eq!(sent, 2);
                                            parked = true;
                                            return Poll::Pending;
                                        }
                                        Poll::Ready(result) => result.expect("selected send"),
                                    }
                                    sent += 1;
                                }
                                Poll::Ready(())
                            }))
                            .expect("send driver")
                            .expect("capacity wake");
                            assert!(parked);
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
            .expect("client setup");
    })
    .expect("peer");
}
