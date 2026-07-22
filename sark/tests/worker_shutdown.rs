#![cfg(target_os = "linux")]

mod support;

use std::io;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use dope::runtime::{Dispatcher, Idle, Launcher};
use dope::{DriverContext, Event};
use o3::cell::BrandCell as Branded;
use sark::{Throughput, driver};

struct Park;

impl<'d> Dispatcher<'d> for Park {
    fn dispatch(self: Pin<&mut Self>, _event: Event<'d>, _driver: &mut DriverContext<'_, 'd>) {
        let _ = self;
    }

    fn activate(
        self: Pin<&mut Self>,
        _target: dope::driver::token::Token,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
        let _ = self;
    }

    fn pre_park(self: Pin<&mut Self>, _driver: &mut DriverContext<'_, 'd>) {
        let _ = self;
    }

    fn idle(self: Pin<&Self>) -> Idle {
        let _ = self;
        Idle::Park(None)
    }
}

#[test]
fn first_worker_error_shuts_down_peer_drivers() {
    let cpus = Launcher::allowed_cpus()
        .unwrap_or_default()
        .into_iter()
        .take(2)
        .collect::<Vec<_>>();
    if cpus.len() < 2 {
        return;
    }

    let server = support::http_server("127.0.0.1:0".parse().unwrap(), Duration::from_secs(10));
    let barrier = Arc::new(Barrier::new(cpus.len()));
    let order = Arc::new(AtomicUsize::new(0));
    let result = server.run(
        cpus,
        |_| driver::Config::for_tcp_profile::<Throughput>(1),
        move |_server, session| {
            barrier.wait();
            if order.fetch_add(1, Ordering::AcqRel) == 0 {
                return Err(io::Error::other("first worker failed"));
            }
            let dispatcher = pin!(Branded::new(Park));
            session.run(dispatcher.as_ref())
        },
    );

    let error = result.unwrap_err();
    assert!(error.to_string().starts_with("launcher worker "));
    let source = error
        .get_ref()
        .and_then(std::error::Error::source)
        .expect("worker failure retains its source");
    assert_eq!(source.to_string(), "first worker failed");
}
