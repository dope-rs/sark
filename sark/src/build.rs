use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::ptr::NonNull;

use dope::launcher::Ctx;
use dope::manifold::env::Bundle;
use dope::manifold::listener::config::Config;
use dope::manifold::listener::{Application, Listener};
use dope::manifold::timer::Timer;
use dope::runtime::profile::Throughput;
use dope::transport::{Tcp, Transport};
use dope::wire::{Identity, Wire};
use dope::{DriverConfig, Executor};
use dope_extra::Trigger;
use dope_tls::{Endpoint, Tls};

use crate::date::{DateHost, Updater};
use crate::timer::TimerHost;

#[derive(Clone, Debug)]
pub struct ServerCfg {
    pub bind: SocketAddr,
    pub max_conn: usize,
    pub backlog: i32,
    pub head_timeout: std::time::Duration,
}

#[derive(Clone)]
pub struct HttpsCfg {
    pub server: ServerCfg,
    pub tls: shin::server::Config,
}

type HttpEnv = Bundle<Tcp, Identity, Throughput>;
type HttpsEnv = Bundle<Tcp, Tls, Throughput>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
pub struct Dispatcher<'d, P, T, W>
where
    P: Application<Wire = W> + DateHost + TimerHost<'d>,
    T: Transport,
    W: Wire,
{
    #[pin]
    #[manifold]
    pub inner: Listener<0, P, Bundle<T, W, Throughput>>,
    #[pin]
    #[manifold]
    pub date: Updater<1>,
    #[pin]
    #[manifold]
    pub timer: Timer<{ crate::timer::SARK_TIMER_ID }>,
    pub _ph: PhantomData<&'d ()>,
}

pub struct Build;

impl Build {
    pub fn http<'d, P: Application<Wire = Identity> + DateHost + TimerHost<'d> + 'd>(
        protocol: P,
        cfg: ServerCfg,
        ctx: Ctx,
        shutdown: Option<&Trigger>,
    ) -> io::Result<()> {
        let cpu = ctx.cpu;
        let listener_cfg = Config::<Tcp> {
            max_conn: cfg.max_conn,
            bind: cfg.bind,
            backlog: cfg.backlog,
            stream_opts: dope::transport::config::tcp::StreamOpts {
                nodelay: dope::transport::config::SocketToggle::Enabled,
                ..Default::default()
            },
            listener_opts: dope::transport::config::tcp::ListenerOpts {
                reuseport: dope::transport::config::SocketToggle::Enabled,
                ..Default::default()
            },
        };
        let driver_cfg =
            <dope::DriverCfg as DriverConfig>::for_tcp_profile::<Throughput>(cfg.max_conn)
                .with_cpu_id(Some(cpu));
        let mut exec = Executor::new(driver_cfg)?;
        let drv = exec.driver_mut();
        if let Some(trigger) = shutdown {
            trigger.register(drv);
        }
        let inner = Listener::<0, P, HttpEnv>::open_in(protocol, listener_cfg, drv)?;
        let date = Updater::<1>::new();
        let mut app = core::pin::pin!(Dispatcher::<'d, P, Tcp, Identity> {
            inner,
            date,
            timer: Timer::new(),
            _ph: PhantomData,
        });
        let timer_handle = app.as_mut().timer_handle();
        let stamp = {
            let handler = app.as_mut().project().inner.handler_mut_pin();
            handler.bind_timer(timer_handle, cfg.head_timeout);
            NonNull::from(handler.date_stamp())
        };
        app.as_mut().project().date.get_mut().bind(stamp);
        exec.run(app.as_mut())
    }

    pub fn https<'d, P: Application<Wire = Tls> + DateHost + TimerHost<'d> + 'd>(
        protocol: P,
        cfg: HttpsCfg,
        ctx: Ctx,
        shutdown: Option<&Trigger>,
    ) -> io::Result<()> {
        let cpu = ctx.cpu;
        let listener_cfg = Config::<Tcp> {
            max_conn: cfg.server.max_conn,
            bind: cfg.server.bind,
            backlog: cfg.server.backlog,
            stream_opts: Default::default(),
            listener_opts: dope::transport::config::tcp::ListenerOpts {
                reuseport: dope::transport::config::SocketToggle::Enabled,
                ..Default::default()
            },
        };
        let max_conn = cfg.server.max_conn;
        let tls_cfg = cfg.tls;
        let driver_cfg = <dope::DriverCfg as DriverConfig>::for_tcp_profile::<Throughput>(max_conn)
            .with_cpu_id(Some(cpu));
        let mut exec = Executor::new(driver_cfg)?;
        let drv = exec.driver_mut();
        if let Some(trigger) = shutdown {
            trigger.register(drv);
        }
        let mut inner = Listener::<0, P, HttpsEnv>::open_in(protocol, listener_cfg, drv)?;
        inner.set_cfg(Endpoint::Server(Box::new(tls_cfg)));
        let date = Updater::<1>::new();
        let mut app = core::pin::pin!(Dispatcher::<'d, P, Tcp, Tls> {
            inner,
            date,
            timer: Timer::new(),
            _ph: PhantomData,
        });
        let timer_handle = app.as_mut().timer_handle();
        let stamp = {
            let handler = app.as_mut().project().inner.handler_mut_pin();
            handler.bind_timer(timer_handle, cfg.server.head_timeout);
            NonNull::from(handler.date_stamp())
        };
        app.as_mut().project().date.get_mut().bind(stamp);
        exec.run(app.as_mut())
    }
}
