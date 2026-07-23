#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use sark::{HttpServer, HttpsServer, Tcp, Throughput, listener, tcp};

pub const MAX_CONNECTIONS: usize = 16;
pub const HTTP_LISTENER_ID: u8 = 0;
pub const DATE_UPDATER_ID: u8 = 1;

pub type TestHttpServer = HttpServer<HTTP_LISTENER_ID, DATE_UPDATER_ID, Throughput>;
pub type TestHttpsServer = HttpsServer<HTTP_LISTENER_ID, DATE_UPDATER_ID, Throughput>;

pub fn http_server(bind: SocketAddr, head_timeout: Duration) -> TestHttpServer {
    HttpServer::new(
        listener::Config::<Tcp> {
            bind,
            max_connections: MAX_CONNECTIONS,
            backlog: 16,
            stream: tcp::stream::Config {
                no_delay: Some(true),
                ..Default::default()
            },
            transport: tcp::listener::Config {
                reuse_port: true,
                ..Default::default()
            },
            egress: Default::default(),
        },
        head_timeout,
    )
}

pub fn https_server(bind: SocketAddr, head_timeout: Duration) -> TestHttpsServer {
    HttpsServer::new(
        listener::Config::<Tcp> {
            bind,
            max_connections: MAX_CONNECTIONS,
            backlog: 16,
            stream: Default::default(),
            transport: tcp::listener::Config {
                reuse_port: true,
                ..Default::default()
            },
            egress: Default::default(),
        },
        head_timeout,
    )
}
