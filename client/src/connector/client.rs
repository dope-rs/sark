use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use cartel_core::{Extract, Registrable, Reply, Slot};
use dope::driver::token::Token;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope_fiber::{Either, Fiber, TimerExt as _, poll_fn, race, wait_fn};
use dope_net::Transport;
use http::Method;
use o3::buffer::{Lease, Pool};
use sark_core::http::Response;

use crate::connector::error::Error;
use crate::connector::redirect::RedirectState;
use crate::connector::session::{Outcome, Port, Session};

struct ExtractResponse;

type HandleMarker<'a, S, E> = PhantomData<(&'a (), fn() -> (S, E))>;

unsafe impl Extract<Outcome> for ExtractResponse {
    type Output = Outcome;

    fn extract(slot: &mut Slot<Outcome>) -> Option<Self::Output> {
        if !slot.completed() {
            return None;
        }
        if slot.take_overflow() {
            return Some(Err(Error::CapacityOverflow));
        }
        Some(slot.pop().unwrap_or(Err(Error::Closed)))
    }
}

pub struct HttpHandle<'a, 'd, const ID: u8, S, E> {
    port: &'d Port<'d>,
    marker: HandleMarker<'a, S, E>,
}

impl<S, E, const ID: u8> Copy for HttpHandle<'_, '_, ID, S, E> {}

impl<S, E, const ID: u8> Clone for HttpHandle<'_, '_, ID, S, E> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, 'd, const ID: u8, S, E> HttpHandle<'a, 'd, ID, S, E>
where
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn from_port(port: &'d Port<'d>) -> Self {
        Self {
            port,
            marker: PhantomData,
        }
    }

    pub fn from_cell(conn: Pin<&Connector<'d, ID, Session<'d>, S, E>>) -> Self {
        Self::from_port(conn.get_ref().session().port)
    }

    pub fn sleep(
        &self,
        duration: Duration,
    ) -> impl Fiber<'d, Output = ()> + 'd + use<'d, ID, S, E> {
        let timer: &'d _ = self.port.timer();
        timer.sleep(duration)
    }

    pub fn connection_count(&self) -> usize {
        self.port.shared.connection_count()
    }

    pub fn wait_active<'b>(&'b self) -> impl Fiber<'d, Output = Result<(), Error>> + 'b {
        let handle = self;
        wait_fn(move |cx, waiter| {
            let shared = &handle.port.shared;
            if shared.has_connection() {
                return Poll::Ready(Ok(()));
            }
            if !shared.try_register_active(waiter, cx.as_ref()) {
                return Poll::Ready(Err(Error::Backpressure));
            }
            if shared.has_connection() {
                shared.wake();
                return Poll::Ready(Ok(()));
            }
            Poll::Pending
        })
    }

    pub fn host<'b>(&'b self) -> impl Fiber<'d, Output = String> + 'b {
        let handle = self;
        poll_fn(move |_cx| Poll::Ready(handle.port.shared.host.clone()))
    }

    pub fn get<'b>(
        &'b self,
        path: &'b str,
    ) -> impl Fiber<'d, Output = Result<Response, Error>> + 'b {
        self.send(Method::GET, path, &[])
    }

    pub fn send<'b>(
        &'b self,
        method: Method,
        path: &'b str,
        body: &'b [u8],
    ) -> impl Fiber<'d, Output = Result<Response, Error>> + 'b {
        self.send_with_headers(method, path, &[], body)
    }

    pub fn send_with_headers<'b>(
        &'b self,
        method: Method,
        path: &'b str,
        headers: &'b [(&'b str, &'b str)],
        body: &'b [u8],
    ) -> impl Fiber<'d, Output = Result<Response, Error>> + 'b {
        let handle = *self;
        let validation = <str as HeaderField>::validate_all(headers);
        let max_redirects = handle.port.shared.max_redirects;
        let origin = &handle.port.shared.origin;
        dope_fiber::fiber!('d => async move {
            validation?;
            let mut method = method;
            let mut body = body;
            let mut response = handle
                .dispatch_with_retry(&method, path, headers, body)
                .await?;
            if !response.status().is_redirection() {
                return Ok(response);
            }
            let mut redirects = Box::new(RedirectState::new(max_redirects, origin, path)?);
            loop {
                let status = response.status().as_u16();
                let location = response
                    .headers()
                    .get("location")
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| Error::Http("redirect without Location header".into()))?;
                method = redirects.advance(status, location, &method)?;
                if method == Method::GET {
                    body = &[];
                }
                response = handle
                    .dispatch_with_retry(&method, redirects.path_and_query(), headers, body)
                    .await?;
                if !response.status().is_redirection() {
                    return Ok(response);
                }
            }
        })
    }

    fn dispatch_with_retry<'b>(
        self,
        method: &'b Method,
        path: &'b str,
        headers: &'b [(&'b str, &'b str)],
        body: &'b [u8],
    ) -> impl Fiber<'d, Output = Result<Response, Error>> + 'b
    where
        Self: 'b,
    {
        let handle = self;
        let retry = handle.port.shared.retry;
        let timer: &'d _ = handle.port.timer();
        dope_fiber::fiber!('d => async move {
            let attempts = retry.attempts(method);
            let mut attempt = 0;
            loop {
                match handle.dispatch_once(method, path, headers, body).await {
                    Ok(response) => return Ok(response),
                    Err(error)
                        if retry.should_retry(method, &error)
                            && attempt + 1 < attempts =>
                    {
                        attempt += 1;
                        let backoff = Duration::from_millis(25 * u64::from(attempt));
                        timer.sleep(backoff).await;
                    }
                    Err(error) => return Err(error),
                }
            }
        })
    }

    fn dispatch_once<'b>(
        self,
        method: &'b Method,
        path: &'b str,
        headers: &'b [(&'b str, &'b str)],
        body: &'b [u8],
    ) -> impl Fiber<'d, Output = Result<Response, Error>> + 'b
    where
        Self: 'b,
    {
        let handle = self;
        dope_fiber::fiber!('d => async move {
            let shared = &handle.port.shared;
            let request_timeout = shared.request_timeout;
            let request = Encode::request(
                handle.port.requests.as_ref(),
                method,
                path,
                &shared.host,
                headers,
                body,
            )?;
            let mut request = Some(request);
            let acquire = wait_fn(move |cx, waiter| {
                let now = Instant::now();
                let shared = &handle.port.shared;
                let idle = shared.idle_timeout;
                let chosen = shared.acquire(now, idle, |token| handle.port.io.close(token));
                match chosen {
                    Some(token) => {
                        let req = request.take().expect("dispatch enqueue polled twice");
                        Poll::Ready(Enqueue::submit(handle, token, req).map(|reply| (token, reply)))
                    }
                    None => {
                        if !shared.try_register_active(waiter, cx.as_ref()) {
                            return Poll::Ready(Err(Error::Backpressure));
                        }
                        let chosen = shared.acquire(now, idle, |token| handle.port.io.close(token));
                        match chosen {
                            Some(token) => {
                                shared.wake();
                                let req = request.take().expect("dispatch enqueue polled twice");
                                Poll::Ready(
                                    Enqueue::submit(handle, token, req)
                                        .map(|reply| (token, reply)),
                                )
                            }
                            None => Poll::Pending,
                        }
                    }
                }
            });
            let acquire_deadline = handle.sleep(request_timeout);
            let (conn_id, reply) = match race(acquire, acquire_deadline).await {
                Either::Left(result) => result?,
                Either::Right(()) => return Err(Error::Timeout),
            };

            let reply_deadline = handle.sleep(request_timeout);
            match race(reply, reply_deadline).await {
                Either::Left(outcome) => outcome,
                Either::Right(()) => {
                    handle.port.io.close(conn_id);
                    Err(Error::Timeout)
                }
            }
        })
    }
}

struct Enqueue;

impl Enqueue {
    fn submit<'a, 'd, const ID: u8, S, E>(
        handle: HttpHandle<'a, 'd, ID, S, E>,
        conn_id: Token,
        request: Lease<'d>,
    ) -> Result<Reply<'d, Outcome, ExtractResponse>, Error>
    where
        S: Dialer<E::Transport> + 'd,
        E: Env + 'd,
        E::Transport: Transport<Addr: Clone>,
    {
        let shared = &handle.port.shared;
        if !handle.port.io.is_active(conn_id) {
            shared.close_connection(conn_id);
            return Err(Error::NotConnected);
        }
        let arena = shared.arena(conn_id).ok_or(Error::NotConnected)?;
        if !arena.can_register() {
            return Err(Error::Backpressure);
        }
        if handle.port.io.try_enqueue(conn_id, request).is_err() {
            shared.make_available(conn_id);
            return Err(Error::Backpressure);
        }
        let mut reply = Reply::new();
        assert!(reply.try_attach(arena));
        shared.submitted(conn_id, Instant::now());
        Ok(reply)
    }
}

struct Encode;

impl Encode {
    fn request<'d>(
        pool: Pin<&'d Pool>,
        method: &Method,
        path: &str,
        host: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Result<Lease<'d>, Error> {
        let mut buf = pool.try_acquire().ok_or(Error::Backpressure)?;
        let initial: [&[u8]; 6] = [
            method.as_str().as_bytes(),
            b" ",
            path.as_bytes(),
            b" HTTP/1.1\r\nHost: ",
            host.as_bytes(),
            b"\r\nConnection: keep-alive\r\nAccept: \x2a/\x2a\r\n",
        ];
        let mut value = body.len();
        let mut digits = [0; 20];
        let mut cursor = digits.len();
        if !body.is_empty() {
            loop {
                cursor -= 1;
                digits[cursor] = b'0' + (value % 10) as u8;
                value /= 10;
                if value == 0 {
                    break;
                }
            }
        }
        let content_length: [&[u8]; 3] = if body.is_empty() {
            [&[], &[], &[]]
        } else {
            [b"Content-Length: ", &digits[cursor..], b"\r\n"]
        };
        buf.try_extend_from_slices(initial)
            .map_err(|_| Error::Backpressure)?;
        for (name, value) in headers {
            buf.try_extend_from_slices([name.as_bytes(), b": ", value.as_bytes(), b"\r\n"])
                .map_err(|_| Error::Backpressure)?;
        }
        buf.try_extend_from_slices(content_length)
            .map_err(|_| Error::Backpressure)?;
        buf.try_extend_from_slices([b"\r\n", body])
            .map_err(|_| Error::Backpressure)?;
        Ok(buf)
    }
}

trait HeaderField {
    fn validate_all(headers: &[(&str, &str)]) -> Result<(), Error>;
    fn is_valid_name(&self) -> bool;
    fn is_valid_value(&self) -> bool;
    fn is_reserved_name(&self) -> bool;
}

impl HeaderField for str {
    fn validate_all(headers: &[(&str, &str)]) -> Result<(), Error> {
        for (name, value) in headers {
            if !name.is_valid_name() {
                return Err(Error::Http("invalid request header name".into()));
            }
            if !value.is_valid_value() {
                return Err(Error::Http("invalid request header value".into()));
            }
            if name.is_reserved_name() {
                return Err(Error::Http("reserved request header".into()));
            }
        }
        Ok(())
    }

    fn is_valid_name(&self) -> bool {
        !self.is_empty()
            && self.bytes().all(|b| {
                matches!(
                    b,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                        | b'0'..=b'9'
                        | b'A'..=b'Z'
                        | b'a'..=b'z'
                )
            })
    }

    fn is_valid_value(&self) -> bool {
        self.bytes()
            .all(|b| b == b'\t' || (0x20..=0x7e).contains(&b))
    }

    fn is_reserved_name(&self) -> bool {
        self.eq_ignore_ascii_case("host")
            || self.eq_ignore_ascii_case("connection")
            || self.eq_ignore_ascii_case("content-length")
    }
}
