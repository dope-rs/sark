use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use cartel_core::{Extract, Registrable, ReplyStream, Slot};
use dope::driver::token::Token;
use dope::manifold::connector::session::Connector;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope_fiber::abi::Fiber;
use dope_fiber::abi::pollfn::PollFn;
use dope_fiber::abi::race::{Either, Race};
use dope_fiber::sleep::TimerExt;
use dope_fiber::wait::WaitFn;
use dope_net::Transport;
use http::Method;
use o3::buffer::{self, Lease, Pool};
use o3::cell::RegionToken;
use sark_core::http::Response;

use crate::connector::error::Error;
use crate::connector::redirect::RedirectState;
use crate::connector::response::ResponseEvent;
use crate::connector::session::{Outcome, Port, Session};

struct ExtractResponseEvent;

type HandleMarker<'a, S, E> = PhantomData<(&'a (), fn() -> (S, E))>;

impl Extract<Outcome> for ExtractResponseEvent {
    type Output = Outcome;

    fn extract(slot: &mut Slot<Outcome>) -> Option<Self::Output> {
        if let Some(outcome) = slot.pop() {
            return Some(outcome);
        }
        if slot.take_overflow() {
            return Some(Err(Error::CapacityOverflow));
        }
        None
    }
}

#[must_use = "the response stream must be consumed to observe the response"]
pub struct ResponseStream<'d> {
    conn_id: Token,
    reply: ReplyStream<'d, Outcome, ExtractResponseEvent>,
    done: bool,
}

impl<'d> ResponseStream<'d> {
    /// Waits for the next response event.
    ///
    /// `None` is the typed end-of-response marker.
    pub fn next_event(
        &mut self,
    ) -> impl Fiber<'d, Output = Option<Result<ResponseEvent, Error>>> + '_ {
        let stream = self;
        PollFn::new(move |cx| {
            if stream.done {
                return Poll::Ready(None);
            }
            let poll = Pin::new(&mut stream.reply).poll_next(cx);
            if matches!(poll, Poll::Ready(None)) {
                stream.done = true;
            }
            poll
        })
    }

    /// Returns true after the end-of-response marker has been observed.
    pub fn is_done(&self) -> bool {
        self.done
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
        WaitFn::new(move |cx, waiter| {
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
        PollFn::new(move |_cx| Poll::Ready(handle.port.shared.host.clone()))
    }

    pub fn get<'b>(
        &'b self,
        path: &'b str,
    ) -> impl Fiber<'d, Output = Result<Response, Error>> + 'b {
        self.send(Method::GET, path, &[])
    }

    /// Dispatches a GET and returns its transfer-decoded event stream.
    ///
    /// Unlike [`Self::get`], this does not collect the body, follow redirects,
    /// or apply content decoding.
    pub fn get_stream<'b>(
        &'b self,
        path: &'b str,
    ) -> impl Fiber<'d, Output = Result<ResponseStream<'d>, Error>> + 'b {
        self.send_stream(Method::GET, path, &[])
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

    /// Dispatches a request and returns its transfer-decoded event stream.
    ///
    /// Unlike [`Self::send`], this does not collect the body, follow redirects,
    /// or apply content decoding.
    pub fn send_stream<'b>(
        &'b self,
        method: Method,
        path: &'b str,
        body: &'b [u8],
    ) -> impl Fiber<'d, Output = Result<ResponseStream<'d>, Error>> + 'b {
        self.send_stream_with_headers(method, path, &[], body)
    }

    /// Dispatches a request with headers and returns its response event stream.
    ///
    /// Retries only cover failures before the stream is returned. Redirect and
    /// content-decoding policies belong to the buffered API.
    pub fn send_stream_with_headers<'b>(
        &'b self,
        method: Method,
        path: &'b str,
        headers: &'b [(&'b str, &'b str)],
        body: &'b [u8],
    ) -> impl Fiber<'d, Output = Result<ResponseStream<'d>, Error>> + 'b {
        let handle = *self;
        let validation = <str as HeaderField>::validate_all(headers);
        let retry = handle.port.shared.retry;
        let timer: &'d _ = handle.port.timer();
        dope_fiber::fiber!('d => async move {
            validation?;
            let attempts = retry.attempts(&method);
            let mut attempt = 0;
            loop {
                match handle
                    .dispatch_stream_once(&method, path, headers, body)
                    .await
                {
                    Ok(stream) => return Ok(stream),
                    Err(error)
                        if retry.should_retry(&method, &error) && attempt + 1 < attempts =>
                    {
                        attempt += 1;
                        timer
                            .sleep(Duration::from_millis(25 * u64::from(attempt)))
                            .await;
                    }
                    Err(error) => return Err(error),
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
            let stream = handle
                .dispatch_stream_once(method, path, headers, body)
                .await?;
            let conn_id = stream.conn_id;
            let collect = collect_response(stream, handle.port.codec.max_response_body);
            let deadline = handle.sleep(handle.port.shared.request_timeout);
            let response = match Race::new(collect, deadline).await {
                Either::Left(response) => response?,
                Either::Right(()) => {
                    PollFn::new(move |mut cx| {
                        handle
                            .port
                            .io
                            .close(cx.as_mut().region_token(), conn_id);
                        Poll::Ready(())
                    })
                    .await;
                    return Err(Error::Timeout);
                }
            };
            handle.decompress_response(response).await
        })
    }

    fn dispatch_stream_once<'b>(
        self,
        method: &'b Method,
        path: &'b str,
        headers: &'b [(&'b str, &'b str)],
        body: &'b [u8],
    ) -> impl Fiber<'d, Output = Result<ResponseStream<'d>, Error>> + 'b
    where
        Self: 'b,
    {
        let handle = self;
        dope_fiber::fiber!('d => async move {
            let shared = &handle.port.shared;
            let request_timeout = shared.request_timeout;
            let request = Encode::request(
                &handle.port.requests,
                method,
                path,
                &shared.host,
                headers,
                body,
            )?;
            let mut request = Some(request);
            let acquire = WaitFn::new(move |mut cx, waiter| {
                let now = Instant::now();
                let shared = &handle.port.shared;
                let idle = shared.idle_timeout;
                let chosen = shared.acquire(
                    cx.as_mut().region_token(),
                    now,
                    idle,
                    |region, token| handle.port.io.close(region, token),
                );
                match chosen {
                    Some(token) => {
                        let req = request.take().expect("dispatch enqueue polled twice");
                        Poll::Ready(
                            Enqueue::submit(handle, token, req, cx.as_mut().region_token())
                                .map(|reply| (token, reply)),
                        )
                    }
                    None => {
                        if !shared.try_register_active(waiter, cx.as_ref()) {
                            return Poll::Ready(Err(Error::Backpressure));
                        }
                        let chosen = shared.acquire(
                            cx.as_mut().region_token(),
                            now,
                            idle,
                            |region, token| handle.port.io.close(region, token),
                        );
                        match chosen {
                            Some(token) => {
                                shared.wake();
                                let req = request.take().expect("dispatch enqueue polled twice");
                                Poll::Ready(
                                    Enqueue::submit(
                                        handle,
                                        token,
                                        req,
                                        cx.as_mut().region_token(),
                                    )
                                        .map(|reply| (token, reply)),
                                )
                            }
                            None => Poll::Pending,
                        }
                    }
                }
            });
            let acquire_deadline = handle.sleep(request_timeout);
            let (conn_id, reply) = match Race::new(acquire, acquire_deadline).await {
                Either::Left(result) => result?,
                Either::Right(()) => return Err(Error::Timeout),
            };
            Ok(ResponseStream {
                conn_id,
                reply,
                done: false,
            })
        })
    }

    fn decompress_response(
        self,
        response: Response,
    ) -> impl Fiber<'d, Output = Result<Response, Error>> {
        let mut response = Some(response);
        PollFn::new(move |mut cx| {
            let mut response = response
                .take()
                .expect("response decompression polled after completion");
            let gunzip = self
                .port
                .shared
                .gunzip
                .borrow_mut(cx.as_mut().region_token());
            Session::decompress(
                gunzip,
                &mut response,
                self.port.shared.decompression,
                self.port.codec.max_response_body,
            )?;
            Poll::Ready(Ok(response))
        })
    }
}

fn collect_response<'d>(
    mut stream: ResponseStream<'d>,
    max_body: usize,
) -> impl Fiber<'d, Output = Result<Response, Error>> {
    dope_fiber::fiber!('d => async move {
        let mut response = None;
        let mut body = BodyCollector::new(max_body);
        let mut trailers_seen = false;
        while let Some(outcome) = stream.next_event().await {
            match outcome? {
                ResponseEvent::Informational(_) if response.is_none() => {}
                ResponseEvent::Informational(_) => {
                    return Err(Error::Parse(
                        "informational response after final response head".into(),
                    ));
                }
                ResponseEvent::Head(head) if response.is_none() => {
                    body.set_expected(content_length(head.as_response()));
                    response = Some(head.into_response());
                }
                ResponseEvent::Head(_) => {
                    return Err(Error::Parse("duplicate final response head".into()));
                }
                ResponseEvent::Data(data) if response.is_some() && !trailers_seen => {
                    body.push(data)?;
                }
                ResponseEvent::Data(_) => {
                    return Err(Error::Parse("response data outside body".into()));
                }
                ResponseEvent::Trailers(trailers)
                    if response.is_some() && !trailers_seen =>
                {
                    trailers_seen = true;
                    response
                        .as_mut()
                        .expect("response head checked")
                        .headers_mut()
                        .extend_trailers(
                            trailers
                                .iter()
                                .map(|(name, value)| (name.clone(), value.clone())),
                        );
                }
                ResponseEvent::Trailers(_) => {
                    return Err(Error::Parse("duplicate response trailers".into()));
                }
            }
        }
        let mut response =
            response.ok_or_else(|| Error::Parse("response ended before final head".into()))?;
        body.apply(&mut response);
        Ok(response)
    })
}

fn content_length(response: &Response) -> Option<usize> {
    response
        .headers()
        .get(http::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

struct BodyCollector {
    first: Option<buffer::Shared>,
    owned: Option<Vec<u8>>,
    expected: Option<usize>,
    len: usize,
    max: usize,
}

impl BodyCollector {
    fn new(max: usize) -> Self {
        Self {
            first: None,
            owned: None,
            expected: None,
            len: 0,
            max,
        }
    }

    fn set_expected(&mut self, expected: Option<usize>) {
        self.expected = expected.filter(|expected| *expected <= self.max);
    }

    fn push(&mut self, data: buffer::Shared) -> Result<(), Error> {
        self.len = self
            .len
            .checked_add(data.len())
            .filter(|len| *len <= self.max)
            .ok_or_else(|| Error::Parse("response body exceeds size limit".into()))?;
        if let Some(owned) = self.owned.as_mut() {
            owned.extend_from_slice(data.as_ref());
            return Ok(());
        }
        let Some(first) = self.first.take() else {
            self.first = Some(data);
            return Ok(());
        };
        let capacity = self.expected.unwrap_or(self.len).max(self.len);
        let mut owned = Vec::with_capacity(capacity);
        owned.extend_from_slice(first.as_ref());
        owned.extend_from_slice(data.as_ref());
        self.owned = Some(owned);
        Ok(())
    }

    fn apply(self, response: &mut Response) {
        if let Some(owned) = self.owned {
            response.set_body(owned);
        } else if let Some(first) = self.first {
            response.set_body(first);
        }
    }
}

struct Enqueue;

impl Enqueue {
    fn submit<'a, 'd, const ID: u8, S, E>(
        handle: HttpHandle<'a, 'd, ID, S, E>,
        conn_id: Token,
        request: Lease<'d>,
        region: &mut RegionToken<'d>,
    ) -> Result<ReplyStream<'d, Outcome, ExtractResponseEvent>, Error>
    where
        S: Dialer<E::Transport> + 'd,
        E: Env + 'd,
        E::Transport: Transport<Addr: Clone>,
    {
        let shared = &handle.port.shared;
        if !handle.port.io.is_active(conn_id) {
            shared.close_connection(region, conn_id);
            return Err(Error::NotConnected);
        }
        let arena = shared.arena(conn_id).ok_or(Error::NotConnected)?;
        if !arena.can_register(region) {
            return Err(Error::Backpressure);
        }
        if handle
            .port
            .io
            .try_enqueue(region, conn_id, request)
            .is_err()
        {
            shared.make_available(region, conn_id);
            return Err(Error::Backpressure);
        }
        let mut reply = ReplyStream::new();
        assert!(reply.try_attach(region, arena));
        shared.submitted(region, conn_id, Instant::now());
        Ok(reply)
    }
}

struct Encode;

impl Encode {
    fn request<'d>(
        pool: &'d Pool,
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
