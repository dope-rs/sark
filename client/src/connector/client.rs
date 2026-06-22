use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use cartel_core::{Extract, Reply, Slot};
use dope::WakeRef;
use dope::fiber::{Fiber, Holding};
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::runtime::token::Token;
use dope::transport::Transport;
use http::Method;
use o3::buffer::Owned;
use sark_core::http::Response;

use crate::connector::error::Error;
use crate::connector::redirect::RedirectState;
use crate::connector::session::{Outcome, Session};

struct ExtractResponse;

impl Extract<Outcome> for ExtractResponse {
    type Output = Outcome;

    fn extract(slot: &mut Slot<Outcome>) -> Option<Self::Output> {
        if !slot.completed() {
            return None;
        }
        Some(slot.pop().unwrap_or(Err(Error::Closed)))
    }
}

pub trait Client<'d, S, E>
where
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn wait_active<'b>(&'b self) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'b>;

    fn host<'b>(&'b self) -> Fiber<'d, impl Future<Output = String> + 'b>;

    fn get<'b>(
        &'b self,
        path: &'b str,
    ) -> Fiber<'d, impl Future<Output = Result<Response, Error>> + 'b>;

    fn send<'b>(
        &'b self,
        method: Method,
        path: &'b str,
        body: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<Response, Error>> + 'b>;

    fn send_with_headers<'b>(
        &'b self,
        method: Method,
        path: &'b str,
        headers: &'b [(&'b str, &'b str)],
        body: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<Response, Error>> + 'b>;
}

impl<'d, const ID: u8, S, E> Client<'d, S, E> for Holding<'d, Connector<ID, Session, S, E>>
where
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn wait_active<'b>(&'b self) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'b> {
        let holding = *self;
        Fiber::new(poll_fn(move |cx| {
            let mut h = holding.hold();
            let shared = &mut h.as_mut().session_mut().shared;
            if shared.any_ready() {
                return Poll::Ready(Ok(()));
            }
            if let Some(e) = shared.fatal.as_ref() {
                return Poll::Ready(Err(Error::Http(e.to_string())));
            }
            shared.active_wakers.register(WakeRef::verified(cx.waker()));
            Poll::Pending
        }))
    }

    fn host<'b>(&'b self) -> Fiber<'d, impl Future<Output = String> + 'b> {
        let holding = *self;
        Fiber::new(poll_fn(move |_cx| {
            let mut h = holding.hold();
            Poll::Ready(h.as_mut().session().shared.host.clone())
        }))
    }

    fn get<'b>(
        &'b self,
        path: &'b str,
    ) -> Fiber<'d, impl Future<Output = Result<Response, Error>> + 'b> {
        self.send(Method::GET, path, &[])
    }

    fn send<'b>(
        &'b self,
        method: Method,
        path: &'b str,
        body: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<Response, Error>> + 'b> {
        self.send_with_headers(method, path, &[], body)
    }

    fn send_with_headers<'b>(
        &'b self,
        method: Method,
        path: &'b str,
        headers: &'b [(&'b str, &'b str)],
        body: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<Response, Error>> + 'b> {
        let holding = *self;
        Fiber::new(async move {
            <str as HeaderField>::validate_all(headers)?;
            let (host, max_redirects) = {
                let mut h = holding.hold();
                let pin = h.as_mut();
                let shared = &pin.session().shared;
                (shared.host.clone(), shared.max_redirects)
            };
            let base: http::Uri = format!("http://{host}/")
                .parse()
                .map_err(|e| Error::Http(format!("invalid host: {e}")))?;
            let mut redirects = RedirectState::new(max_redirects, base, path);

            let mut method = method;
            let mut body = body.to_vec();
            let mut path = path.to_string();

            loop {
                let resp = holding
                    .dispatch_with_retry(&method, &path, headers, &body)
                    .await?;
                let status = resp.status().as_u16();
                if !resp.status().is_redirection() {
                    return Ok(resp);
                }
                let location = resp
                    .headers()
                    .get("location")
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| Error::Http("redirect without Location header".into()))?
                    .to_string();
                let next_method = redirects.advance(status, &location, &method)?;
                if next_method == Method::GET {
                    body.clear();
                }
                method = next_method;
                path = redirects.path_and_query();
            }
        })
    }
}

trait Dispatch<'d, S, E>
where
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn dispatch_with_retry<'b>(
        &'b self,
        method: &'b Method,
        path: &'b str,
        headers: &'b [(&'b str, &'b str)],
        body: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<Response, Error>> + 'b>;

    fn dispatch_once<'b>(
        &'b self,
        method: &'b Method,
        path: &'b str,
        headers: &'b [(&'b str, &'b str)],
        body: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<Response, Error>> + 'b>;
}

impl<'d, const ID: u8, S, E> Dispatch<'d, S, E> for Holding<'d, Connector<ID, Session, S, E>>
where
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn dispatch_with_retry<'b>(
        &'b self,
        method: &'b Method,
        path: &'b str,
        headers: &'b [(&'b str, &'b str)],
        body: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<Response, Error>> + 'b> {
        let holding = *self;
        Fiber::new(async move {
            let (attempts, retry) = {
                let mut h = holding.hold();
                let retry = h.as_mut().session().shared.retry;
                (retry.attempts(method), retry)
            };
            let mut last_err = Error::Closed;
            for attempt in 0..attempts {
                match holding.dispatch_once(method, path, headers, body).await {
                    Ok(resp) => return Ok(resp),
                    Err(e) if retry.should_retry(method, &e) && attempt + 1 < attempts => {
                        last_err = e;
                        let backoff = Duration::from_millis(25 * u64::from(attempt + 1));
                        holding.sleep(backoff).await;
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(last_err)
        })
    }

    fn dispatch_once<'b>(
        &'b self,
        method: &'b Method,
        path: &'b str,
        headers: &'b [(&'b str, &'b str)],
        body: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<Response, Error>> + 'b> {
        let holding = *self;
        Fiber::new(async move {
            let host = {
                let mut h = holding.hold();
                h.as_mut().session().shared.host.clone()
            };
            let request_timeout = {
                let mut h = holding.hold();
                h.as_mut().session().shared.request_timeout
            };
            let request = Encode::request(method, path, &host, headers, body);
            let mut request = Some(request);
            let acquire = poll_fn(move |cx| {
                let mut h = holding.hold();
                let now = Instant::now();
                let fatal = h
                    .as_mut()
                    .session()
                    .shared
                    .fatal
                    .as_ref()
                    .map(|e| e.to_string());
                if let Some(m) = fatal {
                    return Poll::Ready(Err(Error::Http(m)));
                }
                let idle = h.as_mut().session().shared.idle_timeout;
                let (chosen, recycle) = h.as_mut().session_mut().shared.acquire(now, idle);
                for tok in recycle {
                    h.as_mut().request_close(tok);
                }
                match chosen {
                    Some(tok) => {
                        let req = request.take().expect("dispatch enqueue polled twice");
                        Poll::Ready(Enqueue::submit(holding, tok, req).map(|reply| (tok, reply)))
                    }
                    None => {
                        h.as_mut()
                            .session_mut()
                            .shared
                            .active_wakers
                            .register(WakeRef::verified(cx.waker()));
                        Poll::Pending
                    }
                }
            });
            let (conn_id, reply) = {
                let mut acquire = std::pin::pin!(acquire);
                let mut deadline = std::pin::pin!(holding.sleep(request_timeout));
                let got = poll_fn(|cx| {
                    if let Poll::Ready(r) = acquire.as_mut().poll(cx) {
                        return Poll::Ready(Ok(r));
                    }
                    if deadline.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(Err(()));
                    }
                    Poll::Pending
                })
                .await;
                match got {
                    Ok(r) => r?,
                    Err(()) => return Err(Error::Timeout),
                }
            };

            let mut reply = std::pin::pin!(reply);
            let mut deadline = std::pin::pin!(holding.sleep(request_timeout));
            let raced = poll_fn(|cx| {
                if let Poll::Ready(out) = reply.as_mut().poll(cx) {
                    return Poll::Ready(Ok(out));
                }
                if deadline.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Err(()));
                }
                Poll::Pending
            })
            .await;
            match raced {
                Ok(outcome) => outcome,
                Err(()) => {
                    holding.hold().request_close(conn_id);
                    Err(Error::Timeout)
                }
            }
        })
    }
}

struct Enqueue;

impl Enqueue {
    fn submit<'d, const ID: u8, S, E>(
        holding: Holding<'d, Connector<ID, Session, S, E>>,
        conn_id: Token,
        request: o3::buffer::Shared,
    ) -> Result<Pin<Box<Reply<'d, Outcome, ExtractResponse>>>, Error>
    where
        S: Dialer<E::Transport> + 'd,
        E: Env + 'd,
        E::Transport: Transport<Addr: Clone>,
    {
        let mut h = holding.hold();
        let mut conn = h.as_mut();
        let fatal = conn
            .as_mut()
            .session()
            .shared
            .fatal
            .as_ref()
            .map(|e| e.to_string());
        if let Some(m) = fatal {
            return Err(Error::Http(m));
        }
        match conn.as_mut().state_for(conn_id) {
            None => {
                conn.as_mut().session_mut().shared.drop_conn(conn_id);
                return Err(Error::NotConnected);
            }
            Some(channel) => {
                if !channel.enqueue(request) {
                    return Err(Error::Backpressure);
                }
            }
        }
        let slab = conn
            .as_mut()
            .session_mut()
            .shared
            .slab_ptr_for(conn_id)
            .ok_or(Error::NotConnected)?;
        let mut reply: Pin<Box<Reply<'d, Outcome, ExtractResponse>>> = Box::pin(Reply::new());
        // SAFETY: `slab` is connector-lifetime (boxed, never freed while a Reply references it) and so outlives this Reply's 'd brand.
        unsafe {
            reply.as_mut().get_mut().register_mut_raw(slab);
        }
        conn.as_mut()
            .session_mut()
            .shared
            .touch(conn_id, Instant::now());
        conn.request_flush(conn_id);
        Ok(reply)
    }
}

struct Encode;

impl Encode {
    fn request(
        method: &Method,
        path: &str,
        host: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> o3::buffer::Shared {
        let header_len: usize = headers
            .iter()
            .map(|(name, value)| name.len() + value.len() + 4)
            .sum();
        let mut buf = Owned::with_capacity(96 + path.len() + host.len() + header_len + body.len());
        buf.extend_from_slice(method.as_str().as_bytes());
        buf.extend_from_slice(b" ");
        buf.extend_from_slice(path.as_bytes());
        buf.extend_from_slice(b" HTTP/1.1\r\nHost: ");
        buf.extend_from_slice(host.as_bytes());
        buf.extend_from_slice(b"\r\nConnection: keep-alive\r\nAccept: \x2a/\x2a\r\n");
        for (name, value) in headers {
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(b": ");
            buf.extend_from_slice(value.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        if !body.is_empty() {
            buf.extend_from_slice(b"Content-Length: ");
            buf.extend_from_slice(body.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(body);
        buf.freeze()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_request_includes_custom_headers() {
        let req = Encode::request(
            &Method::POST,
            "/oauth/token",
            "auth.sportradar.com",
            &[("Content-Type", "application/x-www-form-urlencoded")],
            b"grant_type=client_credentials",
        );
        let text = std::str::from_utf8(req.as_ref()).expect("request utf8");

        assert!(text.contains("POST /oauth/token HTTP/1.1\r\n"));
        assert!(text.contains("Host: auth.sportradar.com\r\n"));
        assert!(text.contains("Content-Type: application/x-www-form-urlencoded\r\n"));
        assert!(text.contains("Content-Length: 29\r\n"));
        assert!(text.ends_with("\r\ngrant_type=client_credentials"));
    }

    #[test]
    fn validate_headers_rejects_injection_and_reserved_headers() {
        assert!(
            <str as HeaderField>::validate_all(&[("Content-Type", "application/json")]).is_ok()
        );
        assert!(<str as HeaderField>::validate_all(&[("Bad\r\nName", "x")]).is_err());
        assert!(<str as HeaderField>::validate_all(&[("X-Test", "ok\r\nInjected: yes")]).is_err());
        assert!(<str as HeaderField>::validate_all(&[("Content-Length", "10")]).is_err());
        assert!(<str as HeaderField>::validate_all(&[("X-Test", "ok\tvalue")]).is_ok());
        assert!(<str as HeaderField>::validate_all(&[("X-Test", "bad\0nul")]).is_err());
        assert!(<str as HeaderField>::validate_all(&[("X-Test", "bad\x01ctl")]).is_err());
        assert!(<str as HeaderField>::validate_all(&[("CONTENT-LENGTH", "10")]).is_err());
        assert!(<str as HeaderField>::validate_all(&[("Host", "evil")]).is_err());
    }
}
