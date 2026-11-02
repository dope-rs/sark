use std::{borrow::Cow, num};

use o3::{alloc, base::time::Positive};
use sark_http::{field, uri::target};

use crate::client::{self, headers};

pub struct Config {
    endpoint: target::Target,
    pub(super) handshake: Handshake,
    pub(super) max_frame_payload: num::NonZero<usize>,
    pub(super) max_message_size: num::NonZero<usize>,
    pub(super) max_outbound_frame_payload: num::NonZero<usize>,
    /// Maximum age of one transport generation before reconnect.
    pub(super) max_connection_age: Option<Positive>,
}

pub(super) struct Handshake {
    pub(super) user_agent: Option<field::Value<String>>,
    pub(super) headers: Vec<headers::Field>,
}

fn limit(max: usize) -> num::NonZero<usize> {
    num::NonZero::new(max).unwrap_or(num::NonZero::<usize>::MIN)
}

impl Config {
    pub fn new(host: impl AsRef<str>, path: impl AsRef<str>) -> Result<Self, client::Error> {
        let origin = target::Origin::parse(path.as_ref()).map_err(client::Error::Origin)?;
        let endpoint = target::Target::new(host.as_ref(), origin).map_err(client::Error::Target)?;
        Ok(Self::from_target(endpoint))
    }

    pub fn from_target(endpoint: target::Target) -> Self {
        Self {
            endpoint,
            handshake: Handshake {
                user_agent: None,
                headers: Vec::new(),
            },
            max_frame_payload: limit(client::DEFAULT_MAX_MESSAGE),
            max_message_size: limit(client::DEFAULT_MAX_MESSAGE),
            max_outbound_frame_payload: limit(client::DEFAULT_MAX_OUTBOUND_FRAME),
            max_connection_age: None,
        }
    }

    pub fn host(&self) -> &str {
        self.endpoint.authority()
    }

    pub fn path(&self) -> &str {
        self.endpoint.path_query()
    }

    pub fn with_host(mut self, host: impl AsRef<str>) -> Result<Self, client::Error> {
        self.endpoint = self
            .endpoint
            .retarget(host.as_ref())
            .map_err(client::Error::Target)?;
        Ok(self)
    }

    pub fn with_path(mut self, path: impl AsRef<str>) -> Result<Self, client::Error> {
        let origin = target::Origin::parse(path.as_ref()).map_err(client::Error::Origin)?;
        self.endpoint = self
            .endpoint
            .with_origin(origin)
            .map_err(client::Error::Origin)?;
        Ok(self)
    }

    pub fn user_agent<'a>(mut self, agent: impl Into<Cow<'a, str>>) -> Result<Self, client::Error> {
        let agent = field::Value::new(agent.into())
            .map_err(client::Error::Agent)?
            .try_owned()
            .map_err(|cause| client::Error::Allocation(cause.into()))?;
        self.handshake.user_agent = Some(agent);
        Ok(self)
    }

    pub fn with_header<'a, 'b>(
        mut self,
        name: impl Into<Cow<'a, str>>,
        value: impl Into<Cow<'b, str>>,
    ) -> Result<Self, client::Error> {
        let header = headers::Field::new(name.into(), value.into())?
            .try_owned()
            .map_err(|cause| client::Error::Allocation(cause.into()))?;
        alloc::VecExt::try_reserve(&mut self.handshake.headers, 1)
            .map_err(client::Error::Allocation)?;
        self.handshake.headers.push(header);
        Ok(self)
    }

    pub fn message(mut self, max: usize) -> Self {
        self.max_message_size = limit(max);
        self
    }

    pub fn frame(mut self, max: usize) -> Self {
        self.max_frame_payload = limit(max);
        self
    }

    pub fn outbound(mut self, max: usize) -> Self {
        self.max_outbound_frame_payload = limit(max);
        self
    }

    pub fn age(mut self, window: Option<Positive>) -> Self {
        self.max_connection_age = window;
        self
    }
}

const _: () = assert!(core::mem::size_of::<target::Target>() <= 4 * core::mem::size_of::<usize>());
