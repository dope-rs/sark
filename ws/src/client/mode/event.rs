use dope::{core::driver::route::Identity, manifold::client::session::turns};
use o3::{
    base::cell::{local::Access, region},
    buffer::storage,
};

use crate::client::{self, channel, headers, mode, protocol, send};

pub(in crate::client) trait Event<'d, H, ID: Identity> {
    fn handshake(
        handler: &mut H,
        req: headers::Request<'_>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<headers::Encoded, client::Error>;

    fn open(
        handler: &mut H,
        port: &channel::Port<'_, 'd, ID>,
        parts: &mut turns::Parts<'_, '_, 'd, protocol::Connection, storage::Shared, ID>,
    );

    fn message(
        handler: &mut H,
        msg: client::Message<'_>,
        parts: &mut turns::Parts<'_, '_, 'd, protocol::Connection, storage::Shared, ID>,
    );

    fn close(
        handler: &mut H,
        reason: client::CloseReason,
        region: &mut region::Token<region::Identity<'d>>,
    );
}

impl<'d, H: client::Handler<'d>, ID: Identity> Event<'d, H, ID> for mode::Local {
    fn handshake(
        handler: &mut H,
        req: headers::Request<'_>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<headers::Encoded, client::Error> {
        handler.handshake(req, Access::new(region))
    }

    fn open(
        handler: &mut H,
        _: &channel::Port<'_, 'd, ID>,
        parts: &mut turns::Parts<'_, '_, 'd, protocol::Connection, storage::Shared, ID>,
    ) {
        handler.open(Access::new(parts.driver.region_token()));
    }

    fn message(
        handler: &mut H,
        msg: client::Message<'_>,
        parts: &mut turns::Parts<'_, '_, 'd, protocol::Connection, storage::Shared, ID>,
    ) {
        handler.message(msg, Access::new(parts.driver.region_token()));
    }

    fn close(
        handler: &mut H,
        reason: client::CloseReason,
        region: &mut region::Token<region::Identity<'d>>,
    ) {
        handler.close(reason, Access::new(region));
    }
}

impl<'d, H: client::Observer, ID: Identity> Event<'d, H, ID> for mode::Observe {
    fn handshake(
        handler: &mut H,
        req: headers::Request<'_>,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> Result<headers::Encoded, client::Error> {
        handler.handshake(req)
    }

    fn open(
        handler: &mut H,
        _: &channel::Port<'_, 'd, ID>,
        _: &mut turns::Parts<'_, '_, 'd, protocol::Connection, storage::Shared, ID>,
    ) {
        handler.open();
    }

    fn message(
        handler: &mut H,
        msg: client::Message<'_>,
        _: &mut turns::Parts<'_, '_, 'd, protocol::Connection, storage::Shared, ID>,
    ) {
        handler.message(msg);
    }

    fn close(
        handler: &mut H,
        reason: client::CloseReason,
        _: &mut region::Token<region::Identity<'d>>,
    ) {
        handler.close(reason);
    }
}

impl<'d, H: client::Egress<'d, ID>, ID: Identity> Event<'d, H, ID> for mode::Egress {
    fn handshake(
        _: &mut H,
        req: headers::Request<'_>,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> Result<headers::Encoded, client::Error> {
        req.finish()
    }

    fn open(
        handler: &mut H,
        port: &channel::Port<'_, 'd, ID>,
        parts: &mut turns::Parts<'_, '_, 'd, protocol::Connection, storage::Shared, ID>,
    ) {
        let mut sender = send::Sender::new(
            parts.conn_id,
            parts.sink.reborrow(),
            port,
            parts.driver.region_token(),
        );
        handler.open(&mut sender);
    }

    fn message(
        handler: &mut H,
        msg: client::Message<'_>,
        _: &mut turns::Parts<'_, '_, 'd, protocol::Connection, storage::Shared, ID>,
    ) {
        handler.message(msg);
    }

    fn close(
        handler: &mut H,
        reason: client::CloseReason,
        _: &mut region::Token<region::Identity<'d>>,
    ) {
        handler.close(reason);
    }
}
