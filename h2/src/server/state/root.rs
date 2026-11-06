//! Listener deadline root synchronized with one H2 connection.

use dope::{
    core::driver::{route::Identity, schedule::clock},
    manifold::network::listener::conn::{self, deadline},
    net::{link::egress::data, wire},
};

pub(crate) fn try_sync<'d, ID: Identity, W, C, B>(
    deadline: Option<clock::Deadline<'d>>,
    conn: &mut conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, W, C, B>,
) -> Result<(), deadline::Error>
where
    W: wire::Spec,
    B: data::raw::Payload<'d>,
{
    let Some(deadline) = deadline else {
        conn.deadline().cancel();
        return Ok(());
    };
    conn.deadline().try_arm(deadline)
}
