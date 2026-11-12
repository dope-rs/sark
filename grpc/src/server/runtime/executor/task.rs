use dope::{core::driver::route::KeyTag, manifold::network::listener::conn};
use dope_fiber::task::storage::set::group;
use o3::depot::map;

pub(in crate::server) struct Group;

pub(in crate::server) struct Task<'d, Target> {
    pub(super) owner: conn::Id<'d, KeyTag<0>>,
    pub(in crate::server) target: Target,
}

impl<'d, Target> group::Policy<Task<'d, Target>> for Group {
    type Key = conn::Id<'d, KeyTag<0>>;

    fn key(task: &Task<'d, Target>) -> Self::Key {
        task.owner
    }
}

impl<'d> map::Hasher<conn::Id<'d, KeyTag<0>>> for Group {
    fn hash(&self, owner: &conn::Id<'d, KeyTag<0>>) -> u64 {
        u64::from(owner.index().get()).wrapping_mul(0x9E37_79B9_7F4A_7C15)
    }
}
