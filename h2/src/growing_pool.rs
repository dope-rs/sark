use o3::buffer::{SharedLease, SharedPool, SharedPoolPlan};

pub(crate) struct GrowingSharedPool {
    groups: Vec<SharedPool>,
    plan: SharedPoolPlan,
    allocated_slots: usize,
}

impl GrowingSharedPool {
    pub(crate) fn from_plan(plan: SharedPoolPlan) -> Self {
        debug_assert!(plan.max_slots() != 0);
        let mut pool = Self {
            groups: Vec::new(),
            plan,
            allocated_slots: 0,
        };
        pool.grow();
        pool
    }

    pub(crate) fn try_acquire(&mut self) -> Option<SharedLease> {
        for group in &self.groups {
            if let Some(lease) = group.try_acquire() {
                return Some(lease);
            }
        }
        if self.allocated_slots == self.plan.max_slots() {
            return None;
        }
        self.grow()
    }

    fn grow(&mut self) -> Option<SharedLease> {
        let remaining = self.plan.max_slots() - self.allocated_slots;
        let slots = self.allocated_slots.max(1).min(remaining);
        self.allocated_slots += slots;
        let pool = SharedPool::from_layout(self.plan.layout_up_to(slots));
        let lease = pool.try_acquire();
        self.groups.push(pool);
        lease
    }
}
