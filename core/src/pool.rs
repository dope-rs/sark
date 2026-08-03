use o3::buffer::{SharedLease, SharedPool, SharedPoolPlan};

pub struct GrowingSharedPool {
    groups: Vec<SharedPool>,
    plan: SharedPoolPlan,
    allocated_slots: usize,
}

impl GrowingSharedPool {
    pub fn from_plan(plan: SharedPoolPlan) -> Self {
        let mut pool = Self {
            groups: Vec::new(),
            plan,
            allocated_slots: 0,
        };
        if pool.plan.max_slots() != 0 {
            pool.grow();
        }
        pool
    }

    pub fn try_acquire(&mut self) -> Option<SharedLease> {
        for group in &self.groups {
            if let Some(lease) = group.try_acquire() {
                return Some(lease);
            }
        }
        if self.allocated_slots >= self.plan.max_slots() {
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

#[cfg(test)]
mod tests {
    use super::GrowingSharedPool;
    use o3::buffer::SharedPoolPlan;

    #[test]
    fn zero_slots_stays_empty() {
        let plan = SharedPoolPlan::new(0, 8).expect("valid empty plan");
        let mut pool = GrowingSharedPool::from_plan(plan);
        assert!(pool.try_acquire().is_none());
    }

    #[test]
    fn grows_to_the_plan_limit_and_reuses_slots() {
        let plan = SharedPoolPlan::new(3, 8).expect("valid plan");
        let mut pool = GrowingSharedPool::from_plan(plan);
        let first = pool.try_acquire().expect("first slot");
        let second = pool.try_acquire().expect("second slot");
        let third = pool.try_acquire().expect("third slot");
        assert!(pool.try_acquire().is_none());

        drop(second);
        assert!(pool.try_acquire().is_some());
        drop((first, third));
    }
}
