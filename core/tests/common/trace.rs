use sark_core::body::credit;

#[derive(Default)]
pub struct Trace {
    pub changes: Vec<credit::Mutation>,
}

impl credit::Evidence for Trace {
    type Change = credit::Mutation;

    fn record(&mut self, change: credit::Mutation) {
        self.changes.push(change);
    }
}

pub struct Released<F>(pub F);

impl<F: FnMut()> credit::Evidence for Released<F> {
    type Change = credit::Mutation;

    fn record(&mut self, change: Self::Change) {
        if matches!(change, credit::Mutation::Released { .. }) {
            (self.0)();
        }
    }
}
