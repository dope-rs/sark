use sark_core::body::credit;

pub struct Credits(pub Vec<credit::Mutation>);

impl credit::Evidence for Credits {
    type Change = credit::Mutation;

    fn record(&mut self, change: Self::Change) {
        self.0.push(change);
    }
}
