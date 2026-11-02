use crate::segment::input::Input;

/// Immutable owners of one complete JSON compound whose grammar was admitted.
pub struct Admitted<'d> {
    pub(in crate::segment) input: Input<'d>,
}

impl Admitted<'_> {
    pub fn release(&mut self, budget: &mut usize) -> std::task::Poll<()> {
        self.input.release(budget).0
    }
}
