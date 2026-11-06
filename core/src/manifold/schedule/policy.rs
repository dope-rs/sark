use crate::manifold::schedule::WorkClass;

#[derive(Clone, Copy)]
pub(super) enum Round {
    Single,
    ControlFirst,
    DataFirst,
}

#[derive(Default)]
pub(super) struct Policy(u8);

impl Policy {
    const PREFER_DATA: u8 = 1;
    const DATA_BLOCKED: u8 = 2;

    pub(super) const fn is_data_next(&self) -> bool {
        self.0 & Self::PREFER_DATA != 0
    }

    pub(super) const fn is_blocked(&self) -> bool {
        self.0 & Self::DATA_BLOCKED != 0
    }

    pub(super) fn block_data(&mut self) {
        self.0 |= Self::DATA_BLOCKED;
    }

    pub(super) fn release_data(&mut self) {
        self.0 &= !Self::DATA_BLOCKED;
    }

    pub(super) fn finish_round(&mut self, round: Round, activity: WorkClass) {
        match (round, activity) {
            (Round::DataFirst | Round::Single, WorkClass::Data) => self.0 &= !Self::PREFER_DATA,
            (Round::ControlFirst | Round::Single, WorkClass::Control) => {
                self.0 |= Self::PREFER_DATA
            }
            (Round::DataFirst, WorkClass::Control) => self.0 &= !Self::PREFER_DATA,
            (Round::ControlFirst, WorkClass::Data) => self.0 |= Self::PREFER_DATA,
        }
    }

    pub(super) fn reset(&mut self) {
        self.0 = 0;
    }
}
