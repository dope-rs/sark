/// One item whose presence remains proven until it is consumed.
pub trait Ready {
    type Item;

    fn take(self) -> Self::Item;
}
