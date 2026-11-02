use o3::depot::map::compact;

/// Retains the complete owner while its two directory memberships are retired.
pub(in crate::payload::compress::dynamic) struct Clear<O, const N: usize = 0>(pub O);

impl<O> Clear<O>
where
    Self: compact::raw::Write,
    Clear<O, 1>: compact::raw::Write,
{
    /// # Safety
    /// Retire both directories' indexed memberships before exposing the returned owner.
    pub(in crate::payload::compress::dynamic) unsafe fn reset(mut self) -> O {
        // SAFETY: this operation retains the owner until both directories are reset.
        unsafe { compact::raw::Write::clear(&mut self) };
        let mut exact = Clear::<O, 1>(self.0);
        // SAFETY: the same exclusive owner retains its untouched exact directory.
        unsafe { compact::raw::Write::clear(&mut exact) };
        exact.0
    }
}
