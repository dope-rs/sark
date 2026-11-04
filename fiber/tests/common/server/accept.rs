use std::{
    io,
    marker::PhantomData,
    net::SocketAddr,
    os::fd::{AsFd as _, FromRawFd as _, OwnedFd, RawFd},
};

use dope::core::{
    Backend, Errno,
    dope_sys::io::{file::Dir, socket::Socket as _},
    driver,
    io::socket::Addr,
};

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(driver = 'd)]
pub struct Listeners<'d, A, B> {
    #[pin]
    #[app]
    first: A,
    #[pin]
    #[app]
    second: B,
    #[dispatcher(state)]
    driver: PhantomData<driver::Reference<'d>>,
}

impl<'d, A, B> Listeners<'d, A, B> {
    pub fn new(first: A, second: B) -> Self {
        Self {
            first,
            second,
            driver: PhantomData,
        }
    }
}

unsafe extern "C" {
    fn dup(fd: RawFd) -> RawFd;
}

/// Retains a native fault descriptor before the affine listener enters io_uring.
pub fn duplicate(address: SocketAddr) -> io::Result<OwnedFd> {
    let mut directory =
        Dir::<Backend>::open::<Errno>(c"/proc/self/fd").expect("process descriptors");
    let (_, mut entries) = directory.entries();
    while let Some(entry) = entries.try_next::<Errno>().expect("descriptor entry") {
        let Some(number) = entry
            .name
            .to_str()
            .ok()
            .and_then(|name| name.parse::<RawFd>().ok())
        else {
            continue;
        };
        // SAFETY: dup accepts a descriptor number and either returns a fresh owner
        // or rejects it. No borrow of a concurrently closing candidate is forged.
        let duplicate = unsafe { dup(number) };
        if duplicate < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(Errno::BAD_DESCRIPTOR.code().get()) {
                continue;
            }
            return Err(error);
        }
        // SAFETY: this is the unique owner returned by the successful dup above.
        let duplicate = unsafe { OwnedFd::from_raw_fd(duplicate) };
        let local = Addr::local(duplicate.as_fd())
            .ok()
            .and_then(|local| local.into_std().ok());
        if local == Some(address)
            && Backend::is_listening::<Errno>(duplicate.as_fd()).unwrap_or(false)
        {
            return Ok(duplicate);
        }
    }
    Err(io::Error::new(io::ErrorKind::NotFound, "bound listener"))
}
