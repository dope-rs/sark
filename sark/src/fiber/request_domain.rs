use std::pin::Pin;
use std::task::Poll;

use dope_fiber::{Context, Fiber};

#[pin_project::pin_project]
pub struct RequestDomain<F, O> {
    #[pin]
    fiber: F,
    owner: O,
}

impl<F, O> RequestDomain<F, O> {
    pub fn new(fiber: F, owner: O) -> Self {
        Self { fiber, owner }
    }
}

impl<'d, F, O> Fiber<'d> for RequestDomain<F, O>
where
    F: Fiber<'d>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        Fiber::poll(self.project().fiber, context)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::RequestDomain;

    struct DropLog {
        name: &'static str,
        log: Rc<RefCell<Vec<&'static str>>>,
    }

    impl Drop for DropLog {
        fn drop(&mut self) {
            self.log.borrow_mut().push(self.name);
        }
    }

    #[test]
    fn drops_fiber_before_owner() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let domain = RequestDomain::new(
            DropLog {
                name: "fiber",
                log: Rc::clone(&log),
            },
            DropLog {
                name: "owner",
                log: Rc::clone(&log),
            },
        );

        drop(domain);

        assert_eq!(&*log.borrow(), &["fiber", "owner"]);
    }

    #[test]
    fn zero_sized_owner_adds_no_storage() {
        assert_eq!(
            size_of::<RequestDomain<[usize; 4], ()>>(),
            size_of::<[usize; 4]>(),
        );
    }
}
