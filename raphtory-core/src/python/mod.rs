use crate::storage::locked_view::LockedView;
use raphtory_api::python::repr::Repr;
use std::ops::Deref;

mod time;

impl<'a, T: Repr> Repr for LockedView<'a, T> {
    fn repr(&self) -> String {
        self.deref().repr()
    }
}
