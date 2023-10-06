use crate::{core::storage::timeindex::TimeIndex, prelude::TimeIndexEntry};
use enum_dispatch::enum_dispatch;

#[enum_dispatch]
pub trait TimeIndexViewOps {}

#[enum_dispatch]
pub enum TimeIndexView<'a> {
    Indexed(TimeIndex<TimeIndexEntry>),
    #[cfg(feature = "arrow")]
    Sorted(&'a [i64]),
}
