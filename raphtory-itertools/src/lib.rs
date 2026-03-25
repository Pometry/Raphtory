pub(crate) mod merge;
pub(crate) mod merge_impl;
mod take;

pub use merge::{FastMerge, FastMergeExt};
pub use take::{ReTake, TakeExt};
