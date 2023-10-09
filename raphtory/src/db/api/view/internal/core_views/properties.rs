use crate::{core::entities::properties::tprop::TProp, prelude::TimeIndexEntry};
use std::ops::Range;

pub enum TPropView<'a> {
    TProp(&'a TProp),
    Arrow,
}

impl<'a> TPropView<'a> {
    pub fn range(&self, w: Range<TimeIndexEntry>) -> TPropView {
        todo!()
    }

    pub fn active(&self, w: Range<TimeIndexEntry>) -> bool {
        todo!()
    }
}

impl<'a> From<&'a TProp> for TPropView<'a> {
    fn from(value: &'a TProp) -> Self {
        Self::TProp(value)
    }
}
