use crate::core::timeindex::TimeIndex;

use super::props::Props;


pub(crate) struct EdgeStore<const N: usize> {
    src: usize,
    dst: usize,
    // time_index: TimeIndex,
    props: Props
}
