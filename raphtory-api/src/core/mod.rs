use iter_enum::{
    DoubleEndedIterator, ExactSizeIterator, FusedIterator, IndexedParallelIterator, Iterator,
    ParallelExtend, ParallelIterator,
};
use serde::{Deserialize, Serialize};

pub mod entities;
pub mod input;
pub mod storage;
pub mod utils;

/// Denotes the direction of an edge. Can be incoming, outgoing or both.
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Debug, Default, Serialize, Deserialize)]
pub enum Direction {
    OUT,
    IN,
    #[default]
    BOTH,
}

#[derive(
    Iterator,
    DoubleEndedIterator,
    ExactSizeIterator,
    FusedIterator,
    ParallelIterator,
    ParallelExtend,
    IndexedParallelIterator,
)]
pub enum DirectionVariants<Out, In, Both> {
    Out(Out),
    In(In),
    Both(Both),
}
