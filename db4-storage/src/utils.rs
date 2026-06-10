use iter_enum::{
    DoubleEndedIterator, ExactSizeIterator, FusedIterator, IndexedParallelIterator, Iterator,
    ParallelIterator,
};

#[derive(
    Clone,
    Debug,
    Iterator,
    DoubleEndedIterator,
    ExactSizeIterator,
    ParallelIterator,
    IndexedParallelIterator,
    FusedIterator,
)]
pub enum Iter2<I1, I2> {
    I1(I1),
    I2(I2),
}

#[derive(
    Copy,
    Clone,
    Iterator,
    ExactSizeIterator,
    DoubleEndedIterator,
    ParallelIterator,
    IndexedParallelIterator,
    FusedIterator,
)]
pub enum Iter3<I, J, K> {
    I(I),
    J(J),
    K(K),
}

#[derive(
    Copy,
    Clone,
    Iterator,
    ExactSizeIterator,
    DoubleEndedIterator,
    ParallelIterator,
    IndexedParallelIterator,
    FusedIterator,
)]
pub enum Iter4<I, J, K, L> {
    I(I),
    J(J),
    K(K),
    L(L),
}
