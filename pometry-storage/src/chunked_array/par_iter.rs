use crate::arrow2::{array::PrimitiveArray, trusted_len::TrustedLen, types::NativeType};

use rayon::iter::{
    plumbing::{bridge, Consumer, Producer, ProducerCallback, UnindexedConsumer},
    IndexedParallelIterator, ParallelIterator,
};

#[derive(Clone, Debug)]
pub(crate) struct PArrayWrap<T: NativeType>(pub PrimitiveArray<T>);

impl<T: NativeType> PArrayWrap<T> {
    pub(crate) fn into_par_iter(self) -> ParDataIter<T> {
        ParDataIter::new(self.0)
    }
}

pub struct ParDataIter<T: NativeType> {
    arr: PrimitiveArray<T>,
}

impl<T: NativeType> ParDataIter<T> {
    pub fn new(arr: PrimitiveArray<T>) -> Self {
        Self { arr }
    }
}

impl<T: NativeType> ParallelIterator for ParDataIter<T> {
    type Item = Option<T>;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(self.arr.len())
    }
}

impl<T: NativeType> IndexedParallelIterator for ParDataIter<T> {
    fn len(&self) -> usize {
        self.arr.len()
    }

    fn drive<C>(self, consumer: C) -> C::Result
    where
        C: Consumer<Self::Item>,
    {
        bridge(self, consumer)
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        let producer = DataProducer::from(self);
        callback.callback(producer)
    }
}

struct DataProducer<T: NativeType> {
    arr: PrimitiveArray<T>,
}

impl<T: NativeType> Producer for DataProducer<T> {
    type Item = Option<T>;

    type IntoIter = BoxedZipValidity<T>;

    fn into_iter(self) -> Self::IntoIter {
        BoxedZipValidity::new(self.arr.into_iter())
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let arr_len = self.arr.len();

        let left = self.arr.clone().sliced(0, index);
        let right = self.arr.sliced(index, arr_len - index);
        (DataProducer { arr: left }, DataProducer { arr: right })
    }
}

impl<T: NativeType> From<ParDataIter<T>> for DataProducer<T> {
    fn from(iterator: ParDataIter<T>) -> Self {
        Self { arr: iterator.arr }
    }
}

struct BoxedZipValidity<T> {
    iter: Box<dyn DoubleEndedIterator<Item = Option<T>>>,
}

impl<T> BoxedZipValidity<T> {
    fn new<I: DoubleEndedIterator<Item = Option<T>> + TrustedLen + 'static>(iter: I) -> Self {
        Self {
            iter: Box::new(iter),
        }
    }
}

impl<T> Iterator for BoxedZipValidity<T> {
    type Item = Option<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.iter.nth(n)
    }
}

impl<T> DoubleEndedIterator for BoxedZipValidity<T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}

impl<T> ExactSizeIterator for BoxedZipValidity<T> {
    fn len(&self) -> usize {
        self.size_hint().0
    }
}

#[cfg(test)]
mod test {

    use crate::arrow2::array::PrimitiveArray;
    use rayon::prelude::*;

    use super::*;

    #[test]
    fn test_par_iter() {
        let arr = PrimitiveArray::<i32>::from_slice(&[1, 2, 3, 4, 5]);
        let iter = PArrayWrap(arr).into_par_iter().with_min_len(2);
        let v = iter.collect::<Vec<_>>();
        assert_eq!(v, vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
    }
}
