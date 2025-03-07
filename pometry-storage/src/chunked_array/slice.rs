use crate::{
    arrow2::{
        datatypes::ArrowDataType as DataType,
        types::{NativeType, Offset},
    },
    chunked_array::{
        array_ops::{ArrayOps, BaseArrayOps},
        chunked_iter::IntoChunkedIter,
    },
    prelude::{Chunked, IntoBoolCol, IntoPrimitiveCol, IntoUtf8Col},
};
use rayon::{
    iter::plumbing::{bridge, Consumer, Producer, ProducerCallback, UnindexedConsumer},
    prelude::*,
};
use std::{
    cmp::min,
    collections::Bound,
    fmt::{Debug, Formatter},
    marker::PhantomData,
    ops::{Range, RangeBounds},
};

/// Traits for implementing Array operations on chunked arrays
///
/// This is split into two parts as the Box<dyn Array> does not have a way to get concrete values.

pub trait GivesItem<'a> {
    type Item;
}

#[derive(Copy, Clone)]
pub struct ChunkedArraySlice<'a, A> {
    array: A,
    pub(crate) start: usize,
    end: usize,
    marker: PhantomData<&'a A>,
}

impl<'a, A: Debug> Debug for ChunkedArraySlice<'a, A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkedArraySlice")
            .field("array", &self.array)
            .field("start", &self.start)
            .field("end", &self.end)
            .finish()
    }
}

pub trait DoubleEndedExactSizeIterator: Iterator + DoubleEndedIterator + ExactSizeIterator {}

impl<A: Iterator + DoubleEndedIterator + ExactSizeIterator> DoubleEndedExactSizeIterator for A {}

fn subrange<R: RangeBounds<usize>>(old: &Range<usize>, new: R) -> Range<usize> {
    let new_start = match new.start_bound() {
        Bound::Included(new_start) => old.start.saturating_add(*new_start),
        Bound::Excluded(new_start) => old.start.saturating_add(*new_start).saturating_add(1),
        Bound::Unbounded => old.start,
    };
    let new_end = match new.end_bound() {
        Bound::Included(new_end) => old.start.saturating_add(*new_end).saturating_add(1),
        Bound::Excluded(new_end) => old.start.saturating_add(*new_end),
        Bound::Unbounded => old.end,
    };
    let start = min(new_start, old.end);
    let end = min(new_end, old.end);
    start..end
}

impl<'a, A> ChunkedArraySlice<'a, A> {
    fn resolve_index(&self, i: usize) -> usize {
        assert!(
            i < self.range().len(),
            "index out of bounds {} {:?}",
            i,
            self.range()
        );
        self.start + i
    }

    pub fn range(&self) -> Range<usize> {
        self.start..self.end
    }
}

impl<'b, 'a: 'b, A: BaseArrayOps<'a> + 'a> ChunkedArraySlice<'b, &'b A> {
    pub fn into_owned(self) -> ChunkedArraySlice<'a, A> {
        let range = self.range();
        let array = self.array.clone();
        ChunkedArraySlice::new(array, range)
    }
}

impl<'a, A: BaseArrayOps<'a> + 'a> BaseArrayOps<'a> for ChunkedArraySlice<'a, A> {
    type BaseSlice = A::BaseSlice;
    type OwnedBaseSlice = A::OwnedBaseSlice;

    fn len(&self) -> usize {
        self.range().len()
    }

    fn data_type(&self) -> Option<&DataType> {
        self.array.data_type()
    }

    fn is_empty(&self) -> bool {
        self.range().is_empty()
    }

    fn slice<R: RangeBounds<usize>>(&'a self, range: R) -> ChunkedArraySlice<'a, Self::BaseSlice> {
        let range = subrange(&self.range(), range);
        self.array.slice(range)
    }

    fn sliced<R: RangeBounds<usize>>(
        self,
        range: R,
    ) -> ChunkedArraySlice<'a, Self::OwnedBaseSlice> {
        let range = subrange(&self.range(), range);
        self.array.sliced(range)
    }
}

impl<'a, A: ArrayOps<'a> + Chunked<Chunk = <A as ArrayOps<'a>>::Chunk> + Send + Sync + 'a>
    ArrayOps<'a> for ChunkedArraySlice<'a, A>
{
    type Value = A::Value;
    type IntoValue = A::IntoValue;
    type Chunk = <A as ArrayOps<'a>>::Chunk;
    type Slice = Self::BaseSlice;
    type OwnedSlice = Self::OwnedBaseSlice;

    fn get(&'a self, i: usize) -> Self::Value {
        let index = self.resolve_index(i);
        self.array.get(index)
    }

    fn into_get(self, i: usize) -> Self::IntoValue {
        let index = self.resolve_index(i);
        self.array.into_get(index)
    }

    fn iter_chunks(
        &'a self,
    ) -> impl DoubleEndedIterator<Item = &'a Self::Chunk> + ExactSizeIterator {
        self.array.iter_chunks_range(self.range())
    }
}

impl<'a, A: IntoPrimitiveCol<'a>> ChunkedArraySlice<'a, A> {
    pub fn into_primitive_col<T: NativeType>(
        self,
        col_idx: usize,
    ) -> Option<ChunkedArraySlice<'a, A::Column<T>>> {
        let range = self.range();
        let array = self.array.into_primitive_col::<T>(col_idx)?;
        Some(ChunkedArraySlice::new(array, range))
    }
}

impl<'a, A: IntoBoolCol<'a>> ChunkedArraySlice<'a, A> {
    pub fn into_bool_col(self, col_idx: usize) -> Option<ChunkedArraySlice<'a, A::Column>> {
        let range = self.range();
        let array = self.array.into_bool_col(col_idx)?;
        Some(ChunkedArraySlice::new(array, range))
    }
}

impl<'a, A: IntoUtf8Col<'a>> ChunkedArraySlice<'a, A> {
    pub fn into_utf8_col<I: Offset>(
        self,
        col_idx: usize,
    ) -> Option<ChunkedArraySlice<'a, A::Column<I>>> {
        let range = self.range();
        let array = self.array.into_utf8_col::<I>(col_idx)?;
        Some(ChunkedArraySlice::new(array, range))
    }
}

impl<
        'a,
        A: ArrayOps<'a> + Chunked<Chunk = <A as ArrayOps<'a>>::Chunk> + 'a + IntoChunkedIter<'a>,
    > ChunkedArraySlice<'a, A>
{
    pub fn into_par_iter_chunks(
        self,
    ) -> impl IndexedParallelIterator<Item = &'a <A as Chunked>::Chunk> + 'a {
        let range = self.range();
        self.array.into_par_iter_chunks_range(range)
    }
}

impl<'a, A: ArrayOps<'a> + 'a> Iterator for ChunkedArraySlice<'a, A> {
    type Item = A::IntoValue;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.range().next()?;
        self.start = i + 1;
        Some(self.array.clone().into_get(i))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.range().len();
        (len, Some(len))
    }
}

impl<'a, A: ArrayOps<'a> + 'a> DoubleEndedIterator for ChunkedArraySlice<'a, A> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let i = self.range().next_back()?;
        self.end = i;
        Some(self.array.clone().into_get(i))
    }
}

impl<'a, A: ArrayOps<'a> + 'a> ExactSizeIterator for ChunkedArraySlice<'a, A> {}

impl<'a, A: BaseArrayOps<'a>> ChunkedArraySlice<'a, A> {
    pub fn new(array: A, range: Range<usize>) -> Self {
        Self {
            array,
            start: range.start,
            end: range.end,
            marker: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        BaseArrayOps::len(self)
    }
}

// impl<'a, A: PrimitiveCol<'a>> PrimitiveCol<'a> for ChunkedArraySlice<'a, A> {
//     type Column<T: NativeType> = ChunkedArraySlice<'a, A::Column<T>>;

//     fn primitive_col<T: NativeType>(&'a self, col_idx: usize) -> Option<Self::Column<T>> {
//         let array = self.array.primitive_col(col_idx)?;
//         Some(ChunkedArraySlice::new(array, self.range.clone()))
//     }
// }

// impl<'a, A: IntoPrimitiveCol<'a>> IntoPrimitiveCol<'a> for ChunkedArraySlice<'a, A> {
//     type Column<T: NativeType> = ChunkedArraySlice<'a, A::Column<T>>;

//     fn into_primitive_col<T: NativeType>(self, col_idx: usize) -> Option<Self::Column<T>> {
//         let array = self.array.into_primitive_col(col_idx)?;
//         Some(ChunkedArraySlice::new(array, self.range.clone()))
//     }
// }

// impl<'a, A: IntoUtf8Col<'a>> IntoUtf8Col<'a> for ChunkedArraySlice<'a, A> {
//     type Column<I> = ChunkedArraySlice<'a, A::Column<I>> where I: Offset+ 'a;

//     fn into_utf8_col<I: Offset>(self, col_idx: usize) -> Option<Self::Column<I>> {
//         let array = self.array.into_utf8_col(col_idx)?;
//         Some(ChunkedArraySlice::new(array, self.range.clone()))
//     }
// }

// impl<'a, A: IntoNonNullPrimitiveCol<'a>> IntoNonNullPrimitiveCol<'a> for ChunkedArraySlice<'a, A> {
//     type Column<T: NativeType> = ChunkedArraySlice<'a, A::Column<T>>;

//     fn into_non_null_primitive_col<T: NativeType>(self, col_idx: usize) -> Option<Self::Column<T>> {
//         let array = self.array.into_non_null_primitive_col(col_idx)?;
//         Some(ChunkedArraySlice::new(array, self.range.clone()))
//     }
// }

// impl<'a, A: NonNullPrimitiveCol<'a>> NonNullPrimitiveCol<'a> for ChunkedArraySlice<'a, A> {
//     type Column<T: NativeType> = ChunkedArraySlice<'a, A::Column<T>>;

//     fn non_null_primitive_col<T: NativeType>(&'a self, col_idx: usize) -> Option<Self::Column<T>> {
//         let array = self.array.non_null_primitive_col(col_idx)?;
//         Some(ChunkedArraySlice::new(array, self.range.clone()))
//     }
// }

impl<'a, A> IntoParallelIterator for ChunkedArraySlice<'a, A>
where
    A: ArrayOps<'a, OwnedSlice = A>,
    <A as ArrayOps<'a>>::IntoValue: Send,
{
    type Iter = IntoParIter<'a, A>;

    type Item = A::IntoValue;

    fn into_par_iter(self) -> Self::Iter {
        IntoParIter(self)
    }
}

pub struct IntoParIter<'a, A>(ChunkedArraySlice<'a, A>);

impl<'a, A> ParallelIterator for IntoParIter<'a, A>
where
    A: ArrayOps<'a, OwnedSlice = A>,
    <A as ArrayOps<'a>>::IntoValue: Send,
{
    type Item = A::IntoValue;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(self.0.len())
    }
}

impl<'a, A> IndexedParallelIterator for IntoParIter<'a, A>
where
    A: ArrayOps<'a, OwnedSlice = A>,
    <A as ArrayOps<'a>>::IntoValue: Send,
{
    fn len(&self) -> usize {
        self.0.len()
    }

    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        bridge(self, consumer)
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        let producer = ChunkedProducer(self.0);
        callback.callback(producer)
    }
}

struct ChunkedProducer<'a, A>(ChunkedArraySlice<'a, A>);

impl<'a, A> Producer for ChunkedProducer<'a, A>
where
    A: ArrayOps<'a, OwnedSlice = A>,
    <A as ArrayOps<'a>>::IntoValue: Send,
{
    type Item = A::IntoValue;
    type IntoIter = ChunkedArraySlice<'a, A::OwnedSlice>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.sliced(..)
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let left = self.0.clone().sliced(0..index);
        let right = self.0.sliced(index..);

        (ChunkedProducer(left), ChunkedProducer(right))
    }
}
