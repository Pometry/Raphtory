use super::array_ops::{
    IntoNonNullPrimitiveCol, IntoPrimitiveCol, IntoUtf8Col, NonNullPrimitiveCol,
};
use crate::{
    arrow2::{datatypes::ArrowDataType as DataType, offset::Offset, types::NativeType},
    chunked_array::{
        array_ops::{normalized_range, ArrayOps, BaseArrayOps, PrimitiveCol},
        can_slice_and_len::CanSliceAndLen,
        chunked_offsets::{ChunkedOffsets, ExplodedIndexIter},
        slice::ChunkedArraySlice,
    },
    prelude::{Chunked, IntoBoolCol},
};
use rayon::prelude::*;
use std::{
    fmt::{Debug, Formatter},
    marker::PhantomData,
    ops::{Range, RangeBounds},
};

#[derive(Clone, Default, PartialEq)]
pub struct ChunkedListArray<'a, A> {
    values: A,
    offsets: ChunkedOffsets,
    _marker: PhantomData<&'a A>,
}

impl<'a, A: Debug> Debug for ChunkedListArray<'a, A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkedListArray")
            .field("values", &self.values)
            .field("offsets", &self.offsets)
            .finish()
    }
}

impl<'a, A: ArrayOps<'a>> ChunkedListArray<'a, A> {
    pub fn new_from_parts(values: A, offsets: ChunkedOffsets) -> Self {
        let last_offset: usize = offsets.last();
        assert!(
            last_offset <= values.len(),
            "offsets out of bounds {} <= {}, offsets_len {}",
            last_offset,
            values.len(),
            offsets.len()
        );
        Self {
            values,
            offsets,
            _marker: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn data_type(&self) -> Option<&DataType> {
        self.values.data_type()
    }

    pub fn values(&self) -> &A {
        &self.values
    }

    pub fn offsets(&self) -> &ChunkedOffsets {
        &self.offsets
    }

    pub fn value(&'a self, i: usize) -> ChunkedArraySlice<'a, A::Slice> {
        let (start, end) = self.offsets.start_end(i);
        self.values.slice(start..end)
    }

    pub fn into_value(self, i: usize) -> ChunkedArraySlice<'a, A::OwnedSlice> {
        let (start, end) = self.offsets.start_end(i);
        self.values.sliced(start..end)
    }

    pub fn par_indexed(
        &'a self,
    ) -> impl IndexedParallelIterator<Item = (ChunkedArraySlice<'a, A::Slice>, Range<usize>)> + 'a
    {
        (0..self.len()).into_par_iter().map(|i| {
            let (start, end) = self.offsets().start_end(i);
            (self.value(i), start..end)
        })
    }

    pub fn par_exploded_indexed(
        &'a self,
    ) -> impl IndexedParallelIterator<Item = (usize, A::Value)> + 'a
    where
        ChunkedArraySlice<'a, A::Slice>: IntoParallelIterator<Item = A::Value>,
        <ChunkedArraySlice<'a, A::Slice> as IntoParallelIterator>::Iter: IndexedParallelIterator,
    {
        self.offsets.par_iter_exploded().zip(self.values.par_iter())
    }

    pub fn par_values(
        &'a self,
    ) -> impl IndexedParallelIterator<Item = ChunkedArraySlice<'a, A::Slice>> + 'a {
        (0..self.len()).into_par_iter().map(|i| self.value(i))
    }

    pub fn iter_indexed_chunks(
        &'a self,
    ) -> impl Iterator<Item = (ExplodedIndexIter<'a>, &'a <A as ArrayOps<'a>>::Chunk)>
    where
        A: Chunked<Chunk = <A as ArrayOps<'a>>::Chunk>,
    {
        self.values
            .iter_chunks()
            .enumerate()
            .map(|(chunk_id, chunk)| {
                let start = chunk_id * self.values.chunk_size();
                let end = start + chunk.len();
                (ExplodedIndexIter::range(self.offsets(), start..end), chunk)
            })
    }
}

impl<'a, A: ArrayOps<'a>> BaseArrayOps<'a> for ChunkedListArray<'a, A> {
    type BaseSlice = &'a Self;
    type OwnedBaseSlice = Self;

    fn len(&self) -> usize {
        self.len()
    }

    fn data_type(&self) -> Option<&DataType> {
        self.values.data_type()
    }

    fn slice<R: RangeBounds<usize>>(&'a self, range: R) -> ChunkedArraySlice<'a, Self::BaseSlice> {
        let range = normalized_range(range, self.len());
        ChunkedArraySlice::new(self, range)
    }

    fn sliced<R: RangeBounds<usize>>(
        self,
        range: R,
    ) -> ChunkedArraySlice<'a, Self::OwnedBaseSlice> {
        let range = normalized_range(range, self.len());
        ChunkedArraySlice::new(self, range)
    }
}

impl<'a, A: ArrayOps<'a> + 'a> IntoIterator for ChunkedListArray<'a, A> {
    type Item = ChunkedArraySlice<'a, A::OwnedSlice>;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + Send + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new((0..self.len()).map(move |index| self.clone().into_value(index)))
    }
}

// impl<'a, A: ArrayOps<'a> + 'a> ArrayOps<'a> for ChunkedListArray<'a, A> {
//     type Value = ChunkedArraySlice<'a, A::Slice>;
//     type IntoValue = ChunkedArraySlice<'a, A::OwnedSlice>;
//     type Slice = Self::BaseSlice;
//     type OwnedSlice = Self::OwnedBaseSlice;
//     type Iter = Box<dyn Iterator<Item = Self::Value> + Send + 'a>;

//     fn get(&'a self, i: usize) -> Self::Value {
//         let (start, end) = self.offsets.start_end(i);
//         self.values.slice(start..end)
//     }

//     fn into_get(self, i: usize) -> Self::IntoValue {
//         let (start, end) = self.offsets.start_end(i);
//         self.values.sliced(start..end)
//     }

//     fn iter(&'a self) -> Self::Iter {
//         Box::new((0..self.len()).map(|i| self.get(i)))
//     }

//     fn iter_range(
//         &'a self,
//         range: Range<usize>,
//     ) -> impl Iterator<Item = Self::Value> + ExactSizeIterator + DoubleEndedIterator {
//         range.map(|i| self.get(i))
//     }
// }

impl<'a, A: PrimitiveCol<'a> + 'a> ChunkedListArray<'a, A> {
    pub fn primitive_col<T: NativeType>(
        &'a self,
        col_idx: usize,
    ) -> Option<ChunkedListArray<'a, A::Column<T>>> {
        let values = self.values.primitive_col(col_idx)?;
        let offsets = self.offsets.clone();
        Some(ChunkedListArray::new_from_parts(values, offsets))
    }
}

impl<'a, A: IntoPrimitiveCol<'a> + 'a> ChunkedListArray<'a, A> {
    pub fn into_primitive_col<T: NativeType>(
        self,
        col_idx: usize,
    ) -> Option<ChunkedListArray<'a, A::Column<T>>> {
        let values = self.values.into_primitive_col(col_idx)?;
        let offsets = self.offsets.clone();
        Some(ChunkedListArray::new_from_parts(values, offsets))
    }
}

impl<'a, A: NonNullPrimitiveCol<'a> + 'a> ChunkedListArray<'a, A> {
    pub fn non_null_primitive_col<T: NativeType>(
        &'a self,
        col_idx: usize,
    ) -> Option<ChunkedListArray<'a, A::Column<T>>> {
        let values = self.values.non_null_primitive_col(col_idx)?;
        let offsets = self.offsets.clone();
        Some(ChunkedListArray::new_from_parts(values, offsets))
    }
}
impl<'a, A: IntoNonNullPrimitiveCol<'a> + 'a> ChunkedListArray<'a, A> {
    pub fn into_non_null_primitive_col<T: NativeType>(
        self,
        col_idx: usize,
    ) -> Option<ChunkedListArray<'a, A::Column<T>>> {
        let values = self.values.into_non_null_primitive_col(col_idx)?;
        let offsets = self.offsets;
        Some(ChunkedListArray::new_from_parts(values, offsets))
    }
}

impl<'a, A: IntoUtf8Col<'a> + 'a> ChunkedListArray<'a, A> {
    pub fn into_utf8_col<I: Offset>(
        self,
        col_idx: usize,
    ) -> Option<ChunkedListArray<'a, A::Column<I>>> {
        let values = self.values.into_utf8_col(col_idx)?;
        let offsets = self.offsets;
        Some(ChunkedListArray::new_from_parts(values, offsets))
    }
}

impl<'a, A: 'a> ChunkedListArray<'a, A>
where
    &'a A: IntoUtf8Col<'a>,
{
    pub fn utf8_col<I: Offset>(
        &'a self,
        col_idx: usize,
    ) -> Option<ChunkedListArray<'a, <&'a A as IntoUtf8Col<'a>>::Column<I>>> {
        let values = (&self.values).into_utf8_col(col_idx)?;
        let offsets = self.offsets.clone();
        Some(ChunkedListArray::new_from_parts(values, offsets))
    }
}

impl<'a, A: 'a> ChunkedListArray<'a, A>
where
    &'a A: IntoBoolCol<'a>,
{
    pub fn bool_col(
        &'a self,
        col_idx: usize,
    ) -> Option<ChunkedListArray<'a, <&'a A as IntoBoolCol<'a>>::Column>> {
        let values = (&self.values).into_bool_col(col_idx)?;
        let offsets = self.offsets.clone();
        Some(ChunkedListArray::new_from_parts(values, offsets))
    }
}
