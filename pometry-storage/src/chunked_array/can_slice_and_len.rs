use crate::{
    arrow2::{
        array::{Array, PrimitiveArray, StructArray, Utf8Array},
        buffer::Buffer,
        types::{NativeType, Offset},
    },
    chunked_array::row::Row,
};
use polars_arrow::{array::BooleanArray, offset::OffsetsBuffer};

pub trait CanSliceAndLen: Clone + Send + Sync {
    fn slice(&mut self, start: usize, len: usize);
    fn len(&self) -> usize;
}

impl<T: NativeType> CanSliceAndLen for PrimitiveArray<T> {
    fn slice(&mut self, start: usize, len: usize) {
        self.slice(start, len);
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl CanSliceAndLen for BooleanArray {
    fn slice(&mut self, start: usize, len: usize) {
        self.slice(start, len);
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl<I: Offset> CanSliceAndLen for Utf8Array<I> {
    fn slice(&mut self, start: usize, len: usize) {
        self.slice(start, len);
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl CanSliceAndLen for StructArray {
    fn slice(&mut self, start: usize, len: usize) {
        self.slice(start, len);
    }

    fn len(&self) -> usize {
        <Self as Array>::len(self)
    }
}

impl<T: Clone + Send + Sync> CanSliceAndLen for Buffer<T> {
    fn slice(&mut self, start: usize, len: usize) {
        self.slice(start, len);
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl<O: Offset> CanSliceAndLen for OffsetsBuffer<O> {
    fn slice(&mut self, start: usize, len: usize) {
        self.slice(start, len)
    }

    fn len(&self) -> usize {
        self.len_proxy()
    }
}

pub trait AsIter<'a> {
    type Item;
    fn as_iter(&'a self) -> impl Iterator<Item = Self::Item>;
}

impl<'a> AsIter<'a> for BooleanArray {
    type Item = Option<bool>;

    fn as_iter(&'a self) -> impl Iterator<Item = Self::Item> {
        self.into_iter()
    }
}

impl<'a, T: NativeType> AsIter<'a> for Buffer<T> {
    type Item = T;

    fn as_iter(&'a self) -> impl Iterator<Item = Self::Item> {
        self.as_slice().iter().copied()
    }
}

impl<'a, T: NativeType> AsIter<'a> for PrimitiveArray<T> {
    type Item = Option<T>;

    fn as_iter(&'a self) -> impl Iterator<Item = Self::Item> {
        self.into_iter().map(|v| v.copied())
    }
}

impl<'a> AsIter<'a> for StructArray {
    type Item = Row<'a>;

    fn as_iter(&'a self) -> impl Iterator<Item = Self::Item> {
        (0..<StructArray as Array>::len(self)).map(|i| Row::new(self, i))
    }
}

impl<'a, I: Offset> AsIter<'a> for Utf8Array<I> {
    type Item = Option<&'a str>;
    fn as_iter(&'a self) -> impl Iterator<Item = Self::Item> {
        self.into_iter()
    }
}

impl<'a, I: Offset> AsIter<'a> for OffsetsBuffer<I> {
    type Item = (usize, usize);

    fn as_iter(&'a self) -> impl Iterator<Item = Self::Item> {
        self.windows(2).map(|w| (w[0].to_usize(), w[1].to_usize()))
    }
}
