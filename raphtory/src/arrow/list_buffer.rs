use arrow2::{
    array::{ListArray, PrimitiveArray, StructArray},
    buffer::Buffer,
    offset::OffsetsBuffer,
    types::NativeType,
};
use itertools::Itertools;
use std::ops::Index;

pub struct ListColumn<'a, T> {
    values: &'a Buffer<T>,
    offsets: &'a OffsetsBuffer<i64>,
}

pub fn as_primitive_column<T: NativeType>(
    list: &ListArray<i64>,
    col: usize,
) -> Option<ListColumn<T>> {
    let values = list.values().as_any().downcast_ref::<StructArray>()?;
    let column = values.values().get(col)?;
    let primitive = column.as_any().downcast_ref::<PrimitiveArray<T>>()?;
    let values = primitive.values();
    let offsets = list.offsets();
    Some(ListColumn { values, offsets })
}

impl<'a, T: NativeType> ListColumn<'a, T> {
    pub(crate) fn new(list: &'a ListArray<i64>, col: usize) -> Option<ListColumn<'a, T>> {
        as_primitive_column(list, col)
    }

    pub(crate) fn value(&self, index: usize) -> Buffer<T> {
        let mut new = self.values.clone();
        let offset = self.offsets[index] as usize;
        let length = self.offsets[index + 1] as usize - offset;
        new.slice(offset, length);
        new
    }

    pub fn into_value(self, index: usize) -> &'a [T] {
        &self.values.as_slice()[self.offsets[index] as usize..self.offsets[index + 1] as usize]
    }

    pub fn iter(&self) -> impl Iterator<Item = &[T]> + '_ {
        self.offsets
            .windows(2)
            .map(|o| &self.values[o[0] as usize..o[1] as usize])
    }
}

impl<'a, T: NativeType> Index<usize> for ListColumn<'a, T> {
    type Output = [T];

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.values.as_slice()[self.offsets[index] as usize..self.offsets[index + 1] as usize]
    }
}

#[cfg(test)]
mod test_list_column {
    use crate::arrow::{
        adj_schema,
        list_buffer::{as_primitive_column, ListColumn},
    };
    use arrow2::{
        array::{ListArray, PrimitiveArray, StructArray},
        offset::{Offsets, OffsetsBuffer},
    };

    #[test]
    fn test() {
        let offsets: Vec<i64> = vec![0, 3, 10];
        let values: Vec<u64> = (0..10).collect();

        let dtype = <ListArray<i64>>::default_datatype(adj_schema());
        let offsets = OffsetsBuffer::from(Offsets::try_from(offsets).unwrap());
        let vid_array = PrimitiveArray::from_vec(values.clone()).boxed();
        let eid_array = PrimitiveArray::from_vec(values).boxed();
        let values = StructArray::new(adj_schema(), vec![vid_array, eid_array], None).boxed();

        let inbound_array = ListArray::new(dtype, offsets, values, None);
        let vids: ListColumn<u64> = as_primitive_column(&inbound_array, 0).unwrap();
        assert_eq!(vids[0], [0, 1, 2]);
        assert_eq!(vids[1], [3, 4, 5, 6, 7, 8, 9]);
    }
}