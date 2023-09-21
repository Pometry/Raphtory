use polars_core::utils::arrow::{
    array::{
        MutableArray, MutableListArray, MutablePrimitiveArray, MutableStructArray, MutableUtf8Array,
    },
    types::NativeType,
};
use raphtory::core::{ArcStr, PropType};

pub(crate) mod columnar_graph;

type MPArr<T> = MutablePrimitiveArray<T>;

#[repr(transparent)]
struct MutEdgePair<'a>(&'a mut MutableStructArray);

impl<'a> MutEdgePair<'a> {
    fn add_pair(&mut self, v: u64, e: u64) {
        self.0.value::<MPArr<u64>>(0).unwrap().push(Some(v));
        self.0.value::<MPArr<u64>>(1).unwrap().push(Some(e));
        self.0.push(true);
    }

    fn add_pair_usize(&mut self, v_id: usize, e_id: usize) {
        self.add_pair(v_id as u64, e_id as u64);
    }
}

impl<'a> From<&'a mut MutableStructArray> for MutEdgePair<'a> {
    fn from(arr: &'a mut MutableStructArray) -> Self {
        Self(arr)
    }
}

pub(crate) struct MutTemporalPropColumn {
    top_col_name: ArcStr,
    temporal_props: MutableListArray<i64, MutableStructArray>,
}

impl MutTemporalPropColumn {
    pub(crate) fn new(
        top_col_name: &str,
        temporal_props: MutableListArray<i64, MutableStructArray>,
    ) -> Self {
        Self {
            top_col_name: top_col_name.into(),
            temporal_props,
        }
    }

    // fn as_mut_array<T: NativeType>(&mut self) -> Option<&mut MPArr<T>> {
    //     self.arr.as_mut_any().downcast_mut::<MPArr<T>>()
    // }

    // fn as_utf8_mut(&mut self) -> Option<&mut MutableUtf8Array<i64>> {
    //     self.arr
    //         .as_mut_any()
    //         .downcast_mut::<MutableUtf8Array<i64>>()
    // }
}
