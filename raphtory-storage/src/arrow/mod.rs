use polars_core::utils::arrow::array::{MutablePrimitiveArray, MutableStructArray};

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
