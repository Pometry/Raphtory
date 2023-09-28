use arrow2::{
    array::{
        MutableArray, MutableBooleanArray, MutableListArray, MutablePrimitiveArray,
        MutableStructArray, MutableUtf8Array,
    },
    datatypes::DataType,
    types::NativeType,
};
use itertools::{EitherOrBoth, Itertools};
use raphtory::{
    core::entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
    prelude::{GraphViewOps, Prop},
};

pub mod col_graph2;
pub(crate) mod columnar_graph;
pub(crate) mod edge_frame_builder;
pub(crate) mod mmap;
pub(crate) mod vertex_frame_builder;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow2::error::Error),
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
    #[error("Bad data type for vertex column: {0:?}")]
    DType(DataType),
    #[error("Graph directory is not empty before loading")]
    GraphDirNotEmpty
}

const FRAGMENT_ROW_COUNT: usize = 100_000;

const OUTBOUND_COLUMN: &str = "outbound";
const INBOUND_COLUMN: &str = "inbound";

const V_ADDITIONS_COLUMN: &str = "additions";
const E_ADDITIONS_COLUMN: &str = "additions";
const E_DELETIONS_COLUMN: &str = "deletions";

const NAME_COLUMN: &str = "name";
const TEMPORAL_PROPS_COLUMN: &str = "t_props";

const GID_COLUMN: &str = "global_vertex_id";
const SRC_COLUMN: &str = "src";
const DST_COLUMN: &str = "dst";

pub(crate) const V_COLUMN: &str = "v";
pub(crate) const E_COLUMN: &str = "e";

type MPArr<T> = MutablePrimitiveArray<T>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum GID{
    Num(u64),
    Str(String),
}

impl From<u64> for GID {
    fn from(id: u64) -> Self {
        Self::Num(id)
    }
}

impl From<String> for GID {
    fn from(id: String) -> Self {
        Self::Str(id)
    }
}

impl From<&str> for GID {
    fn from(id: &str) -> Self {
        Self::Str(id.to_string())
    }
}

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

#[derive(Debug)]
pub(crate) struct MutTemporalPropColumn {
    temporal_props: MutableListArray<i64, MutableStructArray>,
}

impl MutTemporalPropColumn {
    pub(crate) fn new(temporal_props: MutableListArray<i64, MutableStructArray>) -> Self {
        Self { temporal_props }
    }

    fn into_inner(self) -> MutableListArray<i64, MutableStructArray> {
        self.temporal_props
    }

    fn try_push_valid(&mut self) -> Result<(), arrow2::error::Error> {
        self.temporal_props.try_push_valid()
    }

    fn add_row(&mut self, row: TPropRow) {
        let row_arr = self.temporal_props.mut_values();
        row_arr.value::<MPArr<i64>>(0).unwrap().push(Some(row.t)); //time

        row.props
            .into_iter()
            .enumerate()
            .for_each(|(i, prop)| match prop {
                Some(Prop::Bool(v)) => {
                    row_arr.mut_values()[i + 1]
                        .as_mut_any()
                        .downcast_mut::<MutableBooleanArray>()
                        .unwrap()
                        .push(Some(v));
                }
                Some(Prop::Str(v)) => {
                    row_arr.mut_values()[i + 1]
                        .as_mut_any()
                        .downcast_mut::<MutableUtf8Array<i64>>()
                        .unwrap()
                        .push(Some(v));
                }
                Some(Prop::I32(v)) => {
                    Self::update_mutable_arr(&mut row_arr.mut_values()[i + 1], v);
                }
                Some(Prop::I64(v)) => {
                    Self::update_mutable_arr(&mut row_arr.mut_values()[i + 1], v);
                }
                Some(Prop::F32(v)) => {
                    Self::update_mutable_arr(&mut row_arr.mut_values()[i + 1], v);
                }
                Some(Prop::F64(v)) => {
                    Self::update_mutable_arr(&mut row_arr.mut_values()[i + 1], v);
                }
                Some(Prop::U8(v)) => {
                    Self::update_mutable_arr(&mut row_arr.mut_values()[i + 1], v);
                }
                Some(Prop::U16(v)) => {
                    Self::update_mutable_arr(&mut row_arr.mut_values()[i + 1], v);
                }
                Some(Prop::U32(v)) => {
                    Self::update_mutable_arr(&mut row_arr.mut_values()[i + 1], v);
                }
                Some(Prop::U64(v)) => {
                    Self::update_mutable_arr(&mut row_arr.mut_values()[i + 1], v);
                }
                None => {
                    row_arr.mut_values()[i + 1].push_null();
                }
                _ => todo!("Add support for other types"),
            });
    }

    fn update_mutable_arr<T: NativeType>(arr: &mut Box<dyn MutableArray + 'static>, t: T) {
        arr.as_mut_any()
            .downcast_mut::<MutablePrimitiveArray<T>>()
            .unwrap()
            .push(Some(t));
    }
}

#[derive(Debug, PartialEq)]
struct TPropRow {
    t: i64,
    props: Vec<Option<Prop>>,
}

impl TPropRow {
    fn new(cap: usize, prop_idx: usize, t: i64, prop: Prop) -> Self {
        let mut props = vec![None; cap];
        props[prop_idx] = Some(prop);
        Self { t, props }
    }

    fn from_vec(t: i64, props: Vec<Option<Prop>>) -> Self {
        Self { t, props }
    }

    fn time(&self) -> &i64 {
        &self.t
    }

    fn merge(self, other: TPropRow) -> Self {
        let mut props = self.props;
        for (i, prop) in other.props.into_iter().enumerate() {
            if let Some(prop) = prop {
                props[i] = Some(prop);
            }
        }
        Self { t: self.t, props }
    }
}

fn load_edge_prop_vec<'a, G: GraphViewOps>(e_ref: EdgeRef, g: &'a G) -> Vec<Vec<TPropRow>> {
    let temp_prop_meta = g.edge_meta().temporal_prop_meta();
    let prop_len = temp_prop_meta.get_keys().len();
    let mut prop_vecs = Vec::with_capacity(prop_len);

    for prop_id in 0..prop_len {
        let name = temp_prop_meta.get_name(prop_id).unwrap();
        let edge_props = g
            .temporal_edge_prop_vec(e_ref, &name, LayerIds::All)
            .into_iter()
            .map(|(t, prop)| TPropRow::new(prop_len, prop_id, t, prop))
            .collect::<Vec<_>>();
        prop_vecs.push(edge_props)
    }

    prop_vecs
}

fn load_vertex_prop_vec<'a, G: GraphViewOps>(vid: VID, g: &'a G) -> Vec<Vec<TPropRow>> {
    let temp_prop_meta = g.vertex_meta().temporal_prop_meta();
    let prop_len = temp_prop_meta.get_keys().len();
    let mut prop_vecs = Vec::with_capacity(prop_len);

    for prop_id in 0..prop_len {
        let name = temp_prop_meta.get_name(prop_id).unwrap();
        let edge_props = g
            .temporal_vertex_prop_vec(vid, &name)
            .into_iter()
            .map(|(t, prop)| TPropRow::new(prop_len, prop_id, t, prop))
            .collect::<Vec<_>>();
        prop_vecs.push(edge_props)
    }

    prop_vecs
}

fn merge_iterators(t_prop_rows: Vec<Vec<TPropRow>>) -> Option<impl Iterator<Item = TPropRow>> {
    t_prop_rows
        .into_iter()
        .map(|v| {
            let iter: Box<dyn Iterator<Item = TPropRow>> = Box::new(v.into_iter());
            iter
        })
        .reduce(|iter1, iter2| {
            Box::new(
                Box::new(iter1.into_iter())
                    .merge_join_by(iter2.into_iter(), |row1, row2| row1.time().cmp(row2.time()))
                    .map(|join_res| match join_res {
                        EitherOrBoth::Both(row1, row2) => row1.merge(row2),
                        EitherOrBoth::Left(row1) => row1,
                        EitherOrBoth::Right(row2) => row2,
                    }),
            )
        })
}

#[cfg(test)]
mod test {

    use super::*;
    use raphtory::{core::entities::LayerIds, db::api::view::internal::GraphOps, prelude::*};

    #[test]
    fn merge_edge_temporal_props_over_time_t1() {
        let graph = Graph::new();
        graph
            .add_edge(
                3,
                2,
                4,
                [
                    ("a", 12.into()),
                    ("b", Prop::str("boom")),
                    ("c", Prop::F32(1f32)),
                ],
                None,
            )
            .expect("Failed to add edge");

        let src = graph.vertex_ref(2, &LayerIds::All, None).unwrap();
        let dst = graph.vertex_ref(4, &LayerIds::All, None).unwrap();

        let e_ref = graph.edge_ref(src, dst, &LayerIds::All, None).unwrap();

        let actual = merge_iterators(load_edge_prop_vec(e_ref, &graph))
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(
            actual,
            vec![TPropRow::from_vec(
                3,
                vec![
                    Some(Prop::I32(12)),
                    Some(Prop::str("boom")),
                    Some(Prop::F32(1f32))
                ]
            ),]
        )
    }

    #[test]
    fn merge_edge_temporal_props_over_t1_t4() {
        let graph = Graph::new();

        graph
            .add_edge(4, 2, 4, [("a", 1)], None)
            .expect("Failed to add edge");
        graph
            .add_edge(1, 2, 4, NO_PROPS, None)
            .expect("Failed to add edge");
        graph
            .add_edge(4, 2, 4, [("b", "bam")], None)
            .expect("Failed to add edge");
        graph
            .add_edge(
                3,
                2,
                4,
                [("b", Prop::str("boom")), ("c", Prop::F32(1f32))],
                None,
            )
            .expect("Failed to add edge");
        graph
            .add_edge(9, 2, 4, [("c", Prop::F32(12f32)), ("a", 12.into())], None)
            .expect("Failed to add edge");

        let src = graph.vertex_ref(2, &LayerIds::All, None).unwrap();
        let dst = graph.vertex_ref(4, &LayerIds::All, None).unwrap();

        let e_ref = graph.edge_ref(src, dst, &LayerIds::All, None).unwrap();

        let actual = merge_iterators(load_edge_prop_vec(e_ref, &graph))
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(
            actual,
            vec![
                TPropRow::from_vec(
                    3,
                    vec![None, Some(Prop::str("boom")), Some(Prop::F32(1f32))]
                ),
                TPropRow::from_vec(4, vec![Some(Prop::I32(1)), Some(Prop::str("bam")), None]),
                TPropRow::from_vec(9, vec![Some(Prop::I32(12)), None, Some(Prop::F32(12f32))]),
            ]
        )
    }
}
