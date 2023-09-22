use itertools::{EitherOrBoth, Itertools};
use polars_core::utils::arrow::array::{
    MutableListArray, MutablePrimitiveArray, MutableStructArray,
};
use raphtory::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds},
        ArcStr,
    },
    prelude::{GraphViewOps, Prop},
};

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

    fn from_tprop_iterators<B: Iterator<Item = TPropRow>, I: IntoIterator<Item = B>>(
        is: I,
    ) -> Self {
        todo!()
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

fn merge_iterators(t_prop_rows: Vec<Vec<TPropRow>>) -> impl Iterator<Item = TPropRow> {
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
        .unwrap()
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
        // graph
        //     .add_edge(9, 2, 4, [("c", Prop::F32(12f32)), ("a", 1.into())], None)
        //     .expect("Failed to add edge");

        let src = graph.vertex_ref(2, &LayerIds::All, None).unwrap();
        let dst = graph.vertex_ref(4, &LayerIds::All, None).unwrap();

        let e_ref = graph.edge_ref(src, dst, &LayerIds::All, None).unwrap();

        let actual = merge_iterators(load_edge_prop_vec(e_ref, &graph)).collect::<Vec<_>>();

        assert_eq!(
            actual,
            vec![
                TPropRow::from_vec(
                    3,
                    vec![
                        Some(Prop::I32(12)),
                        Some(Prop::str("boom")),
                        Some(Prop::F32(1f32))
                    ]
                ),
                // TPropRow::from_vec(4, vec![Some(Prop::I32(1)), None, None]),
                // TPropRow::from_vec(7, vec![None, Some(Prop::str("bam")), None]),
                // TPropRow::from_vec(9, vec![Some(Prop::I32(1)), None, Some(Prop::F32(12f32))]),
            ]
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

        let actual = merge_iterators(load_edge_prop_vec(e_ref, &graph)).collect::<Vec<_>>();

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
