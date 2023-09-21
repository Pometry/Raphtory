use std::{ops::Range, fmt::Debug};

use itertools::Itertools;
use polars_core::{prelude::*, utils::arrow::array::*};
use raphtory::{
    core::{
        entities::{vertices::vertex_ref::VertexRef, LayerIds, EID, VID},
        Direction,
    },
    db::api::view::internal::ExplodedEdgeOps,
    prelude::{GraphViewOps, VertexViewOps},
};

use super::{MPArr, MutEdgePair};

#[derive(Debug)]
pub struct TemporalColGraphFragment {
    vertex_df: DataFrame,
    edge_df: DataFrame,
}

const OUTBOUND_COLUMN: &str = "outbound";
const INBOUND_COLUMN: &str = "inbound";

const V_ADDITIONS_COLUMN: &str = "additions";
const E_ADDITIONS_COLUMN: &str = "additions";
const E_DELETIONS_COLUMN: &str = "deletions";

const GID_COLUMN: &str = "global_vertex_id";
const SRC_COLUMN: &str = "src";
const DST_COLUMN: &str = "dst";

const V_COLUMN: &str = "v";
const E_COLUMN: &str = "e";

impl TemporalColGraphFragment {
    pub fn from_graph<G: GraphViewOps>(g: &G) -> Self {
        // create a lookup table for the vertices sorted by gid
        let mut new_to_old = (0..g.unfiltered_num_vertices()).collect::<Vec<_>>();
        new_to_old.sort_unstable_by_key(|&vid| g.vertex_id(vid.into()));

        let mut old_to_new = vec![0; g.unfiltered_num_vertices()];
        for (new_id, old_id) in new_to_old.iter().enumerate() {
            old_to_new[*old_id] = new_id;
        }

        // create lookup table for the edges sorted by (src_gid, dst_gid)
        let mut edges_new_to_old = (0..g.edges_len(LayerIds::All, None)).collect::<Vec<_>>();
        edges_new_to_old.sort_unstable_by_key(|&eid| {
            let edge = g.find_edge_id(eid.into(), &LayerIds::All, None).unwrap();
            let src_gid = g.vertex_id(edge.src());
            let dst_gid = g.vertex_id(edge.dst());
            (src_gid, dst_gid)
        });

        let mut edges_old_to_new = vec![0; edges_new_to_old.len()];
        for (new_id, old_id) in edges_new_to_old.iter().enumerate() {
            edges_old_to_new[*old_id] = new_id;
        }

        let mut edge_src_column = Vec::with_capacity(edges_new_to_old.len());
        let mut edge_dst_column = Vec::with_capacity(edges_new_to_old.len());

        let mut edge_timestamps = Self::mutable_timestamps_column(E_ADDITIONS_COLUMN);
        let mut edge_deletions = Self::mutable_timestamps_column(E_DELETIONS_COLUMN);

        for &eid in &edges_new_to_old {
            let edge = g.find_edge_id(eid.into(), &LayerIds::All, None).unwrap();
            let src_id: usize = edge.src().into();
            let dst_id: usize = edge.dst().into();

            let new_src_id = &old_to_new[src_id];
            let new_dst_id = &old_to_new[dst_id];

            edge_src_column.push(*new_src_id as u64);
            edge_dst_column.push(*new_dst_id as u64);

            let edge_history = g.edge_history(edge).collect::<Vec<_>>();
            let mut_arr = edge_timestamps.mut_values();
            mut_arr.extend_trusted_len_values(edge_history.into_iter());
            edge_timestamps.try_push_valid().expect("push valid");

            let deletion_history = g.edge_deletion_history(edge, LayerIds::All);
            let mut_arr = edge_deletions.mut_values();
            mut_arr.extend_trusted_len_values(deletion_history.into_iter());
            edge_deletions.try_push_valid().expect("push valid");
        }

        let mut outbound_arr =
            Self::mutable_adj_list_column(OUTBOUND_COLUMN, edges_new_to_old.len());

        let mut inbound_arr = Self::mutable_adj_list_column(INBOUND_COLUMN, edges_new_to_old.len());

        let mut timestamps = Self::mutable_timestamps_column(V_ADDITIONS_COLUMN);

        // loop over each vertex adjacency list
        for vertex in &new_to_old {
            let vid: VID = (*vertex).into();
            for e_ref in g.vertex_edges(vid, Direction::OUT, LayerIds::All, None) {
                let mut row: MutEdgePair = outbound_arr.mut_values().into();
                let vid: usize = e_ref.dst().into();
                let eid: usize = e_ref.pid().into();
                let new_vid = &old_to_new[vid];
                let new_eid = &edges_old_to_new[eid];
                row.add_pair_usize(*new_vid, *new_eid)
            }
            outbound_arr.try_push_valid().expect("push valid"); // one row done

            for e_ref in g.vertex_edges(vid, Direction::IN, LayerIds::All, None) {
                let mut row: MutEdgePair = inbound_arr.mut_values().into();
                let vid: usize = e_ref.src().into();
                let eid: usize = e_ref.pid().into();
                let new_vid = &old_to_new[vid];
                let new_eid = &edges_old_to_new[eid];
                row.add_pair_usize(*new_vid, *new_eid)
            }
            inbound_arr.try_push_valid().expect("push valid"); // one row done

            // add vertex timestamps column
            let vertex_timestamps = g.vertex(VertexRef::Internal(vid)).unwrap().history();
            let mut_arr = timestamps.mut_values();
            mut_arr.extend_trusted_len_values(vertex_timestamps.into_iter());
            timestamps.try_push_valid().expect("push valid"); // one row done
        }

        // gid column
        let gids_sorted_by_gids = new_to_old
            .iter()
            .map(|&vid| g.vertex_id(vid.into()))
            .collect::<Vec<_>>();

        let outbound_arr: ListArray<i64> = outbound_arr.into();
        let outbound: ChunkedArray<ListType> =
            ChunkedArray::with_chunk(OUTBOUND_COLUMN, outbound_arr);

        let inbound_arr: ListArray<i64> = inbound_arr.into();
        let inbound: ChunkedArray<ListType> = ChunkedArray::with_chunk(INBOUND_COLUMN, inbound_arr);

        let timestamps_arr: ListArray<i64> = timestamps.into();
        let timestamps: ChunkedArray<ListType> =
            ChunkedArray::with_chunk(V_ADDITIONS_COLUMN, timestamps_arr);

        let items = Series::new(GID_COLUMN, gids_sorted_by_gids);
        let vertex_df = DataFrame::new(vec![
            items,
            outbound.into_series(),
            inbound.into_series(),
            timestamps.into_series(),
        ])
        .expect(
            "unexpected error, should be able to create vertex dataframe, contact maintainers!",
        );

        let edge_timestamps_arr: ListArray<i64> = edge_timestamps.into();
        let edge_timestamps: ChunkedArray<ListType> =
            ChunkedArray::with_chunk(E_ADDITIONS_COLUMN, edge_timestamps_arr);

        let edge_deletions_arr: ListArray<i64> = edge_deletions.into();
        let edge_deletions: ChunkedArray<ListType> =
            ChunkedArray::with_chunk(E_DELETIONS_COLUMN, edge_deletions_arr);

        // edge graph
        let edge_df = DataFrame::new(vec![
            Series::new(SRC_COLUMN, edge_src_column),
            Series::new(DST_COLUMN, edge_dst_column),
            edge_timestamps.into_series(),
            edge_deletions.into_series(),
        ])
        .expect("unexpected error, should be able to create edge dataframe, contact maintainers!");

        Self { vertex_df, edge_df }
    }

    fn mutable_adj_list_column(
        name: &str,
        cap: usize,
    ) -> MutableListArray<i64, MutableStructArray> {
        let fields = vec![
            ArrowField::new(V_COLUMN, ArrowDataType::UInt64, false),
            ArrowField::new(E_COLUMN, ArrowDataType::UInt64, false),
        ];

        // arrays for outbound
        let out_v = Box::new(MPArr::<u64>::with_capacity(cap));
        let out_e = Box::new(MPArr::<u64>::with_capacity(cap));
        let out_inner =
            MutableStructArray::new(ArrowDataType::Struct(fields.clone()), vec![out_v, out_e]);

        let mut_arr =
            MutableListArray::<i64, MutableStructArray>::new_with_field(out_inner, name, true);

        mut_arr
    }

    fn mutable_timestamps_column(name: &str) -> MutableListArray<i64, MutablePrimitiveArray<i64>> {
        let mut_arr = MutablePrimitiveArray::new();
        let mut_arr = MutableListArray::<i64, MutablePrimitiveArray<i64>>::new_with_field(
            mut_arr, name, false,
        );
        mut_arr
    }

    fn items(&self) -> &ChunkedArray<UInt64Type> {
        self.vertex_df
            .column(GID_COLUMN)
            .unwrap()
            .u64()
            .expect("unexpected error, should be able to downcast, contact maintainers!")
    }

    fn outbound(&self) -> &ChunkedArray<ListType> {
        self.vertex_df
            .column(OUTBOUND_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<ChunkedArray<ListType>>()
            .expect("unexpected error, should be able to downcast, contact maintainers!")
    }

    fn inbound(&self) -> &ChunkedArray<ListType> {
        self.vertex_df
            .column(INBOUND_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<ChunkedArray<ListType>>()
            .expect("unexpected error, should be able to downcast, contact maintainers!")
    }

    fn edge_additions(&self) -> &ChunkedArray<ListType> {
        self.edge_df
            .column(E_ADDITIONS_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<ChunkedArray<ListType>>()
            .expect("unexpected error, should be able to downcast, contact maintainers!")
    }

    fn adj_list(&self, vertex_id: usize, dir: Direction) -> Option<StructArray> {
        let row: usize = vertex_id.into();

        let chunks = match dir {
            Direction::OUT => self.outbound().chunks(),
            Direction::IN => self.inbound().chunks(),
            Direction::BOTH => return None,
        };

        let chunk_size = chunks[0].len(); // we assume all the chunks are the same size

        let chunk_idx = row / chunk_size;
        let idx = row % chunk_size;

        let arr = chunks
            .get(chunk_idx)?
            .as_any()
            .downcast_ref::<ListArray<i64>>()?;
        let arr = arr.value(idx);
        let adj_list = arr.as_any().downcast_ref::<StructArray>()?;
        Some(adj_list.clone())
    }

    pub(crate) fn edges(
        &self,
        vertex_id: VID,
        dir: Direction,
        window: Option<Range<i64>>,
    ) -> Option<Box<dyn Iterator<Item = (VID, EID)>>> {
        match dir {
            Direction::IN | Direction::OUT => {
                let adj_array = self.adj_list(vertex_id.into(), dir)?;
                let v = adj_array.values()[0]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()?
                    .clone();
                let e = adj_array.values()[1]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()?
                    .clone();
                let iter = v
                    .into_iter()
                    .filter_map(|x| x)
                    .zip(e.into_iter().filter_map(|x| x))
                    .map(|(vid, eid)| (VID(vid as usize), EID(eid as usize)));

                let edge_additions_column = self.edge_additions().clone();

                let res: Box<dyn Iterator<Item = (VID, EID)>> = if let Some(range) = window {
                    Box::new(iter.filter(move |(_, eid)| {
                        Self::edge_in_interval(&edge_additions_column, *eid, &range.clone())
                    }))
                } else {
                    Box::new(iter)
                };

                Some(res)
            }
            Direction::BOTH => {
                let out = self.edges(vertex_id, Direction::OUT, window.clone())?;
                let inb = self.edges(vertex_id, Direction::IN, window.clone())?;
                Some(Box::new(out.merge_by(inb, |(v1, _), (v2, _)| v1 < v2)))
            }
        }
    }

    fn overlap<T: PartialOrd + Debug>(a: &Range<T>, sorted_slice: &[T]) -> bool {
        if sorted_slice.is_empty() {
            return false;
        }
        let first = &sorted_slice[0];
        let last = &sorted_slice[sorted_slice.len() - 1];

        if a.start > *last || a.end <= *first {
            return false;
        }
        true
    }

    fn edge_in_interval(arr: &ChunkedArray<ListType>, eid: EID, window: &Range<i64>) -> bool {
        let edge_timestamps = Self::get_edge_additions(arr, eid)
            .expect("unexpected error, failed to load timestamps for edge");
        Self::overlap(window, edge_timestamps.values())
    }

    fn get_edge_additions(arr: &ChunkedArray<ListType>, eid: EID) -> Option<PrimitiveArray<i64>> {
        let chunks = arr.chunks();
        let chunk_size = chunks[0].len(); // we assume all the chunks are the same size
        let e_id: usize = eid.into();
        let chunk = chunks.get(e_id / chunk_size)?;
        let chunk_arr = chunk.as_any().downcast_ref::<ListArray<i64>>()?;
        let edge_timestamps_arr = chunk_arr.value(e_id % chunk_size);
        let edge_timestamps = edge_timestamps_arr
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()?;
        Some(edge_timestamps.clone())
    }

    pub(crate) fn neighbours(
        &self,
        vertex_id: VID,
        dir: Direction,
        window: Option<&Range<i64>>,
    ) -> Option<Box<dyn Iterator<Item = VID>>> {
        // global_vid
        let iter = self
            .edges(vertex_id, dir, window.cloned())?
            .map(|(local_id, _)| local_id);
        Some(Box::new(iter))
    }

    pub(crate) fn global_id(&self, vid: VID) -> Option<u64> {
        let items = self.items();
        let chunks = items.chunks();
        let chunk_size = chunks[0].len(); // we assume all the chunks are the same size
        let vid_usize: usize = vid.into();

        let gid = chunks.get(vid_usize / chunk_size)?;
        let chunk_arr = gid.as_any().downcast_ref::<PrimitiveArray<u64>>()?;
        chunk_arr.get(vid_usize % chunk_size)
    }

    pub(crate) fn resolve_vertex_id(&self, gid: u64) -> Option<VID> {
        let items = self.items();
        let chunks = items.chunks();
        let chunk_size = chunks[0].len(); // we assume all the chunks are the same size

        let (chunk_id, pos) = chunks
            .iter()
            .enumerate()
            .map(|(chunk_id, arr)| {
                let arr = arr.as_any().downcast_ref::<PrimitiveArray<u64>>().unwrap();
                arr.values()
                    .binary_search_by(|x| x.cmp(&gid))
                    .ok()
                    .map(|j| (chunk_id, j))
            })
            .find(|needle| needle.is_some())
            .flatten()?;

        Some((chunk_size * chunk_id + pos).into())
    }
}

#[cfg(test)]
mod test {
    use raphtory::{
        core::{
            entities::{EID, VID},
            Direction,
        },
        db::graph::views::deletion_graph::GraphWithDeletions,
        prelude::*,
    };

    use super::TemporalColGraphFragment;

    #[test]
    fn load_one_edge_graph() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).expect("add edge");

        let g = TemporalColGraphFragment::from_graph(&graph);

        let actual = g
            .edges(0.into(), Direction::OUT, None)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(VID(1), EID(0))]);

        let actual = g
            .edges(1.into(), Direction::IN, None)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(VID(0), EID(0))])
    }

    #[test]
    fn load_triangle_graph() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).expect("add edge");
        graph.add_edge(1, 2, 3, NO_PROPS, None).expect("add edge");
        graph.add_edge(2, 3, 1, NO_PROPS, None).expect("add edge");

        let g = TemporalColGraphFragment::from_graph(&graph);

        // resolve
        let one = g.resolve_vertex_id(1).unwrap();
        assert_eq!(one, 0.into());

        let two = g.resolve_vertex_id(2).unwrap();
        assert_eq!(two, 1.into());

        let three = g.resolve_vertex_id(3).unwrap();
        assert_eq!(three, 2.into());

        // edges
        let actual = g
            .edges(0.into(), Direction::OUT, None)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(VID(1), EID(0))]);

        let actual = g
            .edges(1.into(), Direction::OUT, None)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(VID(2), EID(1))]);

        let actual = g
            .edges(2.into(), Direction::OUT, None)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(VID(0), EID(2))]);
    }

    #[test]
    fn load_star_graph() {
        let graph = GraphWithDeletions::new();
        graph.add_edge(1, 1, 2, NO_PROPS, None).expect("add edge");
        graph.add_edge(2, 1, 3, NO_PROPS, None).expect("add edge");
        graph.add_edge(0, 1, 4, NO_PROPS, None).expect("add edge");

        graph.delete_edge(3, 1, 2, None).expect("delete edge");
        graph.delete_edge(7, 1, 2, None).expect("delete edge");

        let g = TemporalColGraphFragment::from_graph(&graph);
        println!("{:?}", g);

        // edges
        let actual = g
            .edges(0.into(), Direction::OUT, None)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(
            actual,
            vec![(VID(1), EID(0)), (VID(2), EID(1)), (VID(3), EID(2))]
        );
    }

    #[test]
    fn vertex_with_2_edges_different_times() {
        let graph = Graph::new();
        graph.add_edge(2, 3, 1, NO_PROPS, None).expect("add edge");
        graph.add_edge(1, 3, 2, NO_PROPS, None).expect("add edge");

        let g = TemporalColGraphFragment::from_graph(&graph);
        let one = g.resolve_vertex_id(1).unwrap();
        let two = g.resolve_vertex_id(2).unwrap();
        let three = g.resolve_vertex_id(3).unwrap();

        let actual = g
            .edges(three, Direction::OUT, None)
            .expect("should have edges for 3")
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(one, EID(0)), (two, EID(1))]);

        // 3's edges at time [0..2)
        let actual = g
            .edges(three, Direction::OUT, Some(0..2))
            .expect("should have edges for 3")
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(two, EID(1))]);

        // 3's edges at time [2..4)
        let actual = g
            .edges(three, Direction::OUT, Some(2..4))
            .expect("should have edges for 3")
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(one, EID(0))]);
    }

    #[test]
    fn load_2_edges() {
        let graph = Graph::new();

        graph
            .add_edge(4, 2, 4, NO_PROPS, None)
            .expect("Failed to add edge");
        graph
            .add_edge(0, 1, 2, NO_PROPS, None)
            .expect("Failed to add edge");

        let g = TemporalColGraphFragment::from_graph(&graph);

        // resolve
        let one = g.resolve_vertex_id(1).unwrap();
        assert_eq!(one, 0.into());

        let two = g.resolve_vertex_id(2).unwrap();
        assert_eq!(two, 1.into());

        let three = g.resolve_vertex_id(4).unwrap();
        assert_eq!(three, 2.into());

        // edges
        let actual = g
            .edges(two, Direction::OUT, None)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(VID(2), EID(1))]);
    }

    #[test]
    fn overlap(){

        let slice = [2];
        let range = 0..2;

        assert!(!TemporalColGraphFragment::overlap(&range, &slice));

        let slice = [3, 4, 5, 6, 7];

        let overlap = 4..6;
        let overlap_left = 1..5;
        let overlap_right = 5..8;
        let overlap_right_edge = 7..9;

        for range in &[overlap, overlap_left, overlap_right, overlap_right_edge] {
            assert!(TemporalColGraphFragment::overlap(&range, &slice), "{:?} does not overlap {:?}", range, &slice);
        }

        let no_overlap = 0..2;
        let no_overlap_included = 0..3;
        let no_overlap_right = 8..10;

        for range in &[no_overlap, no_overlap_included, no_overlap_right] {
            assert!(!TemporalColGraphFragment::overlap(&range, &slice), "{:?} does overlap {:?}", range, &slice);
        }
    }
}
