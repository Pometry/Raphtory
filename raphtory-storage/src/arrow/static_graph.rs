use itertools::Itertools;
use polars_core::{prelude::*, utils::arrow::array::*};
use raphtory::{
    core::{
        entities::{vertices::vertex_ref::VertexRef, LayerIds, VID},
        Direction,
    },
    prelude::{GraphViewOps, VertexViewOps},
};

use super::{MPArr, MutEdgePair};

#[derive(Debug)]
pub struct StaticGraph {
    vertex_df: DataFrame,
    edge_df: DataFrame,
}

const OUTBOUND_COLUMN: &str = "outbound";
const INBOUND_COLUMN: &str = "inbound";

const V_ADDITIONS_COLUMN: &str = "v_additions";

const GID_COLUMN: &str = "global_vertex_id";
const SRC_COLUMN: &str = "src";
const DST_COLUMN: &str = "dst";

const V_COLUMN: &str = "v";
const E_COLUMN: &str = "e";

impl StaticGraph {
    pub fn from_graph<G: GraphViewOps>(g: &G) -> Self {
        // create a lookup table for the vertices sorted by gid
        let mut vids_sorted_by_gid = (0..g.vertices_len(LayerIds::All, None)).collect::<Vec<_>>();
        vids_sorted_by_gid.sort_unstable_by_key(|&vid| g.vertex_id(vid.into()));

        // create lookup table for the edges sorted by (src_gid, dst_gid)
        let mut eids_sorted_by_src_dst_gid =
            (0..g.edges_len(LayerIds::All, None)).collect::<Vec<_>>();
        eids_sorted_by_src_dst_gid.sort_unstable_by_key(|&eid| {
            let edge = g.find_edge_id(eid.into(), &LayerIds::All, None).unwrap();
            let src_gid = g.vertex_id(edge.src());
            let dst_gid = g.vertex_id(edge.dst());
            (src_gid, dst_gid)
        });

        let mut edge_src_column = Vec::with_capacity(eids_sorted_by_src_dst_gid.len());
        let mut edge_dst_column = Vec::with_capacity(eids_sorted_by_src_dst_gid.len());

        for &eid in &eids_sorted_by_src_dst_gid {
            let edge = g.find_edge_id(eid.into(), &LayerIds::All, None).unwrap();
            let src_gid = g.vertex_id(edge.src());
            let dst_gid = g.vertex_id(edge.dst());
            edge_src_column.push(src_gid);
            edge_dst_column.push(dst_gid);
        }

        let mut outbound_arr =
            Self::mutable_adj_list_column(OUTBOUND_COLUMN, eids_sorted_by_src_dst_gid.len());

        let mut inbound_arr =
            Self::mutable_adj_list_column(INBOUND_COLUMN, eids_sorted_by_src_dst_gid.len());

        let mut timestamps = Self::mutable_timestamps_column(V_ADDITIONS_COLUMN);

        // loop over each vertex adjacency list
        for vertex in &vids_sorted_by_gid {
            let vid: VID = (*vertex).into();
            for e_ref in g.vertex_edges(vid, Direction::OUT, LayerIds::All, None) {
                let mut row: MutEdgePair = outbound_arr.mut_values().into();
                let vid: usize = e_ref.dst().into();
                let eid: usize = e_ref.pid().into();
                let new_vid = &vids_sorted_by_gid[vid];
                let new_eid = &eids_sorted_by_src_dst_gid[eid];
                row.add_pair_usize(*new_vid, *new_eid)
            }
            outbound_arr.try_push_valid().expect("push valid"); // one row done

            for e_ref in g.vertex_edges(vid, Direction::IN, LayerIds::All, None) {
                let mut row: MutEdgePair = inbound_arr.mut_values().into();
                let vid: usize = e_ref.src().into();
                let eid: usize = e_ref.pid().into();
                let new_vid = &vids_sorted_by_gid[vid];
                let new_eid = &eids_sorted_by_src_dst_gid[eid];
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
        let gids_sorted_by_gids = vids_sorted_by_gid
            .iter()
            .map(|&vid| g.vertex_id(vid.into()))
            .collect::<Vec<_>>();

        let outbound_arr: ListArray<i64> = outbound_arr.into();
        let outbound: ChunkedArray<ListType> =
            ChunkedArray::with_chunk(OUTBOUND_COLUMN, outbound_arr);

        let inbound_arr: ListArray<i64> = inbound_arr.into();
        let inbound: ChunkedArray<ListType> = ChunkedArray::with_chunk(INBOUND_COLUMN, inbound_arr);

        let timestamps_arr: ListArray<i64>= timestamps.into();
        let timestamps: ChunkedArray<ListType> = ChunkedArray::with_chunk(V_ADDITIONS_COLUMN, timestamps_arr);

        let items = Series::new(GID_COLUMN, gids_sorted_by_gids);
        let vertex_df = DataFrame::new(vec![items, outbound.into_series(), inbound.into_series(), timestamps.into_series()])
            .expect(
                "unexpected error, should be able to create vertex dataframe, contact maintainers!",
            );

        // edge graph
        let edge_df = DataFrame::new(vec![
            Series::new(SRC_COLUMN, edge_src_column),
            Series::new(DST_COLUMN, edge_dst_column),
        ])
        .expect("unexpected error, should be able to create edge dataframe, contact maintainers!");

        Self {
            vertex_df,
            edge_df,
        }
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

    fn mutable_timestamps_column(
        name: &str,
    ) -> MutableListArray<i64, MutablePrimitiveArray<i64>> {
        let mut_arr = MutablePrimitiveArray::new();
        let mut_arr =
            MutableListArray::<i64, MutablePrimitiveArray<i64>>::new_with_field(mut_arr, name, false);
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
    ) -> Option<Box<dyn Iterator<Item = (u64, u64)>>> {
        match dir {
            Direction::IN | Direction::OUT => {
                let adj_array = self.adj_list(vertex_id.into(), dir)?;
                let v = adj_array.values()[0]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .unwrap()
                    .clone();
                let e = adj_array.values()[1]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .unwrap()
                    .clone();
                let iter: Box<dyn Iterator<Item = (u64, u64)>> = Box::new(
                    v.into_iter()
                        .filter_map(|x| x)
                        .zip(e.into_iter().filter_map(|x| x)),
                );
                Some(iter)
            }
            Direction::BOTH => {
                let out = self.edges(vertex_id, Direction::OUT)?;
                let inb = self.edges(vertex_id, Direction::IN)?;
                Some(Box::new(out.merge_by(inb, |(v1, _), (v2, _)| v1 < v2)))
            }
        }
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
    use raphtory::{core::Direction, prelude::*};

    use super::StaticGraph;

    #[test]
    fn load_one_edge_graph() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).expect("add edge");

        let g = StaticGraph::from_graph(&graph);

        let actual = g
            .edges(0.into(), Direction::OUT)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(1, 0)]);

        let actual = g
            .edges(1.into(), Direction::IN)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(0, 0)])
    }

    #[test]
    fn load_triangle_graph() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).expect("add edge");
        graph.add_edge(1, 2, 3, NO_PROPS, None).expect("add edge");
        graph.add_edge(2, 3, 1, NO_PROPS, None).expect("add edge");

        let g = StaticGraph::from_graph(&graph);

        // resolve
        let one = g.resolve_vertex_id(1).unwrap();
        assert_eq!(one, 0.into());

        let two = g.resolve_vertex_id(2).unwrap();
        assert_eq!(two, 1.into());

        let three = g.resolve_vertex_id(3).unwrap();
        assert_eq!(three, 2.into());

        // edges
        let actual = g
            .edges(0.into(), Direction::OUT)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(1, 0)]);

        let actual = g
            .edges(1.into(), Direction::OUT)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(2, 1)]);

        let actual = g
            .edges(2.into(), Direction::OUT)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(0, 2)]);
    }

    #[test]
    fn load_star_graph() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).expect("add edge");
        graph.add_edge(0, 1, 3, NO_PROPS, None).expect("add edge");
        graph.add_edge(0, 1, 4, NO_PROPS, None).expect("add edge");

        let g = StaticGraph::from_graph(&graph);
        println!("{:?}", g);

        // edges

        let actual = g
            .edges(0.into(), Direction::OUT)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(1, 0), (2, 1), (3, 2)]);
    }
}
