use itertools::Itertools;
use polars_core::{prelude::*, utils::arrow::array::*};
use raphtory::core::{
    entities::{edges::edge_ref::EdgeRef, VID},
    Direction,
};

use super::{IngestEdgeType, MPArr, MutEdgePair};

pub(crate) struct StaticGraph {
    df_graph: DataFrame,
}

const OUTBOUND_COLUMN: &str = "outbound";
const INBOUND_COLUMN: &str = "inbound";

const GID_COLUMN: &str = "global_vertex_id";

const V_COLUMN: &str = "v";
const E_COLUMN: &str = "e";

impl StaticGraph {
    pub(crate) fn from_sorted_edge_refs<
        OUT: Iterator<Item = (EdgeRef, u64)>,
        IN: Iterator<Item = (EdgeRef, u64)>,
    >(
        outbound: OUT,
        inbound: IN,
    ) -> Self {
        let fields = vec![
            ArrowField::new(V_COLUMN, ArrowDataType::UInt64, false),
            ArrowField::new(E_COLUMN, ArrowDataType::UInt64, false),
        ];

        let mut items = MPArr::<u64>::new();

        // arrays for outbound
        let out_v = Box::new(MPArr::<u64>::new());
        let out_e = Box::new(MPArr::<u64>::new());
        let out_inner =
            MutableStructArray::new(ArrowDataType::Struct(fields.clone()), vec![out_v, out_e]);

        let mut outbound_arr = MutableListArray::<i64, MutableStructArray>::new_with_field(
            out_inner,
            OUTBOUND_COLUMN,
            true,
        );

        // arrays for inbound
        let in_v = Box::new(MPArr::<u64>::new());
        let in_e = Box::new(MPArr::<u64>::new());
        let in_inner = MutableStructArray::new(ArrowDataType::Struct(fields), vec![in_v, in_e]);

        let mut inbound_arr = MutableListArray::<i64, MutableStructArray>::new_with_field(
            in_inner,
            INBOUND_COLUMN,
            true,
        );

        let in_iter: Box<dyn Iterator<Item = IngestEdgeType>> = Box::new(
            inbound
                .into_iter()
                .map(|(e, g_id)| IngestEdgeType::Inbound(e, g_id)),
        );
        let out_iter = Box::new(
            outbound
                .into_iter()
                .map(|(e, g_id)| IngestEdgeType::Outbound(e, g_id)),
        );

        let iter = [in_iter, out_iter]
            .into_iter()
            .kmerge_by(|a, b| a.local() < b.local());

        let mut index: usize = 0;
        let mut cur = None;
        for edge in iter {
            let vertex = edge.local();

            // iter may not start at 0 so we need to push None's into the items array to fill the gaps
            while index < vertex.into() {
                items.push(None);
                outbound_arr.try_push_valid().expect("push valid"); // advance one row
                inbound_arr.try_push_valid().expect("push valid"); // advance one row
                index += 1;
            }

            let vertex_gid = edge.gid();
            if cur.is_none() {
                // happens once
                cur = Some(vertex_gid);
            }

            if cur != Some(vertex_gid) {
                items.push(cur);

                outbound_arr.try_push_valid().expect("push valid"); // one row done
                inbound_arr.try_push_valid().expect("push valid"); // one row done

                cur = Some(vertex_gid);
            }

            match edge {
                e @ IngestEdgeType::Outbound(_, _) => {
                    let dst = e.remote_u64();
                    let e = e.pid_u64();
                    let mut row: MutEdgePair = outbound_arr.mut_values().into();
                    row.add_pair(dst, e);
                }
                e @ IngestEdgeType::Inbound(_, _) => {
                    let dst = e.remote_u64();
                    let e = e.pid_u64();
                    let mut row: MutEdgePair = inbound_arr.mut_values().into();
                    row.add_pair(dst, e);
                }
            }
            index += 1;
        }

        if cur.is_some() {
            items.push(cur);
            outbound_arr.try_push_valid().expect("push valid"); // last row;
            inbound_arr.try_push_valid().expect("push valid"); // last row;
        }

        let items: ChunkedArray<UInt64Type> = ChunkedArray::with_chunk(GID_COLUMN, items.into());

        let outbound_arr: ListArray<i64> = outbound_arr.into();
        let outbound: ChunkedArray<ListType> =
            ChunkedArray::with_chunk(OUTBOUND_COLUMN, outbound_arr);

        let inbound_arr: ListArray<i64> = inbound_arr.into();
        let inbound: ChunkedArray<ListType> = ChunkedArray::with_chunk(INBOUND_COLUMN, inbound_arr);

        let df_graph = DataFrame::new(vec![
            items.into_series(),
            outbound.into_series(),
            inbound.into_series(),
        ])
        .expect("unexpected error, should be able to create dataframe, contact maintainers!");

        Self { df_graph }
    }

    fn items(&self) -> &ChunkedArray<UInt64Type> {
        self.df_graph
            .column("global_vertex_id")
            .unwrap()
            .as_any()
            .downcast_ref::<ChunkedArray<UInt64Type>>()
            .expect("unexpected error, should be able to downcast, contact maintainers!")
    }

    fn outbound(&self) -> &ChunkedArray<ListType> {
        self.df_graph
            .column(OUTBOUND_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<ChunkedArray<ListType>>()
            .expect("unexpected error, should be able to downcast, contact maintainers!")
    }

    fn inbound(&self) -> &ChunkedArray<ListType> {
        self.df_graph
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
}

#[cfg(test)]
mod test {
    use raphtory::core::{entities::edges::edge_ref::EdgeRef, Direction};

    use super::StaticGraph;

    #[test]
    fn load_outbound_one_vertex() {
        let g = StaticGraph::from_sorted_edge_refs(
            vec![(EdgeRef::new_outgoing(0.into(), 0.into(), 1.into()), 1u64)].into_iter(),
            vec![].into_iter(),
        );

        let actual = g
            .edges(0.into(), Direction::OUT)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(1, 0)])
    }

    #[test]
    fn load_inbound_one_vertex() {
        let g = StaticGraph::from_sorted_edge_refs(
            vec![].into_iter(),
            vec![(EdgeRef::new_incoming(0.into(), 0.into(), 1.into()), 1u64)].into_iter(),
        );

        // 0 doesn't have any incoming edges
        let actual = g
            .edges(0.into(), Direction::IN)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![]);

        // 1 has one incoming edge
        let actual = g
            .edges(1.into(), Direction::IN)
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![(0, 0)])
    }

    #[test]
    fn load_outbound_two_vertices() {
        let g = StaticGraph::from_sorted_edge_refs(
            vec![
                (EdgeRef::new_outgoing(0.into(), 0.into(), 1.into()), 1u64),
                (EdgeRef::new_outgoing(1.into(), 0.into(), 2.into()), 1u64),
            ]
            .into_iter(),
            vec![].into_iter(),
        );

        let actual = g
            .edges(0.into(), Direction::OUT)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(1, 0), (2, 1)])
    }
}
