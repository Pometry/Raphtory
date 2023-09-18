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

impl StaticGraph {
    pub(crate) fn from_sorted_edge_refs<
        OUT: Iterator<Item = EdgeRef>,
        IN: Iterator<Item = EdgeRef>,
    >(
        outbound: OUT,
        inbound: IN,
    ) -> Self {
        let fields = vec![
            ArrowField::new("v", ArrowDataType::UInt64, false),
            ArrowField::new("e", ArrowDataType::UInt64, false),
        ];

        let mut items = MPArr::<u64>::new();

        // arrays for outbound
        let out_v = Box::new(MPArr::<u64>::new());
        let out_e = Box::new(MPArr::<u64>::new());
        let out_inner =
            MutableStructArray::new(ArrowDataType::Struct(fields.clone()), vec![out_v, out_e]);

        let mut outbound_arr = MutableListArray::<i64, MutableStructArray>::new_with_field(
            out_inner, "outbound", true,
        );

        // arrays for inbound
        let in_v = Box::new(MPArr::<u64>::new());
        let in_e = Box::new(MPArr::<u64>::new());
        let in_inner = MutableStructArray::new(ArrowDataType::Struct(fields), vec![in_v, in_e]);

        let mut inbound_arr =
            MutableListArray::<i64, MutableStructArray>::new_with_field(in_inner, "inbound", true);

        let in_iter: Box<dyn Iterator<Item = IngestEdgeType>> =
            Box::new(inbound.into_iter().map(|t| IngestEdgeType::Inbound(t)));
        let out_iter = Box::new(outbound.into_iter().map(|t| IngestEdgeType::Outbound(t)));

        let iter = [in_iter, out_iter]
            .into_iter()
            .kmerge_by(|a, b| a.local() < b.local());

        let mut cur = None;
        for edge in iter {
            let vertex = edge.vertex_u64();
            if cur.is_none() {
                // happens once
                cur = Some(vertex as u64);
            }

            if cur != Some(vertex) {
                items.push(cur);

                outbound_arr.try_push_valid().expect("push valid"); // one row done
                inbound_arr.try_push_valid().expect("push valid"); // one row done

                cur = Some(vertex);
            }

            match edge {
                e @ IngestEdgeType::Outbound(_) => {
                    let dst = e.remote_u64();
                    let e = e.pid_u64();
                    let mut row: MutEdgePair = outbound_arr.mut_values().into();
                    row.add_pair(dst, e);
                }
                e @ IngestEdgeType::Inbound(_) => {
                    let dst = e.remote_u64();
                    let e = e.pid_u64();
                    let mut row: MutEdgePair = inbound_arr.mut_values().into();
                    row.add_pair(dst, e);
                }
            }
        }

        if cur.is_some() {
            items.push(cur);
            outbound_arr.try_push_valid().expect("push valid"); // last row;
            inbound_arr.try_push_valid().expect("push valid"); // last row;
        }

        let items: ChunkedArray<UInt64Type> =
            ChunkedArray::with_chunk("global_vertex_id", items.into());

        let outbound_arr: ListArray<i64> = outbound_arr.into();
        let outbound: ChunkedArray<ListType> = ChunkedArray::with_chunk("outbound", outbound_arr);

        let inbound_arr: ListArray<i64> = inbound_arr.into();
        let inbound: ChunkedArray<ListType> = ChunkedArray::with_chunk("inbound", inbound_arr);

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
            .column("outbound")
            .unwrap()
            .as_any()
            .downcast_ref::<ChunkedArray<ListType>>()
            .expect("unexpected error, should be able to downcast, contact maintainers!")
    }

    fn inbound(&self) -> &ChunkedArray<ListType> {
        self.df_graph
            .column("inbound")
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
