use std::{path::Path, sync::Arc};

use crate::arrow::{
    edge_frame_builder::EdgeFrameBuilder, vertex_frame_builder::VertexFrameBuilder, Error,
    E_ADDITIONS_COLUMN, GID_COLUMN, INBOUND_COLUMN, OUTBOUND_COLUMN,
};
use arrow2::error::Result as ArrowResult;
use itertools::Itertools;
use polars_core::{
    error::ArrowError,
    prelude::*,
    utils::arrow::{
        array::{
            Array, ListArray, MutableArray, MutableListArray, MutablePrimitiveArray,
            PrimitiveArray, StructArray,
        },
        offset::Offsets,
    },
};
use raphtory::core::{
    entities::{EID, VID},
    Direction,
};

#[derive(Debug)]
struct TempColGraphFragment {
    edge_df: DataFrame,
    vertex_df: DataFrame,
}

impl TempColGraphFragment {
    fn edges(&self, vertex_id: VID, dir: Direction) -> Box<dyn Iterator<Item = (EID, VID)> + Send> {
        match dir {
            Direction::IN | Direction::OUT => {
                let adj_array = self.adj_list(vertex_id.into(), dir);
                if adj_array.is_none() {
                    return Box::new(std::iter::empty());
                }
                let adj_array = adj_array.unwrap();
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
                let iter = v
                    .into_iter()
                    .flatten()
                    .zip(e.into_iter().flatten())
                    .map(|(vid, eid)| (EID(eid as usize), VID(vid as usize)));

                Box::new(iter)
            }
            Direction::BOTH => {
                let out = self.edges(vertex_id, Direction::OUT);
                let inb = self.edges(vertex_id, Direction::IN);
                Box::new(out.merge_by(inb, |(v1, _), (v2, _)| v1 < v2))
            }
        }
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
}

struct TempColGraph {
    fragments: Arc<[TempColGraphFragment]>,
}

pub type Time = i64;

#[derive(Debug)]
pub struct SparseTable {
    chunk_size: usize,
    sorted_gids: Vec<u64>,
    adj_out_chunks: Vec<Box<dyn Array>>,
    edge_chunks: Vec<Box<dyn Array>>,
}

impl SparseTable {
    fn into_graph(self) -> TempColGraphFragment {
        let outbound: ChunkedArray<ListType> =
            unsafe { ChunkedArray::from_chunks(OUTBOUND_COLUMN, self.adj_out_chunks) };

        let vertex_gid_col = Series::new(GID_COLUMN, self.sorted_gids);
        let vertex_df = DataFrame::new(vec![vertex_gid_col, outbound.into_series()]).unwrap();

        let values = MutablePrimitiveArray::from_vec(
            self.time_col.i64().unwrap().into_iter().flatten().collect(),
        );

        let edge_timestamps = MutableListArray::new_from_mutable(
            values,
            Offsets::try_from(self.edge_time_offsets).unwrap(),
            None,
        );

        let edge_timestamps: ListArray<i64> = edge_timestamps.into();

        let edges_timestamps: ChunkedArray<ListType> = unsafe {
            ChunkedArray::from_chunks(E_ADDITIONS_COLUMN, vec![Box::new(edge_timestamps)])
        };

        let edge_df = DataFrame::new(vec![edges_timestamps.into_series()]).unwrap();

        TempColGraphFragment { vertex_df, edge_df }
    }
}

impl TempColGraph {
    pub fn from_sorted_edge_list<P: AsRef<Path>>(
        src_dst_frame: DataFrame, // sorted_by (src, dst, time)
        src_col: &str,
        dst_col: &str,
        time_col: &str,
        graph_dir: P,
    ) -> Result<Self, Error> {
        let src = src_dst_frame.column(src_col)?.u64()?;
        let dst = src_dst_frame.column(dst_col)?.u64()?;
        let time = src_dst_frame.column(time_col)?.i64()?;

        let sprs_table = Self::build_tables(graph_dir, src, dst, time, 100)?;

        Ok(Self {
            fragments: Arc::new([sprs_table.into_graph()]),
        })
    }

    pub fn build_tables<P: AsRef<Path>>(
        base_dir: P,
        srcs: &ChunkedArray<UInt64Type>,
        dsts: &ChunkedArray<UInt64Type>,
        times: &ChunkedArray<Int64Type>,
        chunk_size: usize,
    ) -> ArrowResult<SparseTable> {
        let mut vf_builder = VertexFrameBuilder::new(chunk_size, &base_dir);
        let mut edge_builder = EdgeFrameBuilder::new(chunk_size, &base_dir);

        // initialise vertex global id table to preserve order
        for chunk in srcs.chunks() {
            let arr = chunk
                .as_any()
                .downcast_ref::<PrimitiveArray<u64>>()
                .unwrap();
            for v in arr.values().iter() {
                vf_builder.push_source(*v)
            }
        }
        // g_id, [{v_id1, e_id1}, {v_id2, e_id2}, ...]
        for (event_id, ((src, dst), time)) in srcs
            .into_iter()
            .flatten()
            .zip(dsts.into_iter().flatten())
            .zip(times.into_iter().flatten())
            .enumerate()
        {
            let (src_id, dst_id) = vf_builder.push_update(src, dst)?;
            edge_builder.push_update(time, src_id, dst_id)?;
        }
        vf_builder.finalise_empty_chunks()?;
        edge_builder.finalise()?;
        Ok(SparseTable {
            sorted_gids: vf_builder.sorted_gids,
            adj_out_chunks: vf_builder.adj_out_chunks,
            edge_chunks: edge_builder.edge_chunks,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn load_one_edge_from_sorted_adj_list_num_vertices_no_props() {
        let df = DataFrame::new(vec![
            Series::new("src", vec![1u64]),
            Series::new("dst", vec![2u64]),
            Series::new("time", vec![0i64]),
        ])
        .unwrap();
        let test_dir = TempDir::new().unwrap();

        let src = df.column("src").unwrap().u64().unwrap();
        let dst = df.column("dst").unwrap().u64().unwrap();
        let time = df.column("time").unwrap().i64().unwrap();

        let res = TempColGraph::build_tables(test_dir.path(), src, dst, time, 100).unwrap();
        let graph = res.into_graph();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();

        let expected = vec![(EID(0), VID(1))];

        assert_eq!(actual, expected)
    }

    #[test]
    fn load_one_edge_from_sorted_adj_list_num_vertices_no_props_multiple_timestamps() {
        let df = DataFrame::new(vec![
            Series::new("src", vec![1u64, 1u64, 1u64]),
            Series::new("dst", vec![2u64, 2u64, 2u64]),
            Series::new("time", vec![0i64, 3i64, 7i64]),
        ])
        .unwrap();

        let test_dir = TempDir::new().unwrap();

        let src = df.column("src").unwrap().u64().unwrap();
        let dst = df.column("dst").unwrap().u64().unwrap();
        let time = df.column("time").unwrap().i64().unwrap();

        let res = TempColGraph::build_tables(test_dir.path(), src, dst, time, 100).unwrap();
        let graph = res.into_graph();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();

        let expected = vec![(EID(0), VID(1))];

        assert_eq!(actual, expected)
    }

    #[test]
    fn load_muliple_sorted_edges_no_props() {
        let df = DataFrame::new(vec![
            Series::new(
                "src",
                vec![
                    1u64, 1u64, 1u64, 2u64, 2u64, 2u64, 3u64, 3u64, 3u64, 4u64, 4u64, 4u64,
                ],
            ),
            Series::new(
                "dst",
                vec![
                    2u64, 3u64, 4u64, 3u64, 4u64, 5u64, 4u64, 5u64, 6u64, 5u64, 6u64, 7u64,
                ],
            ),
            Series::new(
                "time",
                vec![
                    0i64, 1i64, 2i64, 3i64, 4i64, 5i64, 6i64, 7i64, 8i64, 9i64, 10i64, 11i64,
                ],
            ),
        ])
        .unwrap();

        let test_dir = TempDir::new().unwrap();

        let src = df.column("src").unwrap().u64().unwrap();
        let dst = df.column("dst").unwrap().u64().unwrap();
        let time = df.column("time").unwrap().i64().unwrap();

        let res = TempColGraph::build_tables(test_dir.path(), src, dst, time, 100).unwrap();

        let graph = res.into_graph();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();

        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2)), (EID(2), VID(3))];

        assert_eq!(actual, expected)
    }

    #[test]
    fn load_multiple_edges_across_chunks() {
        let df = DataFrame::new(vec![
            Series::new(
                "src",
                vec![
                    1u64, 1u64, 1u64, 2u64, 2u64, 2u64, 3u64, 3u64, 3u64, 4u64, 4u64, 4u64,
                ],
            ),
            Series::new(
                "dst",
                vec![
                    2u64, 3u64, 4u64, 3u64, 4u64, 5u64, 4u64, 5u64, 6u64, 5u64, 6u64, 7u64,
                ],
            ),
            Series::new(
                "time",
                vec![
                    0i64, 1i64, 2i64, 3i64, 4i64, 5i64, 6i64, 7i64, 8i64, 9i64, 10i64, 11i64,
                ],
            ),
        ])
        .unwrap();

        let test_dir = TempDir::new().unwrap();

        let src = df.column("src").unwrap().u64().unwrap();
        let dst = df.column("dst").unwrap().u64().unwrap();
        let time = df.column("time").unwrap().i64().unwrap();

        let res = TempColGraph::build_tables(test_dir.path(), src, dst, time, 2).unwrap();
        let graph = res.into_graph();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();

        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2)), (EID(2), VID(3))];

        assert_eq!(actual, expected)
    }
}
