use std::{path::Path, sync::Arc};

use crate::arrow::{
    edge_frame_builder::EdgeFrameBuilder, vertex_frame_builder::VertexFrameBuilder, Error,
    E_ADDITIONS_COLUMN, GID_COLUMN, INBOUND_COLUMN, OUTBOUND_COLUMN,
};
use arrow2::{chunk::Chunk, error::Result as ArrowResult};
use itertools::Itertools;
use polars_core::{
    error::ArrowError,
    prelude::*,
    utils::arrow::{
        array::{Array, ListArray, PrimitiveArray, StructArray},
        offset::Offsets,
    },
};
use raphtory::core::{
    entities::{EID, VID},
    Direction,
};

pub type Time = i64;

#[derive(Debug)]
pub struct TempColGraphFragment {
    chunk_size: usize,
    sorted_gids: Vec<u64>,
    adj_out_chunks: Vec<Chunk<Box<dyn Array>>>,
    edge_chunks: Vec<Chunk<Box<dyn Array>>>,
}

impl TempColGraphFragment {
    pub fn from_sorted_edge_list<P: AsRef<Path>>(
        src_dst_frame: DataFrame, // sorted_by (src, dst, time)
        src_col: &str,
        dst_col: &str,
        time_col: &str,
        chunk_size: usize,
        graph_dir: P,
    ) -> Result<Self, Error> {
        let src = src_dst_frame.column(src_col)?.u64()?;
        let dst = src_dst_frame.column(dst_col)?.u64()?;
        let time = src_dst_frame.column(time_col)?.i64()?;

        let table = Self::build_tables(graph_dir, src, dst, time, chunk_size)?;

        Ok(table)
    }

    fn build_tables<P: AsRef<Path>>(
        base_dir: P,
        srcs: &ChunkedArray<UInt64Type>,
        dsts: &ChunkedArray<UInt64Type>,
        times: &ChunkedArray<Int64Type>,
        chunk_size: usize,
    ) -> ArrowResult<Self> {
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
        for ((src, dst), time) in srcs
            .into_iter()
            .flatten()
            .zip(dsts.into_iter().flatten())
            .zip(times.into_iter().flatten())
        {
            let (src_id, dst_id) = vf_builder.push_update(src, dst)?;
            edge_builder.push_update(time, src_id, dst_id)?;
        }
        vf_builder.finalise_empty_chunks()?;
        edge_builder.finalise()?;
        Ok(TempColGraphFragment {
            chunk_size,
            sorted_gids: vf_builder.sorted_gids,
            adj_out_chunks: vf_builder.adj_out_chunks,
            edge_chunks: edge_builder.edge_chunks,
        })
    }

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

    fn all_edges(&self) -> impl Iterator<Item = (EID, VID, VID)> + '_ {
        self.edge_chunks
            .iter()
            .flat_map(|chunk| {
                let src = chunk[0]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .unwrap()
                    .clone();
                let dst = chunk[1]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .unwrap()
                    .clone();
                src.into_iter().flatten().zip(dst.into_iter().flatten())
            })
            .enumerate()
            .map(|(eid, (src, dst))| (EID(eid), VID(src as usize), VID(dst as usize)))
    }

    fn adj_list(&self, vertex_id: usize, dir: Direction) -> Option<StructArray> {
        let row: usize = vertex_id.into();

        let chunks = match dir {
            Direction::OUT => self.outbound(),
            Direction::IN => self.inbound(),
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

    fn outbound(&self) -> Vec<Box<dyn Array>> {
        self.adj_out_chunks.iter().map(|c| c[0].clone()).collect() // FIXME: don't collect here
    }

    fn inbound(&self) -> Vec<Box<dyn Array>> {
        todo!("inbound not done yet")
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

        let graph = TempColGraphFragment::from_sorted_edge_list(
            df,
            "src",
            "dst",
            "time",
            100,
            test_dir.path(),
        )
        .unwrap();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges().collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(0), VID(1))];
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

        let graph = TempColGraphFragment::from_sorted_edge_list(
            df,
            "src",
            "dst",
            "time",
            100,
            test_dir.path(),
        )
        .unwrap();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges().collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(0), VID(1))];
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

        let graph = TempColGraphFragment::from_sorted_edge_list(
            df,
            "src",
            "dst",
            "time",
            100,
            test_dir.path(),
        )
        .unwrap();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2)), (EID(2), VID(3))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges().collect::<Vec<_>>();
        let expected = vec![
            (EID(0), VID(0), VID(1)),
            (EID(1), VID(0), VID(2)),
            (EID(2), VID(0), VID(3)),
            (EID(3), VID(1), VID(2)),
            (EID(4), VID(1), VID(3)),
            (EID(5), VID(1), VID(4)),
            (EID(6), VID(2), VID(3)),
            (EID(7), VID(2), VID(4)),
            (EID(8), VID(2), VID(5)),
            (EID(9), VID(3), VID(4)),
            (EID(10), VID(3), VID(5)),
            (EID(11), VID(3), VID(6)),
        ];
        assert_eq!(actual, expected);
    }

    #[test]
    fn load_muliple_sorted_edges_no_props_multiple_ts() {
        let df = DataFrame::new(vec![
            Series::new("src", vec![1u64, 1u64, 1u64, 2u64, 2u64, 2u64]),
            Series::new("dst", vec![2u64, 3u64, 3u64, 3u64, 4u64, 4u64]),
            Series::new("time", vec![0i64, 1i64, 2i64, 3i64, 4i64, 5i64]),
        ])
        .unwrap();

        let test_dir = TempDir::new().unwrap();

        let graph = TempColGraphFragment::from_sorted_edge_list(
            df,
            "src",
            "dst",
            "time",
            100,
            test_dir.path(),
        )
        .unwrap();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges().collect::<Vec<_>>();
        let expected = vec![
            (EID(0), VID(0), VID(1)),
            (EID(1), VID(0), VID(2)),
            (EID(2), VID(1), VID(2)),
            (EID(3), VID(1), VID(3)),
        ];
        assert_eq!(actual, expected);
    }

    #[test]
    fn load_muliple_sorted_edges_no_props_multiple_ts_chunks_size_1() {
        let df = DataFrame::new(vec![
            Series::new("src", vec![1u64, 1u64, 1u64, 2u64, 2u64, 2u64]),
            Series::new("dst", vec![2u64, 3u64, 3u64, 3u64, 4u64, 4u64]),
            Series::new("time", vec![0i64, 1i64, 2i64, 3i64, 4i64, 5i64]),
        ])
        .unwrap();

        let test_dir = TempDir::new().unwrap();

        let graph = TempColGraphFragment::from_sorted_edge_list(
            df,
            "src",
            "dst",
            "time",
            1,
            test_dir.path(),
        )
        .unwrap();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges().collect::<Vec<_>>();
        let expected = vec![
            (EID(0), VID(0), VID(1)),
            (EID(1), VID(0), VID(2)),
            (EID(2), VID(1), VID(2)),
            (EID(3), VID(1), VID(3)),
        ];
        assert_eq!(actual, expected);
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

        let graph = TempColGraphFragment::from_sorted_edge_list(
            df,
            "src",
            "dst",
            "time",
            2,
            test_dir.path(),
        )
        .unwrap();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2)), (EID(2), VID(3))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges().collect::<Vec<_>>();
        let expected = vec![
            (EID(0), VID(0), VID(1)),
            (EID(1), VID(0), VID(2)),
            (EID(2), VID(0), VID(3)),
            (EID(3), VID(1), VID(2)),
            (EID(4), VID(1), VID(3)),
            (EID(5), VID(1), VID(4)),
            (EID(6), VID(2), VID(3)),
            (EID(7), VID(2), VID(4)),
            (EID(8), VID(2), VID(5)),
            (EID(9), VID(3), VID(4)),
            (EID(10), VID(3), VID(5)),
            (EID(11), VID(3), VID(6)),
        ];
        assert_eq!(actual, expected);
    }
}
