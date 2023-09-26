use std::{collections::HashMap, sync::Arc};

use itertools::{Chunk, Itertools};
use polars_core::{
    prelude::*,
    utils::arrow::{
        array::{
            Array, ListArray, MutableArray, MutableListArray, MutablePrimitiveArray,
            MutableStructArray, PrimitiveArray, StructArray,
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

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Polars error: {0}")]
    Polars(#[from] polars_core::prelude::PolarsError),
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

const V_COLUMN: &str = "v";
const E_COLUMN: &str = "e";

type Time = i64;

#[derive(Debug)]
pub struct SparseTable {
    sorted_gids: Vec<u64>,

    adj_out_dst_chunks: Vec<Vec<u64>>,
    adj_out_eid_chunks: Vec<Vec<u64>>,
    adj_out_offsets_chunks: Vec<Vec<i64>>,

    edge_time_offsets: Vec<i64>,
    time_col: Series,
}

impl SparseTable {
    fn into_graph(self) -> TempColGraphFragment {
        let fields = vec![
            ArrowField::new(V_COLUMN, ArrowDataType::UInt64, false),
            ArrowField::new(E_COLUMN, ArrowDataType::UInt64, false),
        ];
        let schema = ArrowDataType::Struct(fields);

        let list_outbound_chunks = self
            .adj_out_dst_chunks
            .into_iter()
            .zip(self.adj_out_eid_chunks.into_iter())
            .zip(self.adj_out_offsets_chunks.into_iter())
            .map(|((adj_out_dst, adj_out_eid), adj_out_offsets)| {
                let dst_col = Box::new(MutablePrimitiveArray::<u64>::from_vec(adj_out_dst));
                let eid_col = Box::new(MutablePrimitiveArray::<u64>::from_vec(adj_out_eid));

                let values = MutableStructArray::new(schema.clone(), vec![dst_col, eid_col]);

                let outbound2 = MutableListArray::new_from_mutable(
                    values,
                    Offsets::try_from(adj_out_offsets).unwrap(),
                    None,
                );

                let outbound: ListArray<i64> = outbound2.into();
                let outbound: Box<dyn Array> = Box::new(outbound);
                outbound
            })
            .collect_vec();

        let outbound: ChunkedArray<ListType> =
            unsafe { ChunkedArray::from_chunks(OUTBOUND_COLUMN, list_outbound_chunks) };

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
    fn from_sorted_edge_list(
        src_dst_frame: DataFrame, // sorted_by (src, dst, time)
        src_col: &str,
        dst_col: &str,
        time_col: &str,
    ) -> Result<Self, Error> {
        let mut vertex_idx = 0;

        // global id -> phisical id
        // let mut gid_to_pid = HashMap::default();

        let src_col = src_dst_frame.column(src_col)?;
        let dst_col = src_dst_frame.column(dst_col)?;
        let time_col = src_dst_frame.column(time_col)?;

        if let (Ok(src), Ok(dst), Ok(time)) = (src_col.u64(), dst_col.u64(), time_col.i64()) {
            let sprs_table = Self::build_tables(src, dst, time, 100);
        } else {
            todo!()
        }

        Ok(Self {
            fragments: Arc::new([]),
        })
    }

    fn push_chunks(
        adj_out_eid: &mut Vec<u64>,
        adj_out_dst: &mut Vec<u64>,
        adj_out_offsets: &mut Vec<i64>,
        adj_out_eid_chunks: &mut Vec<Vec<u64>>,
        adj_out_dst_chunks: &mut Vec<Vec<u64>>,
        adj_out_offsets_chunks: &mut Vec<Vec<i64>>,
        chunk_adj_out_offset: &mut i64,
    ) {
        let mut adj_out_eid_prev = Vec::with_capacity(adj_out_eid.len());
        let mut adj_out_dst_prev = Vec::with_capacity(adj_out_dst.len());
        let mut adj_out_offsets_prev = Vec::with_capacity(adj_out_offsets.len());
        // adj_out_offsets_prev.push(0);

        std::mem::swap(&mut adj_out_eid_prev, adj_out_eid);
        std::mem::swap(&mut adj_out_dst_prev, adj_out_dst);
        std::mem::swap(&mut adj_out_offsets_prev, adj_out_offsets);

        adj_out_offsets_prev.push(*chunk_adj_out_offset);

        adj_out_eid_chunks.push(adj_out_eid_prev);
        adj_out_dst_chunks.push(adj_out_dst_prev);
        adj_out_offsets_chunks.push(adj_out_offsets_prev);

        *chunk_adj_out_offset = 0i64;
    }

    pub fn build_tables(
        srcs: &ChunkedArray<UInt64Type>,
        dsts: &ChunkedArray<UInt64Type>,
        times: &ChunkedArray<Int64Type>,
        chunk_size: usize,
    ) -> SparseTable {
        let mut sorted_gids: Vec<u64> = vec![];

        for chunk in srcs.chunks() {
            let arr = chunk
                .as_any()
                .downcast_ref::<PrimitiveArray<u64>>()
                .unwrap();
            for v in arr.values().iter() {
                if sorted_gids.last() == Some(v) {
                    continue;
                } else {
                    sorted_gids.push(*v);
                }
            }
        }

        // to be chunked
        let mut adj_out_eid_chunks: Vec<Vec<u64>> = vec![];
        let mut adj_out_dst_chunks: Vec<Vec<u64>> = vec![];
        let mut adj_out_offsets_chunks: Vec<Vec<i64>> = vec![];

        let mut adj_out_eid = vec![];
        let mut adj_out_dst = vec![];
        let mut adj_out_offsets = vec![0];

        let mut edge_time_offsets: Vec<i64> = vec![0];

        let mut e_id: u64 = 0;
        let mut chunk_adj_out_offset: i64 = 0;
        let mut last_edge: Option<(u64, u64)> = None;
        let mut vertex_count = 0;

        // g_id, [{v_id1, e_id1}, {v_id2, e_id2}, ...]
        for (event_id, ((src, dst), time)) in srcs
            .into_iter()
            .flatten()
            .zip(dsts.into_iter().flatten())
            .zip(times.into_iter().flatten())
            .enumerate()
        {
            // check if can have a chunk cutoff
            if last_edge.filter(|(prev_src, _)| prev_src != &src).is_some()
                && (vertex_count+1) % chunk_size == 0
            {
                println!("chunk cut off at {last_edge:?} {src} {dst} {time}");
                Self::push_chunks(
                    &mut adj_out_eid,
                    &mut adj_out_dst,
                    &mut adj_out_offsets,
                    &mut adj_out_eid_chunks,
                    &mut adj_out_dst_chunks,
                    &mut adj_out_offsets_chunks,
                    &mut chunk_adj_out_offset,
                );
            }

            if Some((src, dst)) != last_edge {
                adj_out_eid.push(e_id);
                let dst_idx = if let Ok(dst_idx) = sorted_gids.binary_search(&dst) {
                    dst_idx
                } else {
                    sorted_gids.push(dst);
                    sorted_gids.len() - 1
                };
                adj_out_dst.push(dst_idx as u64);

                if let Some((prev_src, prev_dst)) = last_edge {
                    if prev_src != src {
                        adj_out_offsets.push(chunk_adj_out_offset);
                        vertex_count += 1;
                    }
                    if prev_src != src || prev_dst != dst {
                        edge_time_offsets.push(event_id as i64);
                    }
                }

                e_id += 1;
                chunk_adj_out_offset += 1;
            }


            last_edge = Some((src, dst));
        }

        if last_edge.is_some() {
            // deal with the last chunk
            let remaining_slots_in_chunk = chunk_size - (adj_out_offsets.len() -1);
            let remaining_vertices = sorted_gids.len() - vertex_count - 1;
            let fill_chunk_remaining = remaining_slots_in_chunk.min(remaining_vertices);
            adj_out_offsets.resize(
                adj_out_offsets.len() + fill_chunk_remaining,
                chunk_adj_out_offset,
            );
            Self::push_chunks(
                &mut adj_out_eid,
                &mut adj_out_dst,
                &mut adj_out_offsets,
                &mut adj_out_eid_chunks,
                &mut adj_out_dst_chunks,
                &mut adj_out_offsets_chunks,
                &mut chunk_adj_out_offset,
            );

            // deal with the rest of the vertices
            let remaining_vertices = remaining_vertices - fill_chunk_remaining;
            let remaining_chunks = remaining_vertices / chunk_size;
            let size_of_last_chunk = remaining_vertices % chunk_size;

            Self::add_empty_chunks(
                remaining_chunks,
                size_of_last_chunk,
                chunk_size,
                &mut adj_out_eid_chunks,
                &mut adj_out_dst_chunks,
                &mut adj_out_offsets_chunks,
            );

            edge_time_offsets.push(times.len() as i64);
        }

        SparseTable {
            sorted_gids,
            adj_out_dst_chunks,
            adj_out_eid_chunks,
            adj_out_offsets_chunks,
            edge_time_offsets,
            time_col: times.clone().into_series(),
        }
    }

    fn add_empty_chunks(
        remaining_chunks: usize,
        size_of_last_chunk: usize,
        chunk_size: usize,
        adj_out_eid_chunks: &mut Vec<Vec<u64>>,
        adj_out_dst_chunks: &mut Vec<Vec<u64>>,
        adj_out_offsets_chunks: &mut Vec<Vec<i64>>,
    ) {
        for _ in 0..remaining_chunks {
            adj_out_offsets_chunks.push(vec![0; chunk_size + 1]);
            adj_out_eid_chunks.push(Vec::with_capacity(0));
            adj_out_dst_chunks.push(Vec::with_capacity(0));
        }
        if size_of_last_chunk > 0 {
            adj_out_offsets_chunks.push(vec![0; size_of_last_chunk + 1]);
            adj_out_eid_chunks.push(Vec::with_capacity(0));
            adj_out_dst_chunks.push(Vec::with_capacity(0));
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn load_one_edge_from_sorted_adj_list_num_vertices_no_props() {
        let df = DataFrame::new(vec![
            Series::new("src", vec![1u64]),
            Series::new("dst", vec![2u64]),
            Series::new("time", vec![0i64]),
        ])
        .unwrap();

        let src = df.column("src").unwrap().u64().unwrap();
        let dst = df.column("dst").unwrap().u64().unwrap();
        let time = df.column("time").unwrap().i64().unwrap();

        let res = TempColGraph::build_tables(src, dst, time, 100);
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

        let src = df.column("src").unwrap().u64().unwrap();
        let dst = df.column("dst").unwrap().u64().unwrap();
        let time = df.column("time").unwrap().i64().unwrap();

        let res = TempColGraph::build_tables(src, dst, time, 100);
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

        let src = df.column("src").unwrap().u64().unwrap();
        let dst = df.column("dst").unwrap().u64().unwrap();
        let time = df.column("time").unwrap().i64().unwrap();

        let res = TempColGraph::build_tables(src, dst, time, 100);

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

        let src = df.column("src").unwrap().u64().unwrap();
        let dst = df.column("dst").unwrap().u64().unwrap();
        let time = df.column("time").unwrap().i64().unwrap();

        let res = TempColGraph::build_tables(src, dst, time, 2);
        let graph = res.into_graph();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();

        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2)), (EID(2), VID(3))];

        assert_eq!(actual, expected)
    }
}
