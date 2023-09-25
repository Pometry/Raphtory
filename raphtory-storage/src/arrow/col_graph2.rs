use std::{collections::HashMap, sync::Arc};

use polars_core::{
    prelude::*,
    utils::arrow::array::{
        MutableListArray, MutablePrimitiveArray, MutableStructArray, PrimitiveArray,
    },
};
use raphtory::core::entities::vertices::vertex_ref::VertexRef;

struct TempColGraphFragment {
    edge_df: DataFrame,
    vertex_df: DataFrame,
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

pub struct SparseTable {
    sorted_gids: Vec<u64>,
    adj_out_dst: Vec<u64>,
    adj_out_eid: Vec<u64>,
    adj_out_offsets: Vec<u64>,
    edge_time_offsets: Vec<u64>,
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
            let src_iter = src.into_iter().filter_map(|x| x);
            let dst_iter = dst.into_iter().filter_map(|x| x);
            let time_iter = time.into_iter().filter_map(|x| x);

            Self::load_sorted_outbound_iter(
                src_iter
                    .zip(dst_iter)
                    .zip(time_iter)
                    .map(|((src, dst), time)| (src, dst, time)),
            );
        }

        Ok(Self {
            fragments: Arc::new([]),
        })
    }

    pub fn build_tables(
        srcs: &ChunkedArray<UInt64Type>,
        dsts: &ChunkedArray<UInt64Type>,
        times: &ChunkedArray<Int64Type>,
        cap: usize,
    ) -> SparseTable {
        // iterate over it once
        // 1. gid_column
        // 2. mapping for every vertex to its fragment

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

        let mut adj_out_eid: Vec<u64> = Vec::with_capacity(cap);
        let mut adj_out_dst: Vec<u64> = Vec::with_capacity(cap);

        let mut adj_out_offsets: Vec<u64> = vec![0];
        let mut edge_time_offsets: Vec<u64> = vec![0];

        let mut e_id: u64 = 0;
        let mut last_edge: Option<(u64, u64)> = None;

        // g_id, [{v_id1, e_id1}, {v_id2, e_id2}, ...]

        for (event_id, ((src, dst), time)) in srcs
            .into_iter()
            .filter_map(|x| x)
            .zip(dsts.into_iter().filter_map(|x| x))
            .zip(times.into_iter().filter_map(|x| x))
            .enumerate()
        {
            if Some((src, dst)) == last_edge {
            } else {
                adj_out_eid.push(e_id);
                let dst_idx = if let Ok(dst_idx) = sorted_gids.binary_search(&dst) {
                    dst_idx
                } else {
                    sorted_gids.push(dst);
                    sorted_gids.len() - 1
                };
                adj_out_dst.push(dst_idx as u64);
                e_id += 1;
            }
            if let Some((prev_src, prev_dst)) = last_edge {
                if prev_src != src {
                    adj_out_offsets.push(e_id);
                }
                if prev_src != src || prev_dst != dst {
                    edge_time_offsets.push(event_id as u64);
                }
            }
            last_edge = Some((src, dst));
        }

        if last_edge.is_some() {
            adj_out_offsets.resize(sorted_gids.len() + 1, e_id);
            edge_time_offsets.push(times.len() as u64);
        }

        SparseTable {
            sorted_gids,
            adj_out_dst,
            adj_out_eid,
            adj_out_offsets,
            edge_time_offsets,
        }
    }

    fn load_sorted_outbound_iter<
        SRC: Into<VertexRef>,
        DST: Into<VertexRef>,
        I: Iterator<Item = (SRC, DST, Time)>,
    >(
        is: I,
    ) {
        let mut vertex_fragments: Vec<MutableVertexFragment> = vec![];
        let mut edge_fragments: Vec<MutableEdgeFragment> = vec![];

        let mut v_id: usize = 0;

        let mut e_id: usize = 0;

        let mut global_to_local: HashMap<u64, usize> = HashMap::default();

        // let mut cur: Option<> = None;

        for (src, dst, time) in is {
            // need the global mapping from gid to physical id
            // need to get the fragment for the src, if it exists
        }
    }
}

struct MutableVertexFragment {
    global_id_arr: MutablePrimitiveArray<u64>,
    outbound_arr: MutableListArray<i64, MutableStructArray>,
    timestamp_arr: MutableListArray<i64, MutablePrimitiveArray<i64>>,
}

impl MutableVertexFragment {
    fn new(cap: usize) -> Self {
        let fields = vec![
            ArrowField::new(V_COLUMN, ArrowDataType::UInt64, false),
            ArrowField::new(E_COLUMN, ArrowDataType::UInt64, false),
        ];

        // arrays for outbound
        let out_v = Box::new(MutablePrimitiveArray::<u64>::with_capacity(cap));
        let out_e = Box::new(MutablePrimitiveArray::<u64>::with_capacity(cap));
        let out_inner =
            MutableStructArray::new(ArrowDataType::Struct(fields.clone()), vec![out_v, out_e]);

        let outbound_arr = MutableListArray::<i64, MutableStructArray>::new_with_field(
            out_inner,
            OUTBOUND_COLUMN,
            false,
        );

        let timestamp_arr = MutableListArray::<i64, MutablePrimitiveArray<i64>>::new_with_field(
            MutablePrimitiveArray::<i64>::with_capacity(cap),
            V_ADDITIONS_COLUMN,
            false,
        );

        Self {
            global_id_arr: MutablePrimitiveArray::<u64>::with_capacity(cap),
            outbound_arr,
            timestamp_arr,
        }
    }
}

struct MutableEdgeFragment {
    src_arr: MutablePrimitiveArray<u64>,
    dst_arr: MutablePrimitiveArray<u64>,
    timestamp_arr: MutableListArray<i64, MutablePrimitiveArray<i64>>,
}

impl MutableEdgeFragment {
    fn new(cap: usize) -> Self {
        Self {
            src_arr: MutablePrimitiveArray::<u64>::with_capacity(cap),
            dst_arr: MutablePrimitiveArray::<u64>::with_capacity(cap),
            timestamp_arr: MutableListArray::<i64, MutablePrimitiveArray<i64>>::new_with_field(
                MutablePrimitiveArray::<i64>::with_capacity(cap),
                E_ADDITIONS_COLUMN,
                false,
            ),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use polars_core::prelude::*;
    #[test]
    fn load_one_edge_from_sorted_adj_list_num_vertices_no_props() {
        let df = DataFrame::new(vec![
            Series::new("src", vec![1]),
            Series::new("dst", vec![2]),
            Series::new("time", vec![0]),
        ])
        .unwrap();

        let src = df.column("src").unwrap().u64().unwrap();
        let dst = df.column("dst").unwrap().u64().unwrap();
        let time = df.column("time").unwrap().i64().unwrap();

        let res = TempColGraph::build_tables(src, dst, time, 1);
        assert_eq!(res.sorted_gids, vec![1, 2]);
        assert_eq!(res.adj_out_dst, vec![1]);
        assert_eq!(res.adj_out_eid, vec![0]);
        assert_eq!(res.adj_out_offsets, vec![0, 1]);
        assert_eq!(res.edge_time_offsets, vec![0, 1]);
    }
}
