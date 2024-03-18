use std::{iter, sync::Arc};

use arrow2::{
    array::{Arrow2Arrow as A2A, PrimitiveArray},
    datatypes::DataType,
};
use pl_array::Arrow2Arrow as PolarsA2A;
use polars_arrow::{array as pl_array, datatypes::Field};
use polars_core::{
    datatypes::{ArrowDataType, PolarsDataType, UInt64Type},
    series::{IntoSeries, Series},
};
use polars_lazy::dsl::Expr;
use raphtory::{
    arrow::{
        chunked_array::{
            array_ops::{ArrayOps, Chunked},
            chunked_array::ChunkedArray,
            list_array::ChunkedListArray,
            ChunkedArraySlice,
        },
        graph_impl::ArrowGraph,
        prelude::BaseArrayOps,
    },
    core::Direction,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use super::{Context, DataBlock, Source};

pub trait Operator {
    fn execute(&self, input: DataBlock, ctx: Context) -> impl Iterator<Item = DataBlock>;
}

#[derive(Debug, Clone)]
pub enum PhysicalOperator {
    Expand(Expand),
    Filter(Filter),
}

#[derive(Debug, Clone)]
pub struct Expand {
    dir: Direction,
    from_col: usize,
    filter: Expr,
}

#[derive(Debug, Clone)]
pub struct Filter {
    expr: Expr,
}

pub struct EdgeScan {
    layer: String,
    columns: Arc<[usize]>,
}

impl EdgeScan {
    pub fn new(layer: impl AsRef<str>, columns: Arc<[usize]>) -> Self {
        Self {
            layer: layer.as_ref().to_string(),
            columns,
        }
    }
}

impl Source for EdgeScan {
    fn produce<'a, 'g>(
        &'a self,
        graph: &'g ArrowGraph,
        producer: Arc<dyn Fn(DataBlock) + Send + Sync + 'a>,
    ) -> Result<(), super::ExecError> {
        let layer_id = graph
            .find_layer_id(&self.layer)
            .ok_or(super::ExecError::LayerNotFound(self.layer.clone()))?;
        let layer = graph.layer(layer_id);

        let edges = layer.edges_storage();

        let chunk_size = edges.time().values().chunk_size();
        let chunked_lists_ts = edges.time();
        let offsets = chunked_lists_ts.offsets();
        let values = chunked_lists_ts.values();
        let num_chunks = values.num_chunks();

        println!("ofsets: {:?}", offsets);
        println!("values: {:?}", values);

        let out = (0..num_chunks).into_iter().try_for_each(|chunk_id| {
            let time_values = values.chunk(chunk_id);
            let start_offset = chunk_id * chunk_size;
            let end_offset = (chunk_id + 1) * chunk_size;

            let (start, end, local_offsets) = offsets.make_local_offsets(start_offset, end_offset);
            println!("start_offset: {start_offset}, end_offset: {end_offset}, start: {}, end: {}, local_offsets: {:?}", start, end, local_offsets);

            let srcs = edges.srcs().sliced(start..end);
            let dsts = edges.dsts().sliced(start..end);

            // take every chunk here and surface the primitive arrays
            // convert from arrow2 to arrow-rs then to polars
            let (srcs, dsts): (Vec<_>, Vec<_>) = srcs
                .iter_chunks()
                .zip(dsts.iter_chunks())
                .map(|(srcs, dsts)| {
                    let srcs = PrimitiveArray::new(DataType::UInt64, srcs.clone(), None).to_data();
                    let dsts = PrimitiveArray::new(DataType::UInt64, dsts.clone(), None).to_data();

                    let srcs: pl_array::PrimitiveArray<u64> =
                        polars_arrow::array::PrimitiveArray::from_data(&srcs);
                    let dsts: pl_array::PrimitiveArray<u64> =
                        polars_arrow::array::PrimitiveArray::from_data(&dsts);
                    (srcs.boxed(), dsts.boxed())
                })
                .unzip();

            let srcs: polars_core::chunked_array::ChunkedArray<UInt64Type> =
                unsafe { polars_core::chunked_array::ChunkedArray::from_chunks("src", srcs) };

            let dsts: polars_core::chunked_array::ChunkedArray<UInt64Type> =
                unsafe { polars_core::chunked_array::ChunkedArray::from_chunks("dst", dsts) };

            let srcs = srcs.into_series();
            let dsts = dsts.into_series();

            let time_values =
                PrimitiveArray::new(DataType::Int64, time_values.clone(), None).to_data();
            let time_values: pl_array::PrimitiveArray<i64> =
                polars_arrow::array::PrimitiveArray::from_data(&time_values);

            let offsets = polars_arrow::offset::Offsets::try_from(local_offsets)
                .expect("Failed to make offsets");
            let time = pl_array::ListArray::new(
                ArrowDataType::LargeList(Box::new(Field::new("time", ArrowDataType::Int64, false))),
                offsets.into(),
                time_values.boxed(),
                None,
            );

            let time: Series =
                Series::from_arrow("time", time.boxed()).expect("Failed to make time series");

            let df = polars_core::frame::DataFrame::new(vec![srcs, dsts, time])
                .expect("Failed to make a dataframe");

            let block = DataBlock { data: df };
            producer(block);
            Ok(())
        });

        out
    }
}

pub struct NodeScan {
    columns: Arc<[usize]>, // name could be one column
}

impl Operator for Expand {
    fn execute(&self, input: DataBlock, ctx: Context) -> impl Iterator<Item = DataBlock> {
        iter::empty()
    }
}

impl Operator for Filter {
    fn execute(&self, input: DataBlock, ctx: Context) -> impl Iterator<Item = DataBlock> {
        iter::empty()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use polars_core::df;
    use raphtory::{
        arrow::{graph_impl::ArrowGraph, Time},
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
    };
    use tempfile::tempdir;

    #[test]
    fn part_point_check() {
        let v = vec![0, 3, 4, 5];
        let idx = v.partition_point(|v| v <= &5);
        assert_eq!(idx, 4);
    }

    #[test]
    fn test_edge_scan() {
        use super::*;
        use raphtory::arrow::graph_impl::ArrowGraph;
        use std::sync::Arc;

        let graph = Graph::new();

        let edges = [
            (0i64, 0u64, 1u64),
            (1, 0, 1),
            (2, 0, 1),
            (3, 1, 2),
            (4, 2, 3),
        ];

        for (time, src, dst) in edges.iter() {
            graph
                .add_edge(*time, *src, *dst, NO_PROPS, None)
                .expect("Failed to add edge");
        }

        let graph_dir = tempdir().unwrap();
        let graph = ArrowGraph::from_graph(&graph, graph_dir).unwrap();

        let edge_scan = EdgeScan::new("_default", Arc::new([0, 1, 2]));

        edge_scan
            .produce(
                &graph,
                Arc::new(|data| {
                    println!("{:?}", data);
                }),
            )
            .expect("Failed to produce data");
    }

    fn check_edge_scan_sanity(
        edges: Vec<(u64, u64, Time, f64)>,
        chunk_size: usize,
        t_prop_chunk_size: usize,
    ) {
        let graph_dir = tempdir().unwrap();
        let graph = ArrowGraph::make_simple_graph(graph_dir, &edges, chunk_size, t_prop_chunk_size);

        let ((src_col, dst_col), times_col): ((Vec<_>, Vec<_>), Vec<_>) = graph
            .all_edges(0)
            .map(|edge| {
                let times = edge
                    .timestamps()
                    .into_iter_t()
                    .map(Some)
                    .collect::<Vec<_>>();
                let src = edge.src().as_u64();
                let dst = edge.dst().as_u64();
                let ts_arr: polars_arrow::array::PrimitiveArray<i64> = times.into();
                let ts_series =
                    Series::from_arrow("time", ts_arr.boxed()).expect("Failed to make time series");
                ((src, dst), ts_series)
            })
            .unzip();

        let expected = df!(
            "src" => &src_col,
            "dst" => &dst_col,
            "time" => &times_col
        )
        .unwrap();

        println!("expected {:?}", expected);

        let edge_scan = EdgeScan::new("_default", Arc::new([0, 1, 2]));

        let tp = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();

        let (send, recv) = std::sync::mpsc::channel();

        tp.install(|| {
            edge_scan
                .produce(
                    &graph,
                    Arc::new(|data| {
                        send.send(data.data).expect("Failed to send data")
                    }),
                )
                .expect("Failed to produce data");
        });

        drop(send);

        let dfs = recv.iter().collect::<Vec<_>>();

        println!("{:?}", dfs);
    }

    #[test]
    fn test_edge_scan_sanity_simple() {
        let edges = vec![
            (0, 1, 0, 0.0),
            (0, 1, 1, 0.0),
            (0, 1, 2, 0.0),
            (0, 2, 3, 0.0),
            (0, 3, 4, 0.0),
        ];
        check_edge_scan_sanity(edges, 2, 2);
    }
}
