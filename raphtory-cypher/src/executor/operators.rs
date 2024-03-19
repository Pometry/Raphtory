use std::{iter, sync::Arc};

use arrow2::{
    array::{Arrow2Arrow as A2A, PrimitiveArray},
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::{DataType, Field},
    types::NativeType,
};
use pl_array::Arrow2Arrow as PolarsA2A;
use polars_arrow::{array as pl_array, offset::OffsetsBuffer};
use polars_core::{
    datatypes::{ArrowDataType, PolarsDataType, UInt64Type},
    frame::DataFrame,
    series::{IntoSeries, Series},
};
use polars_lazy::dsl::Expr;
use raphtory::{
    arrow::{
        chunked_array::array_ops::{ArrayOps, Chunked},
        graph_fragment::TempColGraphFragment,
        graph_impl::ArrowGraph,
        prelude::BaseArrayOps,
    },
    core::Direction,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use super::{Context, DataBlock, Source};

pub trait Operator {
    fn execute(&self, input: DataBlock, ctx: Context) -> Box<dyn Iterator<Item = DataBlock>>;
}

#[derive(Debug, Clone)]
pub enum PhysicalOperator {
    Expand(Expand),
    Filter(Filter),
    Project(Project),
}

impl PhysicalOperator {
    pub fn boxed(self) -> Box<dyn Operator> {
        match self {
            PhysicalOperator::Expand(op) => Box::new(op),
            PhysicalOperator::Filter(op) => Box::new(op),
            PhysicalOperator::Project(op) => Box::new(op),
        }
    }
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

#[derive(Debug, Clone)]
pub struct Project {
    columns: Arc<[usize]>,
}

impl Operator for Project {
    fn execute(&self, input: DataBlock, ctx: Context) -> Box<dyn Iterator<Item = DataBlock>> {
        let df = input.data;
        let columns = self
            .columns
            .iter()
            .filter_map(|col| df.select_at_idx(*col))
            .cloned();
        Box::new(iter::once(DataBlock {
            data: DataFrame::new(columns.collect()).unwrap(),
        }))
    }
}

pub struct EdgeScan {
    layer: String,
    columns: Arc<[(String, usize)]>,
}

impl EdgeScan {
    pub fn new<S: AsRef<str>>(
        layer: impl AsRef<str>,
        columns: impl IntoIterator<Item = (S, usize)>,
    ) -> Self {
        Self {
            layer: layer.as_ref().to_string(),
            columns: columns
                .into_iter()
                .map(|(name, col_id)| (name.as_ref().to_string(), col_id))
                .collect(),
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

        let out = (0..num_chunks).into_par_iter().try_for_each(|chunk_id| {
            let time_values = values.chunk(chunk_id);
            let start_offset = chunk_id * chunk_size;
            let end_offset = (chunk_id + 1) * chunk_size;

            let (start, end, local_offsets) = offsets.make_local_offsets(start_offset, end_offset);

            let offsets: OffsetsBuffer<i64> =
                polars_arrow::offset::Offsets::try_from(local_offsets)
                    .expect("Failed to make offsets")
                    .into();

            let srcs = edges.srcs().sliced(start..end);
            let dsts = edges.dsts().sliced(start..end);

            // take every chunk here and surface the primitive arrays
            // convert from arrow2 to arrow-rs then to polars
            let (srcs, dsts): (Vec<_>, Vec<_>) = srcs
                .iter_chunks()
                .zip(dsts.iter_chunks())
                .map(|(srcs, dsts)| {
                    let srcs = buffer_to_polars_array::<u64>(srcs, None);
                    let dsts = buffer_to_polars_array::<u64>(dsts, None);
                    (srcs.boxed(), dsts.boxed())
                })
                .unzip();

            let srcs = make_chunked_array::<UInt64Type>(srcs, "src").into_series();
            let dsts = make_chunked_array::<UInt64Type>(dsts, "dst").into_series();

            let time = make_time_col(time_values, offsets.clone());

            let time: Series =
                Series::from_arrow("time", time.boxed()).expect("Failed to make time series");

            let mut columns = vec![srcs, dsts, time];

            for (name, col_id) in self.columns.iter() {
                let series = property_to_polars_series(
                    layer,
                    name,
                    *col_id,
                    chunk_id,
                    edges.data_type(),
                    offsets.clone(),
                );
                columns.push(series);
            }

            let df =
                polars_core::frame::DataFrame::new(columns).expect("Failed to make a dataframe");

            let block = DataBlock { data: df };
            producer(block);
            Ok(())
        });

        out
    }
}

fn property_to_polars_series(
    graph: &TempColGraphFragment,
    name: &str,
    col_id: usize,
    chunk_id: usize,
    fields: &[Field],
    offsets: OffsetsBuffer<i64>,
) -> Series {
    let edges = graph.edges_storage();

    let values = match fields[col_id].data_type() {
        DataType::Int8 => {
            let col = edges.t_prop_col_at_chunk::<i8>(col_id, chunk_id);
            let prop = arrow2_to_polars_array(col);
            prop.boxed()
        }
        DataType::Int16 => {
            let col = edges.t_prop_col_at_chunk::<i16>(col_id, chunk_id);
            let prop = arrow2_to_polars_array(col);
            prop.boxed()
        }
        DataType::Int32 => {
            let col = edges.t_prop_col_at_chunk::<i32>(col_id, chunk_id);
            let prop = arrow2_to_polars_array(col);
            prop.boxed()
        }
        DataType::Int64 => {
            let col = edges.t_prop_col_at_chunk::<i64>(col_id, chunk_id);
            let prop = arrow2_to_polars_array(col);
            prop.boxed()
        }
        DataType::UInt8 => {
            let col = edges.t_prop_col_at_chunk::<u8>(col_id, chunk_id);
            let prop = arrow2_to_polars_array(col);
            prop.boxed()
        }
        DataType::UInt16 => {
            let col = edges.t_prop_col_at_chunk::<u16>(col_id, chunk_id);
            let prop = arrow2_to_polars_array(col);
            prop.boxed()
        }
        DataType::UInt32 => {
            let col = edges.t_prop_col_at_chunk::<u32>(col_id, chunk_id);
            let prop = arrow2_to_polars_array(col);
            prop.boxed()
        }
        DataType::UInt64 => {
            let col = edges.t_prop_col_at_chunk::<u64>(col_id, chunk_id);
            let prop = arrow2_to_polars_array(col);
            prop.boxed()
        }
        DataType::Float32 => {
            let col = edges.t_prop_col_at_chunk::<f32>(col_id, chunk_id);
            let prop = arrow2_to_polars_array(col);
            prop.boxed()
        }
        DataType::Float64 => {
            let col = edges.t_prop_col_at_chunk::<f64>(col_id, chunk_id);
            let prop = arrow2_to_polars_array(col);
            prop.boxed()
        }
        DataType::Utf8 => {
            let col = edges.utf8_t_prop_col_at_chunk::<i32>(col_id, chunk_id);
            let prop = utf8_arrow2_to_polars_array(col);
            prop.boxed()
        }
        DataType::LargeUtf8 => {
            let col = edges.utf8_t_prop_col_at_chunk::<i64>(col_id, chunk_id);
            let prop = utf8_arrow2_to_polars_array(col);
            prop.boxed()
        }
        _ => todo!(),
    };

    let list_col = pl_array::ListArray::new(
        ArrowDataType::LargeList(Box::new(polars_arrow::datatypes::Field::new(
            name,
            values.data_type().clone(),
            true,
        ))),
        offsets,
        values,
        None,
    );

    Series::from_arrow(name, list_col.boxed()).expect("Failed to make series for property")
}

fn buffer_to_polars_array<T: NativeType + polars_arrow::types::NativeType>(
    buffer: &Buffer<T>,
    validity: Option<Bitmap>,
) -> pl_array::PrimitiveArray<T> {
    let dt = DataType::from(<T as arrow2::types::NativeType>::PRIMITIVE);
    let prim_array = PrimitiveArray::new(dt, buffer.clone(), validity);
    arrow2_to_polars_array(&prim_array)
}

fn arrow2_to_polars_array<T: NativeType + polars_arrow::types::NativeType>(
    array: &arrow2::array::PrimitiveArray<T>,
) -> pl_array::PrimitiveArray<T> {
    let prim_array = array.to_data();
    let prim_array: pl_array::PrimitiveArray<T> =
        polars_arrow::array::PrimitiveArray::from_data(&prim_array);
    prim_array
}

fn utf8_arrow2_to_polars_array<I: arrow2::types::Offset + polars_arrow::offset::Offset>(
    array: &arrow2::array::Utf8Array<I>,
) -> pl_array::Utf8Array<I> {
    let prim_array = array.to_data();
    let prim_array: pl_array::Utf8Array<I> = polars_arrow::array::Utf8Array::from_data(&prim_array);
    prim_array
}

fn make_time_col(
    time_values: &Buffer<i64>,
    local_offsets: OffsetsBuffer<i64>,
) -> pl_array::ListArray<i64> {
    let time_values = PrimitiveArray::new(DataType::Int64, time_values.clone(), None).to_data();
    let time_values: pl_array::PrimitiveArray<i64> =
        polars_arrow::array::PrimitiveArray::from_data(&time_values);

    pl_array::ListArray::new(
        ArrowDataType::LargeList(Box::new(polars_arrow::datatypes::Field::new(
            "time",
            ArrowDataType::Int64,
            false,
        ))),
        local_offsets,
        time_values.boxed(),
        None,
    )
}

fn make_chunked_array<T: PolarsDataType>(
    chunks: Vec<Box<dyn pl_array::Array>>,
    name: &str,
) -> polars_core::prelude::ChunkedArray<T> {
    let srcs: polars_core::chunked_array::ChunkedArray<T> =
        unsafe { polars_core::chunked_array::ChunkedArray::from_chunks(name, chunks) };
    srcs
}

pub struct NodeScan {
    columns: Arc<[usize]>, // name could be one column
}

impl Operator for Expand {
    fn execute(&self, input: DataBlock, ctx: Context) -> Box<dyn Iterator<Item = DataBlock>> {
        Box::new(iter::empty())
    }
}

impl Operator for Filter {
    fn execute(&self, input: DataBlock, ctx: Context) -> Box<dyn Iterator<Item = DataBlock>> {
        Box::new(iter::empty())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use polars_core::prelude::*;
    use polars_lazy::{dsl::concat_list, prelude::*};

    use raphtory::arrow::{graph_impl::ArrowGraph, Time};
    use tempfile::tempdir;

    fn check_edge_scan_sanity(
        mut edges: Vec<(u64, u64, Time, f64)>,
        chunk_size: usize,
        t_prop_chunk_size: usize,
    ) {
        edges.sort_by_key(|(src, dst, time, _)| (*src, *dst, *time));
        let graph_dir = tempdir().unwrap();
        let graph = ArrowGraph::make_simple_graph(graph_dir, &edges, chunk_size, t_prop_chunk_size);

        let (((src_col, dst_col), times_col), weight_col): (((Vec<_>, Vec<_>), Vec<_>), Vec<_>) =
            graph
                .all_edges(0)
                .map(|edge| {
                    let times = edge.timestamps().into_iter_t().collect::<Vec<_>>();

                    let weight = edge
                        .prop_history::<f64>(1)
                        .map(|(_, v)| v)
                        .collect::<Vec<_>>();
                    let weight_arr = pl_array::PrimitiveArray::from_vec(weight);
                    let weight_series = Series::from_arrow("weight", weight_arr.boxed())
                        .expect("Failed to make weight series");
                    let src = edge.src().as_u64();
                    let dst = edge.dst().as_u64();
                    let ts_arr = polars_arrow::array::PrimitiveArray::from_vec(times);
                    let ts_series = Series::from_arrow("time", ts_arr.boxed())
                        .expect("Failed to make time series");
                    (((src, dst), ts_series), weight_series)
                })
                .unzip();

        let expected = df!(
            "src" => &src_col,
            "dst" => &dst_col,
            "time" => &times_col,
            "weight" => &weight_col
        )
        .unwrap()
        .lazy()
        .sort_by_exprs([col("src"), col("dst")], [false, false], true, false)
        .collect()
        .unwrap();

        let edge_scan = EdgeScan::new("_default", [("weight", 1)]);

        let tp = rayon::ThreadPoolBuilder::new().build().unwrap();

        let (send, recv) = std::sync::mpsc::channel();

        tp.install(|| {
            edge_scan
                .produce(
                    &graph,
                    Arc::new(|data| send.send(data.data).expect("Failed to send data")),
                )
                .expect("Failed to produce data");
        });

        drop(send);

        let dfs = recv
            .iter()
            .reduce(|df1, df2| df1.vstack(&df2).unwrap())
            .unwrap();

        let df = dfs
            .lazy()
            .sort_by_exprs([col("src"), col("dst")], [false, false], true, false)
            .group_by_stable(["src", "dst"])
            .agg([
                concat_list(["time"]).unwrap().flatten(),
                concat_list(["weight"]).unwrap().flatten(),
            ])
            .collect()
            .unwrap();

        assert_eq!(df, expected);
    }

    #[test]
    fn test_edge_scan_sanity_simple() {
        let edges = vec![
            (0, 1, 0, 1.0),
            (0, 1, 1, 2.0),
            (0, 1, 2, 3.0),
            (0, 2, 3, 4.0),
            (0, 3, 4, 5.0),
        ];
        check_edge_scan_sanity(edges, 2, 2);
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_edge_scan_sanity_proptest(
            edges in prop::collection::vec((0u64..10u64, 0u64..10u64, 0i64..10i64, 0f64..5f64), 1..25),
            chunk_size in 2usize..100,
            t_prop_chunk_size in 2usize..100
        ) {
            check_edge_scan_sanity(edges, chunk_size, t_prop_chunk_size);
        }
    }
}
