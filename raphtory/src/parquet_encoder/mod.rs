use crate::{
    db::api::state::ops::GraphView, errors::GraphError, parquet_encoder::model::get_id_type,
    prelude::*,
};
use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use arrow_json::{reader::Decoder, ReaderBuilder};
use itertools::Itertools;
use model::ParquetTEdge;
use raphtory_api::core::entities::{
    properties::{meta::PropMapper, prop::arrow_dtype_from_prop_type},
    GidType,
};
use rayon::{prelude::*, ThreadPool, ThreadPoolBuilder};
use std::{ops::Range, sync::LazyLock};

mod edges;
mod model;
mod nodes;

mod graph;

pub(crate) use edges::{encode_edge_cprop, encode_edge_deletions, encode_edge_tprop};
pub(crate) use graph::{encode_graph_cprop, encode_graph_tprop};
pub(crate) use nodes::{encode_nodes_cprop, encode_nodes_tprop};

const ROW_GROUP_SIZE: usize = 100_000;
pub(crate) const NODE_GID_COL: &str = "rap_node_gid";
pub(crate) const NODE_VID_COL: &str = "rap_node_vid";
pub(crate) const TYPE_COL: &str = "rap_node_type";
pub(crate) const TYPE_ID_COL: &str = "rap_node_type_id";
pub(crate) const TIME_COL: &str = "rap_time";
pub(crate) const SECONDARY_INDEX_COL: &str = "rap_secondary_index";
pub(crate) const SRC_VID_COL: &str = "rap_src_vid";
pub(crate) const DST_VID_COL: &str = "rap_dst_vid";
pub(crate) const SRC_GID_COL: &str = "rap_src_gid";
pub(crate) const DST_GID_COL: &str = "rap_dst_gid";
pub(crate) const EDGE_COL_ID: &str = "rap_edge_id";
pub(crate) const LAYER_COL: &str = "rap_layer";
pub(crate) const LAYER_ID_COL: &str = "rap_layer_id";

pub trait RecordBatchSink {
    fn send_batch(&mut self, batch: RecordBatch) -> Result<(), GraphError>;
    fn finish(self) -> Result<(), GraphError>
    where
        Self: Sized;
}

pub(crate) fn run_encode<G: GraphView, S: RecordBatchSink>(
    g: &G,
    meta: &PropMapper,
    size: usize,
    make_sink_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
    default_fields_fn: impl Fn(&DataType) -> Vec<Field>,
    encode_fn: impl Fn(Range<usize>, &G, &mut Decoder, &mut S) -> Result<(), GraphError> + Sync,
) -> Result<(), GraphError> {
    let schema = derive_schema(meta, g.id_type(), default_fields_fn)?;

    if size > 0 {
        let chunk_size = (size / rayon::current_num_threads()).max(128);
        let iter = (0..size).into_par_iter().step_by(chunk_size);

        let num_digits = iter.len().to_string().len();

        iter.enumerate().try_for_each(|(chunk, first)| {
            let items = first..(first + chunk_size).min(size);

            let mut sink = make_sink_fn(schema.clone(), chunk, num_digits)?;
            let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder()?;

            encode_fn(items, g, &mut decoder, &mut sink)?;

            sink.finish()?;
            Ok::<_, GraphError>(())
        })?;
    }
    Ok(())
}

pub(crate) fn run_encode_indexed<
    Index,
    II: Iterator<Item = Index>,
    G: GraphView,
    S: RecordBatchSink,
>(
    g: &G,
    meta: &PropMapper,
    items: impl ParallelIterator<Item = (usize, II)>,
    make_sink_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
    default_fields_fn: impl Fn(&DataType) -> Vec<Field>,
    encode_fn: impl Fn(II, &G, &mut Decoder, &mut S) -> Result<(), GraphError> + Sync,
) -> Result<(), GraphError> {
    let schema = derive_schema(meta, g.id_type(), default_fields_fn)?;
    let num_digits = 8;

    items.try_for_each(|(chunk, items)| {
        let mut sink = make_sink_fn(schema.clone(), chunk, num_digits)?;
        let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder()?;

        encode_fn(items, g, &mut decoder, &mut sink)?;

        sink.finish()?;
        Ok::<_, GraphError>(())
    })?;

    Ok(())
}

pub(crate) fn derive_schema(
    prop_meta: &PropMapper,
    id_type: Option<GidType>,
    default_fields_fn: impl Fn(&DataType) -> Vec<Field>,
) -> Result<SchemaRef, GraphError> {
    let fields = arrow_fields(prop_meta);
    let id_type = get_id_type(id_type);

    let make_schema = |id_type: DataType, prop_columns: Vec<Field>| {
        let default_fields = default_fields_fn(&id_type);

        Schema::new(
            default_fields
                .into_iter()
                .chain(prop_columns)
                .collect::<Vec<_>>(),
        )
        .into()
    };

    let schema = if let Ok(id_type) = id_type {
        make_schema(id_type, fields)
    } else {
        make_schema(DataType::UInt64, fields)
    };

    Ok(schema)
}

fn arrow_fields(meta: &PropMapper) -> Vec<Field> {
    meta.keys()
        .iter()
        .zip(meta.ids())
        .filter_map(|(name, prop_id)| {
            meta.get_dtype(prop_id)
                .map(move |prop_type| (name, prop_type))
        })
        .map(|(name, prop_type)| {
            let d_type = arrow_dtype_from_prop_type(&prop_type);
            Field::new(name, d_type, true)
        })
        .collect()
}

pub(crate) static ENCODE_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    ThreadPoolBuilder::new()
        .thread_name(|idx| format!("PS Encode Thread-{idx}"))
        .build()
        .unwrap()
});
