use super::{select_columns, ChunkCollector, ExportTable};
use crate::{
    core::entities::VID,
    db::{
        api::{
            state::{ops::NodeFilterOp, Index},
            view::internal::FilterOps,
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    errors::GraphError,
    parquet_encoder::{
        encode_node_metadata, encode_node_rows, node_cprop_fixed_fields, node_cprop_schema,
        node_tprop_fixed_fields, node_tprop_schema, LAYER_ID_COL, NODE_VID_COL, TYPE_ID_COL,
    },
    prelude::GraphViewOps,
};
use arrow::datatypes::{Schema, SchemaRef};
use raphtory_api::core::storage::arc_str::ArcStr;
use raphtory_storage::graph::graph::GraphStorage;
use rayon::prelude::*;
use std::collections::HashSet;

/// Builds the node update table of `nodes`: one row per update of each node, on its node layer.
/// With `latest_only`, only the updates that hold the latest value of a property are kept, each with
/// just those properties.
///
/// The columns are the node encoder's fixed columns, without the internal node and layer ids,
/// followed by one column per temporal property visible in the view. The time column holds epoch
/// milliseconds, or UTC datetimes when `convert_datetime` is set. Rows are grouped by node, in the
/// order of the collection.
pub fn node_updates<'graph, G, GH, F>(
    nodes: &Nodes<'graph, G, GH, F>,
    latest_only: bool,
    convert_datetime: bool,
) -> Result<ExportTable, GraphError>
where
    G: GraphViewOps<'graph>,
    GH: GraphViewOps<'graph>,
    F: NodeFilterOp + Clone + 'graph,
{
    let graph = &nodes.graph;

    let encoded = node_tprop_schema(graph)?;
    let num_fixed = node_tprop_fixed_fields(encoded.field(0).data_type()).len();
    let visible = graph
        .node_visible_temporal_prop_ids()
        .filter_map(|id| graph.node_visible_temporal_prop_name(id))
        .collect();
    let columns = exported_columns(&encoded, num_fixed, &visible);

    let storage = nodes.base_graph.core_graph().lock();
    let index = nodes.node_list().into_index(&storage);
    let collector = ChunkCollector::default();
    encode_node_rows(
        graph,
        node_chunks(nodes, &storage, &index),
        latest_only,
        |_, chunk, _| Ok(collector.sink(chunk)),
    )?;
    select_columns(
        &encoded,
        collector.into_batches(),
        &columns,
        convert_datetime,
    )
}

/// Builds the node metadata table of `nodes`: one row per node, in the order of the collection.
///
/// The columns are the node metadata encoder's fixed columns, without the internal node and node
/// type ids, followed by one column per metadata key visible in the view.
pub fn node_metadata<'graph, G, GH, F>(
    nodes: &Nodes<'graph, G, GH, F>,
) -> Result<ExportTable, GraphError>
where
    G: GraphViewOps<'graph>,
    GH: GraphViewOps<'graph>,
    F: NodeFilterOp + Clone + 'graph,
{
    let graph = &nodes.graph;

    let encoded = node_cprop_schema(graph)?;
    let num_fixed = node_cprop_fixed_fields(encoded.field(0).data_type()).len();
    let visible = graph
        .node_visible_metadata_ids()
        .filter_map(|id| graph.node_visible_metadata_name(id))
        .collect();
    let columns = exported_columns(&encoded, num_fixed, &visible);

    let storage = nodes.base_graph.core_graph().lock();
    let index = nodes.node_list().into_index(&storage);
    let collector = ChunkCollector::default();
    encode_node_metadata(
        graph,
        node_chunks(nodes, &storage, &index),
        |_, chunk, _| Ok(collector.sink(chunk)),
    )?;
    select_columns(&encoded, collector.into_batches(), &columns, false)
}

/// The columns of the encoder schema `encoded` to export: its first `num_fixed` columns without
/// the internal ids, followed by the properties named in `visible`.
fn exported_columns(encoded: &Schema, num_fixed: usize, visible: &HashSet<ArcStr>) -> Vec<usize> {
    (0..encoded.fields().len())
        .filter(|&i| {
            let name = encoded.field(i).name().as_str();
            if i < num_fixed {
                ![NODE_VID_COL, LAYER_ID_COL, TYPE_ID_COL].contains(&name)
            } else {
                visible.contains(name)
            }
        })
        .collect()
}

/// Splits `nodes`, whose positions are given by `index`, into chunks for parallel encoding.
/// Chunks are numbered in the order of the collection, so rows come out in that order.
fn node_chunks<'a, 'graph, G, GH, F>(
    nodes: &'a Nodes<'graph, G, GH, F>,
    storage: &'a GraphStorage,
    index: &'a Index<VID>,
) -> impl ParallelIterator<Item = (usize, impl Iterator<Item = NodeView<'a, &'a GH>> + 'a)> + 'a
where
    G: GraphViewOps<'graph>,
    GH: GraphViewOps<'graph>,
    F: NodeFilterOp + Clone + 'graph,
{
    let graph = &nodes.graph;
    let chunk_size = (index.len() / rayon::current_num_threads().max(1)).max(1);
    let keep = move |vid: &VID| {
        storage.try_core_node(*vid).is_some_and(|node| {
            nodes.base_graph.filter_node(node.as_ref()) && nodes.predicate.apply(storage, *vid)
        })
    };
    (0..index.len().div_ceil(chunk_size))
        .into_par_iter()
        .map(move |chunk| {
            let positions = chunk * chunk_size..((chunk + 1) * chunk_size).min(index.len());
            let vids = positions
                .filter_map(move |pos| index.value(pos))
                .filter(keep);
            (
                chunk,
                vids.map(move |vid| NodeView::new_internal(graph, vid)),
            )
        })
}
