use super::{select_columns, ChunkCollector, ExportTable};
use crate::{
    core::entities::VID,
    db::{
        api::{state::ops::NodeFilterOp, view::internal::FilterOps},
        graph::{node::NodeView, nodes::Nodes},
    },
    errors::GraphError,
    parquet_encoder::{
        encode_node_rows, node_tprop_fixed_fields, node_tprop_schema, LAYER_ID_COL, NODE_VID_COL,
    },
    prelude::GraphViewOps,
};
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
    let visible: HashSet<_> = graph
        .node_visible_temporal_prop_ids()
        .filter_map(|id| graph.node_visible_temporal_prop_name(id))
        .collect();
    let columns: Vec<usize> = (0..encoded.fields().len())
        .filter(|&i| {
            let name = encoded.field(i).name().as_str();
            if i < num_fixed {
                // we don't export the internal node and layer ids
                name != NODE_VID_COL && name != LAYER_ID_COL
            } else {
                visible.contains(name)
            }
        })
        .collect();

    // chunks are numbered in the order of the collection, so rows come out in that order
    let storage = nodes.base_graph.core_graph().lock();
    let index = nodes.node_list().into_index(&storage);
    let chunk_size = (index.len() / rayon::current_num_threads().max(1)).max(1);
    let index = &index;
    let keep = &|vid: &VID| {
        storage.try_core_node(*vid).is_some_and(|node| {
            nodes.base_graph.filter_node(node.as_ref()) && nodes.predicate.apply(&storage, *vid)
        })
    };
    let chunks = (0..index.len().div_ceil(chunk_size))
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
        });

    let collector = ChunkCollector::default();
    encode_node_rows(graph, chunks, latest_only, |_, chunk, _| {
        Ok(collector.sink(chunk))
    })?;
    select_columns(
        &encoded,
        collector.into_batches(),
        &columns,
        convert_datetime,
    )
}
