use super::{select_columns, ChunkCollector, ExportTable};
use crate::{
    db::{
        api::{state::ops::NodeFilterOp, view::internal::FilterOps},
        graph::{node::NodeView, nodes::Nodes},
    },
    errors::GraphError,
    parquet_encoder::{
        encode_node_rows, get_node_ids_par_iter, node_tprop_fixed_fields, node_tprop_schema,
        LAYER_ID_COL, NODE_VID_COL,
    },
    prelude::GraphViewOps,
};
use rayon::prelude::*;
use std::collections::HashSet;

/// Builds the node update table of `nodes`: one row per update of each node, on its node layer.
///
/// The columns are the node encoder's fixed columns, without the internal node and layer ids,
/// followed by one column per temporal property visible in the view. The time column holds epoch
/// milliseconds, or UTC datetimes when `convert_datetime` is set.
pub fn node_updates<'graph, G, GH, F>(
    nodes: &Nodes<'graph, G, GH, F>,
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

    let storage = nodes.base_graph.core_graph().lock();
    let nodes_locked = storage.nodes();
    let node_list = nodes.node_list();
    let keep = |vid| {
        storage.try_core_node(vid).is_some_and(|node| {
            nodes.base_graph.filter_node(node.as_ref()) && nodes.predicate.apply(&storage, vid)
        })
    };
    let chunks = get_node_ids_par_iter(&node_list, &nodes_locked, false, false, keep)
        .map(|(chunk, vids)| (chunk, vids.map(|vid| NodeView::new_internal(graph, vid))));

    let collector = ChunkCollector::default();
    encode_node_rows(graph, chunks, |_, chunk, _| Ok(collector.sink(chunk)))?;
    select_columns(
        &encoded,
        collector.into_batches(),
        &columns,
        convert_datetime,
    )
}
