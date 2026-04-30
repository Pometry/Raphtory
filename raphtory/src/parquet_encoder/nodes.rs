use crate::{
    core::utils::iter::GenLockedIter,
    db::{
        api::state::ops::{FilterOps, GraphView},
        graph::node::NodeView,
    },
    errors::GraphError,
    parquet_encoder::{
        model::{ParquetCNode, ParquetTNode},
        run_encode_indexed, RecordBatchSink, LAYER_COL, NODE_ID_COL, NODE_VID_COL, ROW_GROUP_SIZE,
        SECONDARY_INDEX_COL, TIME_COL, TYPE_COL, TYPE_ID_COL,
    },
    prelude::{NodeViewOps, Prop},
};
use arrow::datatypes::{DataType, Field, SchemaRef};
use itertools::Itertools;
use raphtory_api::{
    core::{
        entities::{properties::meta::STATIC_GRAPH_LAYER_ID, LayerId, LayerIds},
        storage::arc_str::ArcStr,
    },
    iter::IntoDynBoxed,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::nodes::{node_storage_ops::NodeStorageOps, nodes_ref::NodesStorageEntry},
};
use rayon::iter::ParallelIterator;

fn get_nodes_par_iter<'a, G: GraphView>(
    g: &'a G,
    nodes_locked: &'a NodesStorageEntry,
) -> impl ParallelIterator<Item = (usize, impl Iterator<Item = NodeView<'a, &'a G>> + 'a)> {
    let filtered = g.filtered();

    nodes_locked
        .row_groups_par_iter()
        .map(move |(chunk, vids)| {
            (
                chunk,
                vids.filter_map(move |vid| {
                    let node = g.core_node(vid);
                    if !filtered || g.filter_node(node.as_ref()) {
                        Some(NodeView::new_internal(g, vid))
                    } else {
                        None
                    }
                }),
            )
        })
}

pub(crate) fn encode_nodes_tprop<G: GraphView, S: RecordBatchSink>(
    g: &G,
    sink_factory_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
) -> Result<(), GraphError> {
    let graph_locked = g.core_graph().lock();
    let nodes_locked = graph_locked.nodes();
    run_encode_indexed(
        g,
        g.node_meta().temporal_prop_mapper(),
        get_nodes_par_iter(g, &nodes_locked),
        sink_factory_fn,
        |id_type| {
            vec![
                Field::new(NODE_ID_COL, id_type.clone(), false),
                Field::new(NODE_VID_COL, DataType::UInt64, false),
                Field::new(TYPE_COL, DataType::Utf8, true),
                Field::new(TIME_COL, DataType::Int64, false),
                Field::new(SECONDARY_INDEX_COL, DataType::UInt64, true),
                Field::new(LAYER_COL, DataType::Utf8, true),
            ]
        },
        |nodes, g, decoder, sink| {
            let nodes = nodes.collect::<Vec<_>>();
            let nodes = nodes.into_iter();

            let cols = g.node_meta().temporal_prop_mapper().all_keys();
            let cols = &cols;
            let layer_meta = g.node_meta().layer_meta();
            for node_rows in nodes
                .flat_map(move |node| {
                    GenLockedIter::from(node, |node| {
                        node.rows()
                            .map(|(t, layer_id, props)| ParquetTNode {
                                export_id: node.id(),
                                export_vid: node.node.0,
                                export_node_type: node.node_type(),
                                // emit null for STATIC_GRAPH_LAYER (id 0) so
                                // the loader's null-row fallback restores it
                                export_layer: (layer_id.0 != 0)
                                    .then(|| layer_meta.get_name(layer_id.0)),
                                cols,
                                t,
                                props,
                            })
                            .into_dyn_boxed()
                    })
                })
                .chunks(ROW_GROUP_SIZE)
                .into_iter()
                .map(|chunk| chunk.collect_vec())
            {
                decoder.serialize(&node_rows)?;
                if let Some(rb) = decoder.flush()? {
                    RecordBatchSink::send_batch(sink, rb)?;
                }
            }
            Ok(())
        },
    )
}

pub(crate) fn encode_nodes_cprop<G: GraphView, S: RecordBatchSink>(
    g: &G,
    sink_factory_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
) -> Result<(), GraphError> {
    let graph_locked = g.core_graph().lock();
    let nodes_locked = graph_locked.nodes();
    run_encode_indexed(
        g,
        g.node_meta().metadata_mapper(),
        get_nodes_par_iter(g, &nodes_locked),
        sink_factory_fn,
        |id_type| {
            vec![
                Field::new(NODE_ID_COL, id_type.clone(), false),
                Field::new(NODE_VID_COL, DataType::UInt64, false),
                Field::new(TYPE_COL, DataType::Utf8, true),
                Field::new(TYPE_ID_COL, DataType::UInt64, true),
            ]
        },
        |nodes, _g, decoder, sink| {
            // ONE row per node. Node metadata is layer-less in the public API
            // (NodeView::add_metadata always writes to STATIC_GRAPH_LAYER, and
            // the storage's gid()/node_type_id() always read from layer 0), so
            // there is nothing layer-shaped to emit at the c_node level. The
            // per-layer node counters are restored by the t_node pass — every
            // explicit `add_node(.., Some(layer))` produces a temporal row at
            // that layer, and the loader's `add_props` path increments the
            // layer counter for first-seen nodes.
            for node_rows in nodes
                .map(move |node| ParquetCNode {
                    node,
                    export_vid: node.node.0,
                    export_node_type_id: node.node_type_id(),
                })
                .chunks(ROW_GROUP_SIZE)
                .into_iter()
                .map(|chunk| chunk.collect_vec())
            {
                decoder.serialize(&node_rows)?;

                if let Some(rb) = decoder.flush()? {
                    RecordBatchSink::send_batch(sink, rb)?;
                }
            }

            Ok(())
        },
    )
}
