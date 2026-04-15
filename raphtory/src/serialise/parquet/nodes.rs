use crate::{
    core::utils::iter::GenLockedIter,
    db::{
        api::state::ops::{FilterOps, GraphView},
        graph::node::NodeView,
    },
    errors::GraphError,
    prelude::NodeViewOps,
    serialise::parquet::{
        model::{ParquetCNode, ParquetTNode},
        run_encode_indexed, RecordBatchSink, NODE_ID_COL, NODE_VID_COL, ROW_GROUP_SIZE,
        SECONDARY_INDEX_COL, TIME_COL, TYPE_COL, TYPE_ID_COL,
    },
};
use arrow::datatypes::{DataType, Field, SchemaRef};
use itertools::Itertools;
use raphtory_api::iter::IntoDynBoxed;
use raphtory_storage::graph::nodes::nodes_ref::NodesStorageEntry;
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
        |_| {
            vec![
                Field::new(NODE_VID_COL, DataType::UInt64, false),
                Field::new(TIME_COL, DataType::Int64, false),
                Field::new(SECONDARY_INDEX_COL, DataType::UInt64, true),
            ]
        },
        |nodes, g, decoder, sink| {
            let nodes = nodes.collect::<Vec<_>>();
            let nodes = nodes.into_iter();

            let cols = g.node_meta().temporal_prop_mapper().all_keys();
            let cols = &cols;
            for node_rows in nodes
                .flat_map(move |node| {
                    GenLockedIter::from(node, |node| {
                        node.rows()
                            .map(|(t, props)| ParquetTNode {
                                export_vid: node.node.0,
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
            for node_rows in nodes
                .map(move |node| ParquetCNode {
                    node,
                    export_vid: node.node.0,
                    export_node_type_id: node.node_type_id(),
                })
                .chunks(ROW_GROUP_SIZE)
                .into_iter()
                .map(|chunk| chunk.collect_vec())
            // scope for the decoder
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
