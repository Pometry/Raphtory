use crate::{
    core::utils::iter::GenLockedIter,
    db::{
        api::state::ops::{FilterOps, GraphView},
        graph::{edge::EdgeView, node::NodeView},
    },
    errors::GraphError,
    prelude::{GraphViewOps, NodeViewOps},
    serialise::parquet::{
        create_arrow_writer_sink,
        model::{ParquetCNode, ParquetTNode},
        run_encode_indexed, RecordBatchSink, NODES_C_PATH, NODES_T_PATH, NODE_ID_COL, NODE_VID_COL,
        ROW_GROUP_SIZE, SECONDARY_INDEX_COL, TIME_COL, TYPE_COL, TYPE_ID_COL,
    },
};
use arrow::datatypes::{DataType, Field};
use itertools::Itertools;
use raphtory_api::{core::entities::edges::edge_ref::Dir, iter::IntoDynBoxed};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{
        edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage,
        nodes::nodes_ref::NodesStorageEntry,
    },
};
use rayon::iter::ParallelIterator;
use std::path::Path;

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

pub(crate) fn encode_nodes_tprop<G: GraphView>(
    g: &G,
    path: impl AsRef<Path>,
) -> Result<(), GraphError> {
    let graph_locked = g.core_graph().lock();
    let nodes_locked = graph_locked.nodes();
    let root_dir = path.as_ref().join(NODES_T_PATH);
    run_encode_indexed(
        g,
        g.node_meta().temporal_prop_mapper(),
        get_nodes_par_iter(g, &nodes_locked),
        |schema, chunk, num_digits| {
            create_arrow_writer_sink(&root_dir, schema.clone(), chunk, num_digits)
        },
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
                    RecordBatchSink::write_batch(sink, &rb)?;
                    RecordBatchSink::flush(sink)?;
                }
            }
            Ok(())
        },
    )
}

pub(crate) fn encode_nodes_cprop<G: GraphView>(
    g: &G,
    path: impl AsRef<Path>,
) -> Result<(), GraphError> {
    let graph_locked = g.core_graph().lock();
    let nodes_locked = graph_locked.nodes();
    let root_dir = path.as_ref().join(NODES_C_PATH);
    run_encode_indexed(
        g,
        g.node_meta().metadata_mapper(),
        get_nodes_par_iter(g, &nodes_locked),
        |schema, chunk, num_digits| {
            create_arrow_writer_sink(&root_dir, schema.clone(), chunk, num_digits)
        },
        |id_type| {
            vec![
                Field::new(NODE_ID_COL, id_type.clone(), false),
                Field::new(NODE_VID_COL, DataType::UInt64, false),
                Field::new(TYPE_COL, DataType::Utf8, true),
                Field::new(TYPE_ID_COL, DataType::UInt64, true),
            ]
        },
        |nodes, g, decoder, sink| {
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
                    RecordBatchSink::write_batch(sink, &rb)?;
                    RecordBatchSink::flush(sink)?;
                }
            }

            Ok(())
        },
    )
}
