use crate::{
    core::utils::iter::GenLockedIter,
    db::{
        api::{
            state::{
                ops::{FilterOps, GraphView},
                Index,
            },
            view::internal::List,
        },
        graph::node::NodeView,
    },
    errors::GraphError,
    parquet_encoder::{
        model::{ParquetCNode, ParquetTNode},
        run_encode_indexed, RecordBatchSink, LAYER_COL, LAYER_ID_COL, NODE_GID_COL, NODE_VID_COL,
        ROW_GROUP_SIZE, SECONDARY_INDEX_COL, TIME_COL, TYPE_COL, TYPE_ID_COL,
    },
    prelude::NodeViewOps,
};
use arrow::datatypes::{DataType, Field, SchemaRef};
use either::Either;
use itertools::Itertools;
use raphtory_api::{core::entities::properties::meta::STATIC_GRAPH_LAYER_ID, iter::IntoDynBoxed};
use raphtory_core::entities::VID;
use raphtory_storage::graph::nodes::nodes_ref::NodesStorageEntry;
use rayon::prelude::*;
use storage::api::nodes::NodeEntryOps;

pub(crate) fn get_nodes_par_iter<'a, G: GraphView>(
    g: &'a G,
    node_list: &'a List<VID>,
    nodes_locked: &'a NodesStorageEntry,
) -> impl ParallelIterator<Item = (usize, impl Iterator<Item = NodeView<'a, &'a G>> + 'a)> {
    let filtered = g.filtered();

    match node_list {
        List::All
        | List::List {
            elems: Index::Full(_),
        } => Either::Left(
            nodes_locked
                .row_groups_par_iter()
                .map(move |(chunk, vids)| {
                    (
                        chunk,
                        Either::Left(vids.filter_map(move |vid| {
                            let node = g.core_node(vid);
                            if !filtered || g.filter_node(node.as_ref()) {
                                Some(NodeView::new_internal(g, vid))
                            } else {
                                None
                            }
                        })),
                    )
                }),
        ),
        List::List {
            elems: Index::Partial(index),
        } => {
            let chunk_size = (index.len() / rayon::current_num_threads().max(1)).max(1);
            let list_trusted = g.node_list_trusted();
            let iter = index
                .par_iter()
                .chunks(chunk_size)
                .enumerate()
                .map(move |(c_id, chunk)| {
                    (
                        c_id,
                        Either::Right(chunk.into_iter().filter_map(move |vid| {
                            let node = g.core_node(*vid);
                            if list_trusted || g.filter_node(node.as_ref()) {
                                Some(NodeView::new_internal(g, *vid))
                            } else {
                                None
                            }
                        })),
                    )
                });
            Either::Right(iter)
        }
    }
}

pub(crate) fn encode_nodes_tprop<G: GraphView, S: RecordBatchSink>(
    g: &G,
    sink_factory_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
) -> Result<(), GraphError> {
    let graph_locked = g.core_graph().lock();
    let nodes_locked = graph_locked.nodes();
    let node_list = g.node_list();
    run_encode_indexed(
        g,
        g.node_meta().temporal_prop_mapper(),
        get_nodes_par_iter(g, &node_list, &nodes_locked),
        sink_factory_fn,
        |id_type| {
            vec![
                Field::new(NODE_GID_COL, id_type.clone(), false),
                Field::new(NODE_VID_COL, DataType::UInt64, false),
                Field::new(TYPE_COL, DataType::Utf8, true),
                Field::new(TIME_COL, DataType::Int64, false),
                Field::new(SECONDARY_INDEX_COL, DataType::UInt64, true),
                Field::new(LAYER_COL, DataType::Utf8, true),
                Field::new(LAYER_ID_COL, DataType::UInt64, false),
            ]
        },
        |nodes, g, decoder, sink| {
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
                                // null for STATIC_GRAPH_LAYER
                                export_layer: (layer_id != STATIC_GRAPH_LAYER_ID)
                                    .then(|| layer_meta.get_name(layer_id.0)),
                                export_layer_id: layer_id.0,
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
    let node_list = g.node_list();
    run_encode_indexed(
        g,
        g.node_meta().metadata_mapper(),
        get_nodes_par_iter(g, &node_list, &nodes_locked),
        sink_factory_fn,
        |id_type| {
            vec![
                Field::new(NODE_GID_COL, id_type.clone(), false),
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
