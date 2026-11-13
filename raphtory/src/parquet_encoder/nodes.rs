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
        derive_schema,
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

/// Splits `node_list` into chunks of node ids for parallel encoding. Chunk ids follow the order of
/// the list, so output gathered per chunk can be put back in order.
///
/// A node is kept when `keep` returns true. `keep` is skipped for lists already known to hold only
/// kept nodes: `all_trusted` covers a list of every node, `list_trusted` an explicit list.
pub(crate) fn get_node_ids_par_iter<'a>(
    node_list: &'a List<VID>,
    nodes_locked: &'a NodesStorageEntry,
    all_trusted: bool,
    list_trusted: bool,
    keep: impl Fn(VID) -> bool + Send + Sync + Copy + 'a,
) -> impl ParallelIterator<Item = (usize, impl Iterator<Item = VID> + 'a)> {
    match node_list {
        // TODO: use the node type index instead of scanning every row group
        List::All
        | List::NodeTypeIdx { .. }
        | List::List {
            elems: Index::Full(_),
        } => Either::Left(
            nodes_locked
                .row_groups_par_iter()
                .map(move |(chunk, vids)| {
                    (
                        chunk,
                        Either::Left(vids.filter(move |&vid| all_trusted || keep(vid))),
                    )
                }),
        ),
        List::List { elems } => {
            let chunk_size = (elems.len() / rayon::current_num_threads().max(1)).max(1);
            let keys = match elems {
                Index::Partial(index) => Either::Left(index.par_iter()),
                Index::Sorted { keys, .. } => Either::Right(keys.par_iter()),
                Index::Full(_) => unreachable!("matched by the first arm"),
            };
            let iter = keys
                .chunks(chunk_size)
                .enumerate()
                .map(move |(c_id, chunk)| {
                    (
                        c_id,
                        Either::Right(
                            chunk
                                .into_iter()
                                .copied()
                                .filter(move |&vid| list_trusted || keep(vid)),
                        ),
                    )
                });
            Either::Right(iter)
        }
    }
}

pub(crate) fn get_nodes_par_iter<'a, G: GraphView>(
    g: &'a G,
    node_list: &'a List<VID>,
    list_trusted: bool,
    nodes_locked: &'a NodesStorageEntry,
) -> impl ParallelIterator<Item = (usize, impl Iterator<Item = NodeView<'a, &'a G>> + 'a)> {
    let keep = move |vid| g.filter_node(g.core_node(vid).as_ref());
    get_node_ids_par_iter(node_list, nodes_locked, !g.filtered(), list_trusted, keep)
        .map(move |(chunk, vids)| (chunk, vids.map(move |vid| NodeView::new_internal(g, vid))))
}

/// The columns every node update row starts with; one column per temporal property follows.
pub(crate) fn node_tprop_fixed_fields(id_type: &DataType) -> Vec<Field> {
    vec![
        Field::new(NODE_GID_COL, id_type.clone(), false),
        Field::new(NODE_VID_COL, DataType::UInt64, false),
        Field::new(TYPE_COL, DataType::Utf8, true),
        Field::new(TIME_COL, DataType::Int64, false),
        Field::new(SECONDARY_INDEX_COL, DataType::UInt64, true),
        Field::new(LAYER_COL, DataType::Utf8, true),
        Field::new(LAYER_ID_COL, DataType::UInt64, false),
    ]
}

/// The schema of the batches [`encode_node_rows`] produces for `g`.
pub(crate) fn node_tprop_schema<G: GraphView>(g: &G) -> Result<SchemaRef, GraphError> {
    derive_schema(
        g.node_meta().temporal_prop_mapper(),
        g.id_type(),
        node_tprop_fixed_fields,
    )
}

pub(crate) fn encode_nodes_tprop<G: GraphView, S: RecordBatchSink>(
    g: &G,
    sink_factory_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
) -> Result<(), GraphError> {
    let graph_locked = g.core_graph().lock();
    let nodes_locked = graph_locked.nodes();
    let (node_list, list_trusted) = g.trusted_node_list();
    encode_node_rows(
        g,
        get_nodes_par_iter(g, &node_list, list_trusted, &nodes_locked),
        sink_factory_fn,
    )
}

/// Encodes one row per update of the given nodes, as seen through `g`.
pub(crate) fn encode_node_rows<'a, G, I, S>(
    g: &'a G,
    nodes: impl ParallelIterator<Item = (usize, I)>,
    sink_factory_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
) -> Result<(), GraphError>
where
    G: GraphView,
    I: Iterator<Item = NodeView<'a, &'a G>>,
    S: RecordBatchSink,
{
    run_encode_indexed(
        g,
        g.node_meta().temporal_prop_mapper(),
        nodes,
        sink_factory_fn,
        node_tprop_fixed_fields,
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
    let (node_list, list_trusted) = g.trusted_node_list();
    run_encode_indexed(
        g,
        g.node_meta().metadata_mapper(),
        get_nodes_par_iter(g, &node_list, list_trusted, &nodes_locked),
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
