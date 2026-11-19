use super::*;
use crate::{
    db::{
        api::{
            state::ops::{FilterOps, GraphView},
            view::internal::List,
        },
        graph::edge::EdgeView,
    },
    errors::GraphError,
    parquet_encoder::{model::ParquetDelEdge, nodes::get_nodes_par_iter},
};
use arrow::datatypes::{DataType, Field};
use either::Either;
use model::ParquetCEdge;
use raphtory_api::core::entities::{LayerId, LayerIds};
use raphtory_core::entities::VID;
use raphtory_storage::graph::{
    edges::{edge_storage_ops::EdgeStorageOps, edges::EdgesStorageRef},
    nodes::nodes_ref::NodesStorageEntry,
};

fn get_edges_par_iter<'a, G: GraphView>(
    g: &'a G,
    edges_locked: &'a EdgesStorageRef,
    nodes_locked: &'a NodesStorageEntry,
    node_list: &'a List<VID>,
    node_list_trusted: bool,
    layer_filter: &'a LayerIds,
) -> impl ParallelIterator<Item = (usize, impl Iterator<Item = EdgeView<&'a G>> + 'a)> {
    let filtered = g.filtered();
    if !filtered {
        Either::Left(edges_locked
            .segmented_par_iter()
            .expect(
                "Internal Error: segmented_par_iter cannot be called from unlocked GraphStorage",
            )
            .map(move |(chunk, eids)| {
                (
                    chunk,
                    Either::Left(eids.filter_map(move |eid| {
                        let edge = g.core_edge(Either::Left(eid));
                        if !edge.as_ref().has_layer(layer_filter) {
                            return None;
                        }
                        if !filtered || g.filter_edge(edge.as_ref()) {
                            let edge_ref = edge.out_ref();
                            Some(EdgeView::new(g, edge_ref))
                        } else {
                            None
                        }
                    })),
                )
            }))
    } else {
        Either::Right(
            get_nodes_par_iter(g, node_list, node_list_trusted, nodes_locked).map(
                |(chunk, nodes)| {
                    (
                        chunk,
                        Either::Right(nodes.flat_map(move |node| node.out_edges())),
                    )
                },
            ),
        )
    }
}

fn active_layers<G: GraphView>(g: &G) -> Vec<LayerId> {
    match g.layer_ids() {
        LayerIds::None => vec![],
        LayerIds::All => g.edge_meta().layer_meta().ids().map(LayerId).collect(),
        LayerIds::One(id) => vec![*id],
        LayerIds::Multiple(ids) => ids.iter().collect(),
    }
}

/// The columns every edge update row starts with; one column per temporal property follows.
pub(crate) fn edge_tprop_fixed_fields(id_type: &DataType) -> Vec<Field> {
    vec![
        Field::new(TIME_COL, DataType::Int64, false),
        Field::new(SECONDARY_INDEX_COL, DataType::UInt64, true),
        Field::new(SRC_VID_COL, DataType::UInt64, false),
        Field::new(SRC_GID_COL, id_type.clone(), false),
        Field::new(DST_VID_COL, DataType::UInt64, false),
        Field::new(DST_GID_COL, id_type.clone(), false),
        Field::new(EDGE_COL_ID, DataType::UInt64, false),
        Field::new(LAYER_COL, DataType::Utf8, true),
        Field::new(LAYER_ID_COL, DataType::UInt64, true),
    ]
}

/// The schema of the batches [`encode_edge_rows`] produces for `g`.
pub(crate) fn edge_tprop_schema<G: GraphView>(g: &G) -> Result<SchemaRef, GraphError> {
    derive_schema(
        g.edge_meta().temporal_prop_mapper(),
        g.id_type(),
        edge_tprop_fixed_fields,
    )
}

/// The row of the exploded edge `edge`, with the properties in `props`, or the properties its
/// update set when `props` is `None`.
fn edge_row<G: GraphView>(
    edge: EdgeView<&G>,
    props: Option<Vec<(usize, Prop)>>,
) -> ParquetTEdge<'_, G> {
    let (export_src_id, export_dst_id) = edge.id();
    ParquetTEdge {
        edge,
        export_src_vid: edge.src().node.0,
        export_src_id,
        export_dst_vid: edge.dst().node.0,
        export_dst_id,
        export_eid: edge.edge.pid(),
        export_layer_id: edge.edge.layer().map(|l| l.0),
        props,
    }
}

/// Encodes the given edge update rows, chunk by chunk.
fn encode_edge_update_rows<'a, G, I, S>(
    g: &'a G,
    rows: impl ParallelIterator<Item = (usize, I)>,
    sink_factory_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
) -> Result<(), GraphError>
where
    G: GraphView,
    I: Iterator<Item = ParquetTEdge<'a, G>>,
    S: RecordBatchSink,
{
    run_encode_indexed(
        g,
        g.edge_meta().temporal_prop_mapper(),
        rows,
        sink_factory_fn,
        edge_tprop_fixed_fields,
        |rows, _g, decoder, sink| {
            for edge_rows in rows
                .chunks(ROW_GROUP_SIZE)
                .into_iter()
                .map(|chunk| chunk.collect_vec())
            {
                decoder.serialize(&edge_rows)?;
                if let Some(rb) = decoder.flush()? {
                    RecordBatchSink::send_batch(sink, rb)?;
                }
            }
            Ok(())
        },
    )
}

pub(crate) fn encode_edge_tprop<G: GraphView, S: RecordBatchSink>(
    g: &G,
    sink_factory_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
) -> Result<(), GraphError> {
    let graph_locked = g.core_graph().lock();
    let edges_locked = graph_locked.edges();
    let nodes_locked = graph_locked.nodes();
    let (node_list, node_list_trusted) = g.trusted_node_list();
    // if we go layer by layer, we save a lot of disk space for graphs saved on disk
    for layer_id in active_layers(g) {
        let layer_filter = LayerIds::One(layer_id);
        let rows = get_edges_par_iter(
            g,
            &edges_locked,
            &nodes_locked,
            &node_list,
            node_list_trusted,
            &layer_filter,
        )
        .map(move |(chunk, edges)| {
            let rows = edges
                .flat_map(|e| e.explode())
                .filter(move |edge| edge.edge.layer() == Some(layer_id))
                .map(|edge| edge_row(edge, None));
            (chunk, rows)
        });
        encode_edge_update_rows(g, rows, &sink_factory_fn)?;
    }
    Ok(())
}

/// Encodes one row per update of the given edge views, as seen through `g`, with the rows of each edge
/// together. With `latest_only`, only the updates that hold the latest value of a property are
/// encoded, each with just those properties.
pub(crate) fn encode_edge_rows<'a, G, I, S>(
    g: &'a G,
    edges: impl ParallelIterator<Item = (usize, I)>,
    latest_only: bool,
    sink_factory_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
) -> Result<(), GraphError>
where
    G: GraphView,
    I: Iterator<Item = EdgeView<&'a G>> + Send,
    S: RecordBatchSink,
{
    let rows = edges.map(move |(chunk, edges)| {
        let rows = edges.flat_map(move |edge| {
            if latest_only {
                Either::Left(edge.latest_rows().map(move |(t, layer_id, props)| {
                    let update = EdgeView::new(edge.graph, edge.edge.at(t).at_layer(layer_id));
                    edge_row(update, Some(props))
                }))
            } else {
                Either::Right(edge.explode().into_iter().map(|edge| edge_row(edge, None)))
            }
        });
        (chunk, rows)
    });
    encode_edge_update_rows(g, rows, sink_factory_fn)
}

pub(crate) fn encode_edge_deletions<G: GraphView, S: RecordBatchSink>(
    g: &G,
    sink_factory_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
) -> Result<(), GraphError> {
    let graph_locked = g.core_graph().lock();
    let edges_locked = graph_locked.edges();
    let nodes_locked = graph_locked.nodes();
    let (node_list, node_list_trusted) = g.trusted_node_list();
    for layer_id in active_layers(g) {
        let layer_filter = LayerIds::One(layer_id);
        run_encode_indexed(
            g,
            g.edge_meta().temporal_prop_mapper(),
            get_edges_par_iter(
                g,
                &edges_locked,
                &nodes_locked,
                &node_list,
                node_list_trusted,
                &layer_filter,
            ),
            &sink_factory_fn,
            |id_type| {
                vec![
                    Field::new(TIME_COL, DataType::Int64, false),
                    Field::new(SECONDARY_INDEX_COL, DataType::UInt64, true),
                    Field::new(SRC_VID_COL, DataType::UInt64, false),
                    Field::new(SRC_GID_COL, id_type.clone(), false),
                    Field::new(DST_VID_COL, DataType::UInt64, false),
                    Field::new(DST_GID_COL, id_type.clone(), false),
                    Field::new(EDGE_COL_ID, DataType::UInt64, false),
                    Field::new(LAYER_COL, DataType::Utf8, true),
                    Field::new(LAYER_ID_COL, DataType::UInt64, true),
                ]
            },
            |edges, _g, decoder, sink| {
                for edge_rows in edges
                    .into_iter()
                    .flat_map(|e| e.explode_layers())
                    .filter(|edge| edge.edge.layer() == Some(layer_id))
                    .flat_map(|edge| {
                        edge.deletions().into_iter().map(move |deletion| {
                            let (export_src_id, export_dst_id) = edge.id();
                            ParquetDelEdge {
                                edge,
                                del: deletion,
                                export_src_vid: edge.src().node.0,
                                export_src_id,
                                export_dst_vid: edge.dst().node.0,
                                export_dst_id,
                                export_eid: edge.edge.pid().0,
                                export_layer_id: edge.edge.layer().map(|l| l.0),
                            }
                        })
                    })
                    .chunks(ROW_GROUP_SIZE)
                    .into_iter()
                    .map(|chunk| chunk.collect_vec())
                {
                    decoder.serialize(&edge_rows)?;
                    if let Some(rb) = decoder.flush()? {
                        RecordBatchSink::send_batch(sink, rb)?;
                    }
                }
                Ok(())
            },
        )?;
    }
    Ok(())
}

pub(crate) fn encode_edge_cprop<G: GraphView, S: RecordBatchSink>(
    g: &G,
    sink_factory_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
) -> Result<(), GraphError> {
    let graph_locked = g.core_graph().lock();
    let edges_locked = graph_locked.edges();
    let nodes_locked = graph_locked.nodes();
    let (node_list, node_list_trusted) = g.trusted_node_list();
    for layer_id in active_layers(g) {
        let layer_filter = LayerIds::One(layer_id);
        run_encode_indexed(
            g,
            g.edge_meta().metadata_mapper(),
            get_edges_par_iter(
                g,
                &edges_locked,
                &nodes_locked,
                &node_list,
                node_list_trusted,
                &layer_filter,
            ),
            &sink_factory_fn,
            |id_type| {
                vec![
                    Field::new(SRC_VID_COL, DataType::UInt64, false),
                    Field::new(SRC_GID_COL, id_type.clone(), false),
                    Field::new(DST_VID_COL, DataType::UInt64, false),
                    Field::new(DST_GID_COL, id_type.clone(), false),
                    Field::new(EDGE_COL_ID, DataType::UInt64, false),
                    Field::new(LAYER_COL, DataType::Utf8, true),
                ]
            },
            |edges, _g, decoder, sink| {
                for edge_rows in edges
                    .into_iter()
                    .flat_map(|e| e.explode_layers().into_iter())
                    .filter(|edge| edge.edge.layer() == Some(layer_id))
                    .map(|edge| {
                        let (export_src_id, export_dst_id) = edge.id();
                        ParquetCEdge {
                            edge,
                            export_src_vid: edge.src().node.0,
                            export_src_id,
                            export_dst_vid: edge.dst().node.0,
                            export_dst_id,
                            export_eid: edge.edge.pid().0,
                        }
                    })
                    .chunks(ROW_GROUP_SIZE)
                    .into_iter()
                    .map(|chunk| chunk.collect_vec())
                {
                    decoder.serialize(&edge_rows)?;
                    if let Some(rb) = decoder.flush()? {
                        RecordBatchSink::send_batch(sink, rb)?;
                    }
                }
                Ok(())
            },
        )?;
    }
    Ok(())
}
