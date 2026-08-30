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
            get_nodes_par_iter(g, node_list, g.node_list_trusted(), nodes_locked).map(|(chunk, nodes)| {
                (
                    chunk,
                    Either::Right(nodes.flat_map(move |node| node.out_edges())),
                )
            }),
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

pub(crate) fn encode_edge_tprop<G: GraphView, S: RecordBatchSink>(
    g: &G,
    sink_factory_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
) -> Result<(), GraphError> {
    let graph_locked = g.core_graph().lock();
    let edges_locked = graph_locked.edges();
    let nodes_locked = graph_locked.nodes();
    let node_list = g.node_list();
    // if we go layer by layer, we save a lot of disk space for graphs saved on disk
    for layer_id in active_layers(g) {
        let layer_filter = LayerIds::One(layer_id);
        run_encode_indexed(
            g,
            g.edge_meta().temporal_prop_mapper(),
            get_edges_par_iter(g, &edges_locked, &nodes_locked, &node_list, &layer_filter),
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
                    .flat_map(|e| e.explode())
                    .filter(|edge| edge.edge.layer() == Some(layer_id))
                    .map(|edge| {
                        let (export_src_id, export_dst_id) = edge.id();
                        ParquetTEdge {
                            edge,
                            export_src_vid: edge.src().node.0,
                            export_src_id,
                            export_dst_vid: edge.dst().node.0,
                            export_dst_id,
                            export_eid: edge.edge.pid(),
                            export_layer_id: edge.edge.layer().map(|l| l.0),
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

pub(crate) fn encode_edge_deletions<G: GraphView, S: RecordBatchSink>(
    g: &G,
    sink_factory_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
) -> Result<(), GraphError> {
    let graph_locked = g.core_graph().lock();
    let edges_locked = graph_locked.edges();
    let nodes_locked = graph_locked.nodes();
    let node_list = g.node_list();
    for layer_id in active_layers(g) {
        let layer_filter = LayerIds::One(layer_id);
        run_encode_indexed(
            g,
            g.edge_meta().temporal_prop_mapper(),
            get_edges_par_iter(g, &edges_locked, &nodes_locked, &node_list, &layer_filter),
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
    let node_list = g.node_list();
    for layer_id in active_layers(g) {
        let layer_filter = LayerIds::One(layer_id);
        run_encode_indexed(
            g,
            g.edge_meta().metadata_mapper(),
            get_edges_par_iter(g, &edges_locked, &nodes_locked, &node_list, &layer_filter),
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
