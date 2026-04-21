use super::*;
use crate::{
    db::{
        api::state::ops::{FilterOps, GraphView},
        graph::edge::EdgeView,
    },
    errors::GraphError,
    parquet_encoder::model::ParquetDelEdge,
};
use arrow::datatypes::{DataType, Field};
use model::ParquetCEdge;
use raphtory_api::core::entities::{LayerId, LayerIds};
use raphtory_storage::graph::edges::{edge_storage_ops::EdgeStorageOps, edges::EdgesStorageRef};

fn get_edges_par_iter<'a, G: GraphView>(
    g: &'a G,
    edges_locked: &'a EdgesStorageRef,
    layer_filter: &'a LayerIds,
) -> impl ParallelIterator<Item = (usize, impl Iterator<Item = EdgeView<&'a G>> + 'a)> {
    let filtered = g.filtered();

    edges_locked
        .segmented_par_iter()
        .expect("Internal Error: segmented_par_iter cannot be called from unlocked GraphStorage")
        .map(move |(chunk, eids)| {
            (
                chunk,
                eids.filter_map(move |eid| {
                    let edge = g.core_edge(eid);
                    if !edge.as_ref().has_layer(layer_filter) {
                        return None;
                    }
                    if !filtered || g.filter_edge(edge.as_ref()) {
                        let edge_ref = edge.out_ref();
                        Some(EdgeView::new(g, edge_ref))
                    } else {
                        None
                    }
                }),
            )
        })
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
    // if we go layer by layer, we save a lot of disk space for graphs saved on disk
    for layer_id in active_layers(g) {
        let layer_filter = LayerIds::One(layer_id);
        run_encode_indexed(
            g,
            g.edge_meta().temporal_prop_mapper(),
            get_edges_par_iter(g, &edges_locked, &layer_filter),
            &sink_factory_fn,
            |id_type| {
                vec![
                    Field::new(TIME_COL, DataType::Int64, false),
                    Field::new(SECONDARY_INDEX_COL, DataType::UInt64, true),
                    Field::new(SRC_COL_VID, DataType::UInt64, false),
                    Field::new(SRC_COL_ID, id_type.clone(), false),
                    Field::new(DST_COL_VID, DataType::UInt64, false),
                    Field::new(DST_COL_ID, id_type.clone(), false),
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
    for layer_id in active_layers(g) {
        let layer_filter = LayerIds::One(layer_id);
        run_encode_indexed(
            g,
            g.edge_meta().temporal_prop_mapper(),
            get_edges_par_iter(g, &edges_locked, &layer_filter),
            &sink_factory_fn,
            |id_type| {
                vec![
                    Field::new(TIME_COL, DataType::Int64, false),
                    Field::new(SECONDARY_INDEX_COL, DataType::UInt64, true),
                    Field::new(SRC_COL_VID, DataType::UInt64, false),
                    Field::new(SRC_COL_ID, id_type.clone(), false),
                    Field::new(DST_COL_VID, DataType::UInt64, false),
                    Field::new(DST_COL_ID, id_type.clone(), false),
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
    for layer_id in active_layers(g) {
        let layer_filter = LayerIds::One(layer_id);
        run_encode_indexed(
            g,
            g.edge_meta().metadata_mapper(),
            get_edges_par_iter(g, &edges_locked, &layer_filter),
            &sink_factory_fn,
            |id_type| {
                vec![
                    Field::new(SRC_COL_VID, DataType::UInt64, false),
                    Field::new(SRC_COL_ID, id_type.clone(), false),
                    Field::new(DST_COL_VID, DataType::UInt64, false),
                    Field::new(DST_COL_ID, id_type.clone(), false),
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
