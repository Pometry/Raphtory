use super::*;
use crate::{
    core::utils::iter::GenLockedIter,
    db::{api::state::ops::FilterOps, graph::edge::EdgeView},
    errors::GraphError,
    serialise::parquet::model::ParquetDelEdge,
};
use arrow::datatypes::{DataType, Field};
use either::Either;
use model::ParquetCEdge;
use raphtory_api::{
    core::{entities::EID, storage::timeindex::TimeIndexOps},
    iter::IntoDynBoxed,
};
use raphtory_storage::{core_ops::CoreGraphOps, graph::edges::edge_storage_ops::EdgeStorageOps};
use std::path::Path;

fn get_edges_par_iter<'a, G: GraphView>(
    g: &'a G,
    locked: &'a GraphStorage,
) -> impl ParallelIterator<Item = (usize, impl Iterator<Item = EID> + 'a)> {
    let filtered = g.filtered();

    locked
        .edges()
        .segmented_par_iter()
        .expect("Internal Error: segmented_par_iter cannot be called from unlocked GraphStorage")
        .map(move |(chunk, eids)| {
            (
                chunk,
                eids.filter(move |eid| {
                    !filtered || g.filter_edge(locked.edge_entry(*eid).as_ref())
                }),
            )
        })
}

pub(crate) fn encode_edge_tprop<G: GraphView>(
    g: &G,
    path: impl AsRef<Path>,
) -> Result<(), GraphError> {
    let locked = g.core_graph().lock();
    let edges_par_iter = get_edges_par_iter(g, &locked);
    run_encode_indexed(
        g,
        g.edge_meta().temporal_prop_mapper(),
        edges_par_iter,
        path,
        EDGES_T_PATH,
        |_| {
            vec![
                Field::new(TIME_COL, DataType::Int64, false),
                Field::new(SECONDARY_INDEX_COL, DataType::UInt64, true),
                Field::new(SRC_COL_ID, DataType::UInt64, false),
                Field::new(DST_COL_ID, DataType::UInt64, false),
                Field::new(EDGE_COL_ID, DataType::UInt64, false),
                Field::new(LAYER_COL, DataType::Utf8, true),
                Field::new(LAYER_ID_COL, DataType::UInt64, true),
            ]
        },
        |edges, g, decoder, writer| {
            for edge_rows in edges
                .into_iter()
                .flat_map(|eid| {
                    let edge_ref = g.core_edge(eid).out_ref();
                    EdgeView::new(g, edge_ref).explode()
                })
                .map(|edge| ParquetTEdge {
                    edge,
                    export_src_vid: edge.src().node.0,
                    export_dst_vid: edge.dst().node.0,
                    export_eid: edge.edge.pid(),
                    export_layer_id: edge.edge.layer(),
                })
                .chunks(ROW_GROUP_SIZE)
                .into_iter()
                .map(|chunk| chunk.collect_vec())
            {
                decoder.serialize(&edge_rows)?;
                if let Some(rb) = decoder.flush()? {
                    writer.write(&rb)?;
                    writer.flush()?;
                }
            }
            Ok(())
        },
    )
}

pub(crate) fn encode_edge_deletions<G: GraphView>(
    g: &G,
    path: impl AsRef<Path>,
) -> Result<(), GraphError> {
    let locked = g.core_graph().lock();
    let edges_par_iter = get_edges_par_iter(g, &locked);
    run_encode_indexed(
        g,
        g.edge_meta().temporal_prop_mapper(),
        edges_par_iter,
        path,
        EDGES_D_PATH,
        |_| {
            vec![
                Field::new(TIME_COL, DataType::Int64, false),
                Field::new(SECONDARY_INDEX_COL, DataType::UInt64, true),
                Field::new(SRC_COL_ID, DataType::UInt64, false),
                Field::new(DST_COL_ID, DataType::UInt64, false),
                Field::new(EDGE_COL_ID, DataType::UInt64, false),
                Field::new(LAYER_COL, DataType::Utf8, true),
                Field::new(LAYER_ID_COL, DataType::UInt64, true),
            ]
        },
        |edges, g, decoder, writer| {
            let g = g.core_graph().lock();
            let g = &g;
            let g_edges = g.edges();
            let layers = g
                .unique_layers()
                .map(|s| s.to_string().to_owned())
                .collect::<Vec<_>>();
            let layers = &layers;

            for edge_rows in edges
                .into_iter()
                .flat_map(|eid| {
                    g.unfiltered_layer_ids().flat_map(move |layer_id| {
                        let edge = g_edges.edge(eid);
                        let edge_ref = edge.out_ref();
                        GenLockedIter::from(edge, |edge| {
                            edge.deletions(layer_id).iter().into_dyn_boxed()
                        })
                        .map(move |deletions| {
                            let edge = EdgeView::new(g, edge_ref);
                            ParquetDelEdge {
                                edge,
                                del: deletions,
                                export_src_vid: edge.src().node.0,
                                export_dst_vid: edge.dst().node.0,
                                export_eid: edge.edge.pid().0,
                                export_layer_id: layer_id,
                                export_layer_name: &layers[layer_id - 1],
                            }
                        })
                    })
                })
                .chunks(ROW_GROUP_SIZE)
                .into_iter()
                .map(|chunk| chunk.collect_vec())
            {
                decoder.serialize(&edge_rows)?;
                if let Some(rb) = decoder.flush()? {
                    writer.write(&rb)?;
                    writer.flush()?;
                }
            }
            Ok(())
        },
    )
}

pub(crate) fn encode_edge_cprop<G: GraphView>(
    g: &G,
    path: impl AsRef<Path>,
) -> Result<(), GraphError> {
    let locked = g.core_graph().lock();
    let edges_par_iter = get_edges_par_iter(g, &locked);
    run_encode_indexed(
        g,
        g.edge_meta().metadata_mapper(),
        edges_par_iter,
        path,
        EDGES_C_PATH,
        |_| {
            vec![
                Field::new(SRC_COL_ID, DataType::UInt64, false),
                Field::new(DST_COL_ID, DataType::UInt64, false),
                Field::new(EDGE_COL_ID, DataType::UInt64, false),
                Field::new(LAYER_COL, DataType::Utf8, true),
            ]
        },
        |edges, g, decoder, writer| {
            for edge_rows in edges
                .into_iter()
                .flat_map(|eid| {
                    let edge_ref = g.core_edge(eid).out_ref();
                    EdgeView::new(g, edge_ref).explode_layers().into_iter()
                })
                .map(|edge| ParquetCEdge {
                    edge,
                    export_src_vid: edge.src().node.0,
                    export_dst_vid: edge.dst().node.0,
                    export_eid: edge.edge.pid().0,
                })
                .chunks(ROW_GROUP_SIZE)
                .into_iter()
                .map(|chunk| chunk.collect_vec())
            {
                decoder.serialize(&edge_rows)?;
                if let Some(rb) = decoder.flush()? {
                    writer.write(&rb)?;
                    writer.flush()?;
                }
            }
            Ok(())
        },
    )
}
