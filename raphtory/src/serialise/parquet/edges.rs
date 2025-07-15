use super::*;
use crate::{
    core::utils::iter::GenLockedIter, db::graph::edge::EdgeView, errors::GraphError,
    serialise::parquet::model::ParquetDelEdge,
};
use arrow_schema::{DataType, Field};
use model::ParquetCEdge;
use raphtory_api::{core::storage::timeindex::TimeIndexOps, iter::IntoDynBoxed};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage},
};
use std::path::Path;

pub(crate) fn encode_edge_tprop(
    g: &GraphStorage,
    path: impl AsRef<Path>,
) -> Result<(), GraphError> {
    run_encode_indexed(
        g,
        g.edge_meta().temporal_prop_meta(),
        g.edges().segmented_par_iter(),
        path,
        EDGES_T_PATH,
        |id_type| {
            vec![
                Field::new(TIME_COL, DataType::Int64, false),
                Field::new(SRC_COL, id_type.clone(), false),
                Field::new(DST_COL, id_type.clone(), false),
                Field::new(LAYER_COL, DataType::Utf8, true),
            ]
        },
        |edges, g, decoder, writer| {
            let row_group_size = 100_000;
            let edges = edges.collect::<Vec<_>>();

            for edge_rows in edges
                .into_iter()
                .flat_map(|eid| {
                    let edge_ref = g.core_edge(eid).out_ref();
                    EdgeView::new(g, edge_ref).explode()
                })
                .map(ParquetTEdge)
                .chunks(row_group_size)
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

pub(crate) fn encode_edge_deletions(
    g: &GraphStorage,
    path: impl AsRef<Path>,
) -> Result<(), GraphError> {
    run_encode_indexed(
        g,
        g.edge_meta().temporal_prop_meta(),
        g.edges().segmented_par_iter(),
        path,
        EDGES_D_PATH,
        |id_type| {
            vec![
                Field::new(TIME_COL, DataType::Int64, false),
                Field::new(SRC_COL, id_type.clone(), false),
                Field::new(DST_COL, id_type.clone(), false),
                Field::new(LAYER_COL, DataType::Utf8, true),
            ]
        },
        |edges, g, decoder, writer| {
            let row_group_size = 100_000;
            let g = g.lock();
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
                        .map(move |deletions| ParquetDelEdge {
                            del: deletions,
                            layer: &layers[layer_id - 1],
                            edge: EdgeView::new(g, edge_ref),
                        })
                    })
                })
                .chunks(row_group_size)
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

pub(crate) fn encode_edge_cprop(
    g: &GraphStorage,
    path: impl AsRef<Path>,
) -> Result<(), GraphError> {
    run_encode_indexed(
        g,
        g.edge_meta().const_prop_meta(),
        g.edges().segmented_par_iter(),
        path,
        EDGES_C_PATH,
        |id_type| {
            vec![
                Field::new(SRC_COL, id_type.clone(), false),
                Field::new(DST_COL, id_type.clone(), false),
                Field::new(LAYER_COL, DataType::Utf8, true),
            ]
        },
        |edges, g, decoder, writer| {
            let row_group_size = 100_000;

            for edge_rows in edges
                .into_iter()
                .flat_map(|eid| {
                    let edge_ref = g.core_edge(eid).out_ref();
                    EdgeView::new(g, edge_ref)
                        .explode_layers()
                        .into_iter()
                        .map(|e| e.edge)
                })
                .map(|edge| ParquetCEdge(EdgeView::new(g, edge)))
                .chunks(row_group_size)
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
