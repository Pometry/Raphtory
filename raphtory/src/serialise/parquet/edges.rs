use std::path::Path;

use super::*;
use crate::{
    core::utils::iter::GenLockedIter,
    db::{
        api::{
            storage::graph::edges::edge_storage_ops::EdgeStorageOps,
            view::internal::{CoreGraphOps, TimeSemantics},
        },
        graph::edge::EdgeView,
    },
    serialise::parquet::model::ParquetDelEdge,
};
use arrow_schema::{DataType, Field};
use model::ParquetCEdge;
use raphtory_api::{
    core::{
        entities::{LayerIds, EID},
        storage::timeindex::TimeIndexIntoOps,
    },
    iter::IntoDynBoxed,
};

pub(crate) fn encode_edge_tprop(
    g: &GraphStorage,
    path: impl AsRef<Path>,
) -> Result<(), GraphError> {
    run_encode(
        g,
        g.edge_meta().temporal_prop_meta(),
        g.unfiltered_num_edges(),
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
            let all_layers = LayerIds::All;

            for edge_rows in edges
                .into_iter()
                .map(EID)
                .flat_map(|eid| {
                    let edge_ref = g.core_edge(eid).out_ref();
                    g.edge_exploded(edge_ref, &all_layers)
                })
                .map(|edge| ParquetTEdge(EdgeView::new(g, edge)))
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
    run_encode(
        g,
        g.edge_meta().temporal_prop_meta(),
        g.unfiltered_num_edges(),
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
                .map(EID)
                .flat_map(|eid| {
                    (0..g.unfiltered_num_layers()).flat_map(move |layer_id| {
                        let edge = g_edges.edge(eid);
                        let edge_ref = edge.out_ref();
                        GenLockedIter::from(edge, |edge| {
                            edge.deletions(layer_id).into_iter().into_dyn_boxed()
                        })
                        .map(move |deletions| ParquetDelEdge {
                            del: deletions,
                            layer: &layers[layer_id],
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
    run_encode(
        g,
        g.edge_meta().const_prop_meta(),
        g.unfiltered_num_edges(),
        path,
        EDGES_C_PATH,
        |id_type| {
            vec![
                Field::new(SRC_COL, id_type.clone(), false),
                Field::new(DST_COL, id_type.clone(), false),
                Field::new(LAYER_COL, DataType::Utf8, false),
            ]
        },
        |edges, g, decoder, writer| {
            let row_group_size = 100_000.min(edges.len());
            let all_layers = LayerIds::All;

            for edge_rows in edges
                .into_iter()
                .map(EID)
                .flat_map(|eid| {
                    let edge_ref = g.core_edge(eid).out_ref();
                    g.edge_layers(edge_ref, &all_layers)
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
