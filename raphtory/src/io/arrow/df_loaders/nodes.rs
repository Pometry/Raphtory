#[cfg(feature = "python")]
use crate::io::arrow::df_loaders::build_progress_bar;
use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::api::view::StaticGraphViewOps,
    errors::{into_graph_err, GraphError, LoadError},
    io::arrow::{
        dataframe::{DFChunk, DFView, SecondaryIndexCol},
        df_loaders::{extract_secondary_index_col, process_shared_properties},
        layer_col::lift_node_type_col,
        prop_handler::*,
    },
    prelude::*,
};
use itertools::izip;
#[cfg(feature = "python")]
use kdam::BarExt;
use raphtory_api::core::{
    entities::properties::meta::STATIC_GRAPH_LAYER_ID, storage::timeindex::TimeIndexEntry,
};
use raphtory_core::storage::timeindex::AsTime;
use raphtory_storage::mutation::addition_ops::{InternalAdditionOps, SessionAdditionOps};
use rayon::prelude::*;
use std::collections::HashMap;

pub fn load_nodes_from_df<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + std::fmt::Debug,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    time: &str,
    secondary_index: Option<&str>,
    node_id: &str,
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    if df_view.is_empty() {
        return Ok(());
    }
    let properties_indices = properties
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;
    let metadata_indices = metadata
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;

    let node_type_index =
        node_type_col.map(|node_type_col| df_view.get_index(node_type_col.as_ref()));
    let node_type_index = node_type_index.transpose()?;

    let node_id_index = df_view.get_index(node_id)?;
    let time_index = df_view.get_index(time)?;
    let secondary_index_index = secondary_index
        .map(|col| df_view.get_index(col))
        .transpose()?;

    let session = graph.write_session().map_err(into_graph_err)?;
    let shared_metadata = process_shared_properties(shared_metadata, |key, dtype| {
        session
            .resolve_node_property(key, dtype, true)
            .map_err(into_graph_err)
    })?;

    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading nodes".to_string(), df_view.num_rows)?;

    let mut node_col_resolved = vec![];
    let mut node_type_col_resolved = vec![];

    let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;

    for chunk in df_view.chunks {
        let df = chunk?;
        let prop_cols =
            combine_properties_arrow(properties, &properties_indices, &df, |key, dtype| {
                session
                    .resolve_node_property(key, dtype, false)
                    .map_err(into_graph_err)
            })?;
        let metadata_cols =
            combine_properties_arrow(metadata, &metadata_indices, &df, |key, dtype| {
                session
                    .resolve_node_property(key, dtype, true)
                    .map_err(into_graph_err)
            })?;
        let node_type_col = lift_node_type_col(node_type, node_type_index, &df)?;

        let time_col = df.time_col(time_index)?;
        let node_col = df.node_col(node_id_index)?;

        // Load the secondary index column if it exists, otherwise generate from start_id.
        let secondary_index_col =
            extract_secondary_index_col::<G>(secondary_index_index, &session, &df)?;
        node_col_resolved.resize_with(df.len(), Default::default);
        node_type_col_resolved.resize_with(df.len(), Default::default);

        // TODO: Using parallel iterators results in a 5x speedup, but
        // needs to be implemented such that node VID order is preserved.
        // See: https://github.com/Pometry/pometry-storage/issues/81
        for (gid, resolved, node_type, node_type_resolved) in izip!(
            node_col.iter(),
            node_col_resolved.iter_mut(),
            node_type_col.iter(),
            node_type_col_resolved.iter_mut()
        ) {
            let (vid, res_node_type) = write_locked_graph
                .graph()
                .resolve_node_and_type(gid.as_node_ref(), node_type)
                .map_err(|_| LoadError::FatalError)?;

            *resolved = vid;
            *node_type_resolved = res_node_type;
        }

        let node_stats = write_locked_graph.node_stats().clone();
        let update_time = |time: TimeIndexEntry| {
            let time = time.t();
            node_stats.update_time(time);
        };

        write_locked_graph
            .resize_chunks_to_num_nodes(write_locked_graph.graph().internal_num_nodes());

        write_locked_graph
            .nodes
            .par_iter_mut()
            .try_for_each(|shard| {
                // Zip all columns for iteration.
                let zip = izip!(
                    node_col_resolved.iter(),
                    time_col.iter(),
                    secondary_index_col.iter(),
                    node_type_col_resolved.iter(),
                    node_col.iter()
                );

                for (row, (vid, time, secondary_index, node_type, gid)) in zip.enumerate() {
                    if let Some(mut_node) = shard.resolve_pos(*vid) {
                        let mut writer = shard.writer();
                        let t = TimeIndexEntry(time, secondary_index);
                        let layer_id = STATIC_GRAPH_LAYER_ID;
                        let lsn = 0;

                        update_time(t);
                        writer
                            .store_node_id_and_node_type(mut_node, layer_id, gid, *node_type, lsn);

                        let t_props = prop_cols.iter_row(row);
                        let c_props = metadata_cols
                            .iter_row(row)
                            .chain(shared_metadata.iter().cloned());

                        writer.add_props(t, mut_node, layer_id, t_props, lsn);
                        writer.update_c_props(mut_node, layer_id, c_props, lsn);
                    };
                }

                Ok::<_, GraphError>(())
            })?;

        #[cfg(feature = "python")]
        let _ = pb.update(df.len());
    }

    Ok(())
}

pub(crate) fn load_node_props_from_df<
    'a,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + std::fmt::Debug,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    node_id: &str,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    graph: &G,
) -> Result<(), GraphError> {
    if df_view.is_empty() {
        return Ok(());
    }
    let metadata_indices = metadata
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;

    let node_type_index =
        node_type_col.map(|node_type_col| df_view.get_index(node_type_col.as_ref()));
    let node_type_index = node_type_index.transpose()?;

    let node_id_index = df_view.get_index(node_id)?;
    let session = graph.write_session().map_err(into_graph_err)?;

    let shared_metadata = process_shared_properties(shared_metadata, |key, dtype| {
        session
            .resolve_node_property(key, dtype, true)
            .map_err(into_graph_err)
    })?;

    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading node properties".to_string(), df_view.num_rows)?;

    let mut node_col_resolved = vec![];
    let mut node_type_col_resolved = vec![];

    let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;

    for chunk in df_view.chunks {
        let df = chunk?;
        let metadata_cols =
            combine_properties_arrow(metadata, &metadata_indices, &df, |key, dtype| {
                session
                    .resolve_node_property(key, dtype, true)
                    .map_err(into_graph_err)
            })?;
        let node_type_col = lift_node_type_col(node_type, node_type_index, &df)?;
        let node_col = df.node_col(node_id_index)?;

        node_col_resolved.resize_with(df.len(), Default::default);
        node_type_col_resolved.resize_with(df.len(), Default::default);

        node_col
            .iter()
            .zip(node_col_resolved.iter_mut())
            .zip(node_type_col.iter())
            .zip(node_type_col_resolved.iter_mut())
            .try_for_each(|(((gid, resolved), node_type), node_type_resolved)| {
                let (vid, res_node_type) = write_locked_graph
                    .graph()
                    .resolve_node_and_type(gid.as_node_ref(), node_type)
                    .map_err(|_| LoadError::FatalError)?;
                *resolved = vid;
                *node_type_resolved = res_node_type;
                Ok::<(), LoadError>(())
            })?;

        write_locked_graph
            .resize_chunks_to_num_nodes(write_locked_graph.graph().internal_num_nodes());

        write_locked_graph.nodes.iter_mut().try_for_each(|shard| {
            let mut c_props = vec![];

            for (idx, ((vid, node_type), gid)) in node_col_resolved
                .iter()
                .zip(node_type_col_resolved.iter())
                .zip(node_col.iter())
                .enumerate()
            {
                if let Some(mut_node) = shard.resolve_pos(*vid) {
                    let mut writer = shard.writer();
                    writer.store_node_id_and_node_type(mut_node, 0, gid, *node_type, 0);

                    c_props.clear();
                    c_props.extend(metadata_cols.iter_row(idx));
                    c_props.extend_from_slice(&shared_metadata);
                    writer.update_c_props(mut_node, 0, c_props.drain(..), 0);
                };
            }

            Ok::<_, GraphError>(())
        })?;

        #[cfg(feature = "python")]
        let _ = pb.update(df.len());
    }
    Ok(())
}
