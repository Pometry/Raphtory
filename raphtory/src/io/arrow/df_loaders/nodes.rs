#[cfg(feature = "python")]
use crate::io::arrow::df_loaders::build_progress_bar;
use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::api::view::StaticGraphViewOps,
    errors::{into_graph_err, GraphError, LoadError},
    io::arrow::{
        dataframe::{DFChunk, DFView},
        df_loaders::{
            extract_secondary_index_col, process_shared_properties,
            resolve_nodes_and_type_with_cache, GidKey,
        },
        layer_col::{lift_node_type_col, LayerCol},
        node_col::NodeCol,
        prop_handler::*,
    },
    prelude::*,
};
use arrow::{array::AsArray, datatypes::UInt64Type};
use itertools::izip;
#[cfg(feature = "python")]
use kdam::BarExt;
use raphtory_api::{
    atomic_extra::atomic_vid_from_mut_slice,
    core::{
        entities::properties::meta::STATIC_GRAPH_LAYER_ID,
        storage::{timeindex::TimeIndexEntry, FxDashMap},
    },
};
use raphtory_core::{entities::VID, storage::timeindex::AsTime};
use raphtory_storage::mutation::addition_ops::{InternalAdditionOps, SessionAdditionOps};
use rayon::prelude::*;
use std::collections::HashMap;
use storage::{api::nodes::NodeSegmentOps, pages::locked::nodes::LockedNodePage, Extension};

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
    resolve_nodes: bool,
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

        let (src_vids, gid_str_cache) = get_or_resolve_node_vids::<G>(
            graph,
            node_id_index,
            &mut node_col_resolved,
            resolve_nodes,
            &df,
            &node_col,
            node_type_col,
        )?;

        let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;
        let node_stats = write_locked_graph.node_stats().clone();
        let update_time = |time: TimeIndexEntry| {
            let time = time.t();
            node_stats.update_time(time);
        };

        write_locked_graph
            .nodes
            .par_iter_mut()
            .try_for_each(|shard| {
                // Zip all columns for iteration.
                let zip = izip!(src_vids.iter(), time_col.iter(), secondary_index_col.iter(),);

                // resolve_nodes=false
                // assumes we are loading our own graph, via the parquet loaders,
                // so previous calls have already stored the node ids and types
                if resolve_nodes {
                    store_node_ids_and_type(&gid_str_cache, shard);
                }

                for (row, (vid, time, secondary_index)) in zip.enumerate() {
                    if let Some(mut_node) = shard.resolve_pos(*vid) {
                        let mut writer = shard.writer();
                        let t = TimeIndexEntry(time, secondary_index);
                        let layer_id = STATIC_GRAPH_LAYER_ID;

                        update_time(t);

                        let t_props = prop_cols.iter_row(row);
                        let c_props = metadata_cols
                            .iter_row(row)
                            .chain(shared_metadata.iter().cloned());

                        writer.add_props(t, mut_node, layer_id, t_props);
                        writer.update_c_props(mut_node, layer_id, c_props);
                    };
                }

                Ok::<_, GraphError>(())
            })?;

        #[cfg(feature = "python")]
        let _ = pb.update(df.len());
    }

    Ok(())
}

pub fn load_node_props_from_df<
    'a,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + std::fmt::Debug,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    node_id: &str,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    node_id_col: Option<&str>,      // provided by our parquet encoder
    node_type_id_col: Option<&str>, // provided by our parquet encoder
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
    let node_type_ids_col = node_type_id_col
        .map(|node_type_id_col| df_view.get_index(node_type_id_col.as_ref()))
        .transpose()?;

    let node_id_index = node_id_col
        .map(|node_col| df_view.get_index(node_col.as_ref()))
        .transpose()?;

    let node_gid_index = df_view.get_index(node_id)?;
    let session = graph.write_session().map_err(into_graph_err)?;

    let shared_metadata = process_shared_properties(shared_metadata, |key, dtype| {
        session
            .resolve_node_property(key, dtype, true)
            .map_err(into_graph_err)
    })?;

    let resolve_nodes = node_type_ids_col.is_some() && node_id_index.is_some();

    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading node properties".to_string(), df_view.num_rows)?;

    let mut node_col_resolved = vec![];
    let mut node_type_resolved = vec![];

    for chunk in df_view.chunks {
        let df = chunk?;
        if df.is_empty() {
            continue;
        }
        let metadata_cols =
            combine_properties_arrow(metadata, &metadata_indices, &df, |key, dtype| {
                session
                    .resolve_node_property(key, dtype, true)
                    .map_err(into_graph_err)
            })?;
        let node_type_col = lift_node_type_col(node_type, node_type_index, &df)?;
        let node_col = df.node_col(node_gid_index)?;

        let (node_col_resolved, node_type_col_resolved) = get_or_resolve_node_vids_no_events::<G>(
            graph,
            &session,
            &mut node_col_resolved,
            &mut node_type_resolved,
            node_type_ids_col,
            node_id_index,
            &df,
            &node_col,
            node_type_col,
        )?;

        // We assume this is fast enough
        let max_vid = node_col_resolved
            .iter()
            .map(|vid| vid.index())
            .max()
            .map(VID)
            .unwrap_or(VID(0));
        let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;
        write_locked_graph.resize_chunks_to_vid(max_vid);

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
                    writer.store_node_id_and_node_type(
                        mut_node,
                        STATIC_GRAPH_LAYER_ID,
                        gid,
                        *node_type,
                    );

                    if resolve_nodes {
                        // because we don't call resolve_node above
                        writer.increment_seg_num_nodes()
                    }

                    c_props.clear();
                    c_props.extend(metadata_cols.iter_row(idx));
                    c_props.extend_from_slice(&shared_metadata);
                    if !c_props.is_empty() {
                        writer.update_c_props(mut_node, STATIC_GRAPH_LAYER_ID, c_props.drain(..));
                    }
                };
            }

            Ok::<_, GraphError>(())
        })?;

        #[cfg(feature = "python")]
        let _ = pb.update(df.len());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn get_or_resolve_node_vids<
    'a: 'c,
    'b: 'c,
    'c,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    src_index: usize,
    src_col_resolved: &'a mut Vec<VID>,
    resolve_nodes: bool,
    df: &'b DFChunk,
    src_col: &'a NodeCol,
    node_type_col: LayerCol<'a>,
) -> Result<(&'c [VID], FxDashMap<GidKey<'a>, (VID, usize)>), GraphError> {
    let (src_vids, gid_str_cache) = if resolve_nodes {
        src_col_resolved.resize_with(df.len(), Default::default);

        let atomic_src_col = atomic_vid_from_mut_slice(src_col_resolved);

        let gid_str_cache = resolve_nodes_and_type_with_cache::<G>(
            graph,
            [src_col].as_ref(),
            [atomic_src_col].as_ref(),
            node_type_col,
        )?;
        (src_col_resolved.as_slice(), gid_str_cache)
    } else {
        let srcs = df.chunk[src_index]
            .as_primitive_opt::<UInt64Type>()
            .ok_or_else(|| LoadError::InvalidNodeIdType(df.chunk[src_index].data_type().clone()))?
            .values()
            .as_ref();
        (bytemuck::cast_slice(srcs), FxDashMap::default())
    };
    Ok((src_vids, gid_str_cache))
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn get_or_resolve_node_vids_no_events<
    'a: 'c,
    'b: 'c,
    'c,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    session: &<G as InternalAdditionOps>::WS<'_>,
    node_col_resolved: &'a mut Vec<VID>,
    node_type_resolved: &'a mut Vec<usize>,
    node_type_ids_col: Option<usize>,
    node_id_col: Option<usize>,
    df: &'b DFChunk,
    src_col: &'a NodeCol,
    node_type_col: LayerCol<'a>,
) -> Result<(&'c [VID], &'c [usize]), GraphError> {
    assert!(!(node_type_ids_col.is_none() ^ node_id_col.is_none())); // both some or both none
    if let Some((node_type_index, node_id_col)) = node_type_ids_col.zip(node_id_col) {
        set_meta_for_pre_resolved_nodes_and_node_ids(
            graph,
            session,
            df,
            src_col,
            node_type_col,
            node_type_index,
            node_id_col,
        )
    } else {
        resolve_node_and_meta_for_node_col(
            graph,
            node_col_resolved,
            node_type_resolved,
            df,
            src_col,
            node_type_col,
        )
    }
}

fn resolve_node_and_meta_for_node_col<
    'a,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    node_col_resolved: &'a mut Vec<VID>,
    node_type_resolved: &'a mut Vec<usize>,
    df: &DFChunk,
    src_col: &NodeCol,
    node_type_col: LayerCol<'a>,
) -> Result<(&'a [VID], &'a [usize]), GraphError> {
    node_col_resolved.resize_with(df.len(), Default::default);
    node_type_resolved.resize_with(df.len(), Default::default);

    let mut locked_mapper = graph.node_meta().node_type_meta().write();

    let zip = izip!(
        src_col.iter(),
        node_type_col.iter(),
        node_col_resolved.iter_mut(),
        node_type_resolved.iter_mut()
    );

    let mut last_node_type: Option<&str> = None;
    let mut last_node_type_id: Option<usize> = None;
    for (gid, node_type, vid, node_type_id) in zip {
        if last_node_type != node_type {
            if let Some(name) = node_type {
                let resolved_node_type_id = locked_mapper.get_or_create_id(name).inner();
                *node_type_id = resolved_node_type_id;
                last_node_type_id = Some(resolved_node_type_id);
            }
        } else if let Some(id) = last_node_type_id {
            *node_type_id = id;
        }

        let res_vid = graph
            .resolve_node(gid.as_node_ref())
            .map_err(into_graph_err)?;
        *vid = res_vid.inner();
        last_node_type = node_type;
    }

    Ok((node_col_resolved.as_slice(), node_type_resolved.as_slice()))
}

fn set_meta_for_pre_resolved_nodes_and_node_ids<
    'b,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    session: &<G as InternalAdditionOps>::WS<'_>,
    df: &'b DFChunk,
    src_col: &NodeCol,
    node_type_col: LayerCol<'_>,
    node_type_index: usize,
    node_id_col: usize,
) -> Result<(&'b [VID], &'b [usize]), GraphError> {
    let srcs = df.chunk[node_id_col]
        .as_primitive_opt::<UInt64Type>()
        .ok_or_else(|| LoadError::InvalidNodeIdType(df.chunk[node_id_col].data_type().clone()))?
        .values()
        .as_ref();

    let node_types = df.chunk[node_type_index]
        .as_primitive_opt::<UInt64Type>()
        .ok_or_else(|| LoadError::InvalidNodeType(df.chunk[node_type_index].data_type().clone()))?
        .values()
        .as_ref();

    let mut locked_mapper = graph.node_meta().node_type_meta().write();

    let zip = izip!(
        src_col.iter(),
        srcs.iter(),
        node_type_col.iter(),
        node_types.iter()
    );

    let mut last_node_type: Option<&str> = None;

    for (gid, node_id, node_type, node_type_id) in zip {
        if last_node_type != node_type {
            let node_type_name = node_type.unwrap_or("_default");
            locked_mapper.set_id(node_type_name, *node_type_id as usize);
        }
        last_node_type = node_type;
        session
            .set_node(gid, VID(*node_id as usize))
            .map_err(into_graph_err)?;
    }

    Ok((bytemuck::cast_slice(srcs), bytemuck::cast_slice(node_types)))
}

#[inline(never)]
fn store_node_ids_and_type<NS: NodeSegmentOps<Extension = Extension>>(
    gid_str_cache: &FxDashMap<GidKey<'_>, (VID, usize)>,
    locked_page: &mut LockedNodePage<'_, NS>,
) {
    for entry in gid_str_cache.iter() {
        let (vid, node_type) = entry.value();
        let GidKey { gid, .. } = entry.key();

        if let Some(src_pos) = locked_page.resolve_pos(*vid) {
            let mut writer = locked_page.writer();
            writer.store_node_id_and_node_type(src_pos, STATIC_GRAPH_LAYER_ID, *gid, *node_type);
        }
    }
}
