use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::api::view::StaticGraphViewOps,
    errors::{into_graph_err, GraphError, LoadError},
    prelude::*,
};
use arrow::{array::AsArray, datatypes::UInt64Type};
use itertools::izip;
use raphtory_api::{
    atomic_extra::atomic_vid_from_mut_slice,
    core::{
        entities::{
            properties::{
                meta::{DEFAULT_NODE_TYPE_ID, NODE_TYPE_IDX, STATIC_GRAPH_LAYER_ID},
                prop::AsPropRef,
            },
            LayerId,
        },
        storage::{dict_mapper::MaybeNew, timeindex::EventTime},
    },
};
use raphtory_core::{
    entities::{GidRef, VID},
    storage::timeindex::AsTime,
};
use raphtory_storage::mutation::addition_ops::{InternalAdditionOps, SessionAdditionOps};
use rayon::prelude::*;
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, mpsc},
};
use storage::{
    api::{node_type_index::NodeTypeIndexOps, nodes::NodeSegmentOps},
    pages::locked::nodes::LockedNodePage,
    segments::node_type_index::index::MemNodeTypeIndex,
    Extension,
};

#[cfg(feature = "progress")]
use crate::arrow_loader::df_loaders::build_progress_bar;
use crate::arrow_loader::{
    dataframe::{DFChunk, DFView},
    df_loaders::{
        extract_secondary_index_col, group_rows_by_vid_segment,
        cache::NodeResolveCache, process_shared_properties, secondary_index_at,
    },
    layer_col::{lift_layer_col, lift_node_type_col, LayerCol},
    node_col::NodeCol,
    prop_handler::*,
    LOAD_POOL,
};
#[cfg(feature = "progress")]
use kdam::BarExt;

/// If layer_id_col is provided, then layer_col must also be provided
#[allow(clippy::too_many_arguments)]
pub fn load_nodes_from_df_prefetch<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + std::fmt::Debug,
    I1: Iterator<Item = Result<DFChunk, GraphError>> + Send,
>(
    df_view: DFView<I1>,
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
    layer: Option<&str>,
    layer_col: Option<&str>,
    layer_id_col: Option<&str>,
) -> Result<(), GraphError> {
    let DFView {
        names,
        chunks,
        num_rows,
    } = df_view;

    LOAD_POOL.install(|| {
        rayon::scope(|s| {
            let (tx, rx) = mpsc::sync_channel(2);

            s.spawn(move |_| {
                let sender = tx;
                for chunk in chunks {
                    if let Err(e) = sender.send(chunk) {
                        eprintln!("Error sending chunk to loader: {}", e);
                        break;
                    }
                }
            });

            let df_view_prefetch = DFView {
                names,
                chunks: rx.into_iter(),
                num_rows,
            };

            load_nodes_from_df(
                df_view_prefetch,
                time,
                secondary_index,
                node_id,
                properties,
                metadata,
                shared_metadata,
                node_type,
                node_type_col,
                graph,
                resolve_nodes,
                layer,
                layer_col,
                layer_id_col,
            )?;
            Ok::<(), GraphError>(())
        })?;

        Ok(())
    })
}

#[allow(clippy::too_many_arguments)]
pub fn load_nodes_from_df<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + std::fmt::Debug,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + Send>,
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
    layer: Option<&str>,
    layer_col: Option<&str>,
    layer_id_col: Option<&str>,
) -> Result<(), GraphError> {
    if df_view.is_empty() {
        return Ok(());
    }

    LOAD_POOL.install(move || {
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
        let layer_col_index = layer_col.map(|name| df_view.get_index(name)).transpose()?;
        let layer_id_index = layer_id_col
            .map(|name| df_view.get_index(name))
            .transpose()?;

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

        #[cfg(feature = "progress")]
        let mut pb = build_progress_bar("Loading nodes".to_string(), df_view.num_rows)?;

        let mut node_col_resolved = vec![];
        let mut node_resolve_cache: Option<NodeResolveCache<(VID, usize)>> = None;

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
            let node_type_col_resolved = node_type_col.resolve_node_type(graph)?;

            // Two paths:
            // Fast path (parquet round-trip) when both layer_col and layer_id_col are provided.
            // Slow path (user-facing CSV/parquet without numeric ids) resolve by name
            let layer_col_resolved = if layer.is_some() || layer_col_index.is_some() {
                let layer_col = lift_layer_col(layer, layer_col_index, &df)?;
                let layer_id_values = layer_id_index
                    .map(|idx| {
                        df.chunk[idx]
                            .as_primitive_opt::<UInt64Type>()
                            .ok_or_else(|| {
                                LoadError::InvalidLayerType(df.chunk[idx].data_type().clone())
                            })
                            .map(|array| array.values().as_ref())
                    })
                    .transpose()?;

                Some(layer_col.resolve_layer(layer_id_values, graph, true)?)
            } else {
                None
            };

            let time_col = df.time_col(time_index)?;
            let node_col = df.node_col(node_id_index)?;

            // Load the secondary index column if it exists, otherwise generate from start_id.
            let secondary_index_col =
                extract_secondary_index_col::<G>(secondary_index_index, &session, &df)?;
            node_col_resolved.resize_with(df.len(), Default::default);

            let (src_vids, gid_str_cache) = get_or_resolve_node_vids::<G>(
                graph,
                node_id_index,
                &mut node_resolve_cache,
                &mut node_col_resolved,
                &node_type_col_resolved,
                resolve_nodes,
                &df,
                &node_col,
            )?;

            if resolve_nodes && !gid_str_cache.is_empty() {
                let index = graph.core_graph().node_type_index();
                populate_node_type_index(&gid_str_cache, &index.head());
                index.notify_write();
            }

            let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;
            let node_stats = write_locked_graph.node_stats().clone();

            let max_node_segment_len = write_locked_graph
                .graph()
                .storage()
                .nodes()
                .max_segment_len() as usize;

            let rows_by_segment = group_rows_by_vid_segment(
                src_vids,
                max_node_segment_len as u32,
                write_locked_graph.nodes.len(),
            );

            write_locked_graph
                .nodes
                .par_iter_mut()
                .enumerate()
                .try_for_each(|(segment_id, shard)| {
                    let node_rows = &rows_by_segment[segment_id];

                    if node_rows.is_empty() {
                        // Grab a writer to force a drop -> flush check as the segment might have
                        // writes from previous chunks that need to be flushed.
                        if shard.segment().is_dirty() {
                            let _writer = shard.writer();
                        }

                        return Ok::<_, GraphError>(());
                    }

                    // Zip all columns for iteration.
                    let zip = node_rows.iter().map(|&row| {
                        let vid = &src_vids[row];
                        let time = time_col[row];
                        let secondary_index = secondary_index_at(&secondary_index_col, row);
                        (row, vid, time, secondary_index)
                    });

                    // resolve_nodes=false
                    // assumes we are loading our own graph, via the parquet loaders,
                    // so previous calls have already stored the node ids and types
                    if resolve_nodes {
                        store_node_ids_and_type(&gid_str_cache, shard);
                    }

                    let mut writer = shard.writer();

                    for (row, vid, time, secondary_index) in zip {
                        if let Some(mut_node) = writer.resolve_pos(*vid) {
                            let t = EventTime(time, secondary_index);
                            let layer_id = layer_col_resolved
                                .as_ref()
                                .map_or(STATIC_GRAPH_LAYER_ID, |r| LayerId(r[row]));

                            node_stats.update_time(t.t());

                            let t_props = prop_cols.iter_row(row);
                            let c_props = metadata_cols.iter_row(row).chain(
                                shared_metadata
                                    .iter()
                                    .map(|(id, prop)| (*id, prop.as_prop_ref())),
                            );

                            writer.add_props(t, mut_node, layer_id, t_props);
                            writer.update_c_props(mut_node, layer_id, c_props);
                        };
                    }

                    Ok::<_, GraphError>(())
                })?;

            #[cfg(feature = "progress")]
            let _ = pb.update(df.len());
        }

        Ok::<_, GraphError>(())
    })?;

    Ok(())
}

/// Must be called from a single-threaded context if is_materializing == true && node_id_col.is_none() && node_type_id_col.is_none()
#[allow(clippy::too_many_arguments)]
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
    is_materializing: bool,
    layer: Option<&str>,
    layer_col: Option<&str>,
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

    let layer_col_index = layer_col.map(|name| df_view.get_index(name)).transpose()?;

    let node_gid_index = df_view.get_index(node_id)?;
    let session = graph.write_session().map_err(into_graph_err)?;

    let shared_metadata = process_shared_properties(shared_metadata, |key, dtype| {
        session
            .resolve_node_property(key, dtype, true)
            .map_err(into_graph_err)
    })?;

    let resolve_nodes = node_type_ids_col.is_some() && node_id_index.is_some();

    #[cfg(feature = "progress")]
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
        // In the public API, all node_props/nodes_c/node metadata go to STATIC_GRAPH_LAYER.
        let layer_col_resolved = if layer.is_some() || layer_col_index.is_some() {
            let layer_col = lift_layer_col(layer, layer_col_index, &df)?;
            Some(layer_col.resolve_layer(None, graph, true)?)
        } else {
            None
        };

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
            is_materializing,
        )?;

        // We assume this is fast enough
        let max_vid = node_col_resolved
            .iter()
            .filter(|vid| vid.is_initialised())
            .map(|vid| vid.index())
            .max()
            .map(VID)
            .unwrap_or(VID(0));
        let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;
        write_locked_graph.resize_segments_to_vid(max_vid);

        write_locked_graph
            .nodes
            .par_iter_mut()
            .try_for_each(|shard| {
                let mut c_props = vec![];
                let mut writer = shard.writer();

                for (idx, ((vid, node_type), gid)) in node_col_resolved
                    .iter()
                    .zip(node_type_col_resolved.iter())
                    .zip(node_col.iter())
                    .enumerate()
                    .filter(|(_, ((vid, _), _))| vid.is_initialised())
                // Filter out unresolved vids
                {
                    if let Some(pos) = writer.resolve_pos(*vid) {
                        let row_layer = layer_col_resolved
                            .as_ref()
                            .map_or(STATIC_GRAPH_LAYER_ID, |r| LayerId(r[idx]));

                        writer.store_node_id_and_node_type(pos, Some(gid), *node_type);

                        if resolve_nodes {
                            // because we don't call resolve_node above
                            writer.increment_seg_num_nodes()
                        }

                        c_props.clear();
                        c_props.extend(metadata_cols.iter_row(idx));
                        c_props.extend(shared_metadata.iter().map(|(i, p)| (*i, p.as_prop_ref())));

                        if !c_props.is_empty() {
                            writer.update_c_props(pos, row_layer, c_props.drain(..));
                        }
                    };
                }

                Ok::<_, GraphError>(())
            })?;

        #[cfg(feature = "progress")]
        let _ = pb.update(df.len());
    }
    Ok(())
}

type Resolved<'a> = (GidRef<'a>, (VID, usize));

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn get_or_resolve_node_vids<
    'a: 'c,
    'b: 'c,
    'c,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    src_index: usize,
    node_resolve_cache: &mut Option<NodeResolveCache<(VID, usize)>>,
    src_col_resolved: &'a mut Vec<VID>,
    node_type_resolved: &'a [usize],
    resolve_nodes: bool,
    df: &'b DFChunk,
    src_col: &'a NodeCol,
) -> Result<(&'c [VID], Vec<Resolved<'a>>), GraphError> {
    let (src_vids, gid_str_cache) = if resolve_nodes {
        let cache = node_resolve_cache
            .get_or_insert_with(|| NodeResolveCache::new(df.len(), src_col.dtype()));

        resolve_node_vids_and_types_with_cache(
            graph,
            cache,
            src_col_resolved,
            src_col,
            node_type_resolved,
            df.len(),
        )?
    } else {
        let srcs = df.chunk[src_index]
            .as_primitive_opt::<UInt64Type>()
            .ok_or_else(|| LoadError::InvalidNodeIdType(df.chunk[src_index].data_type().clone()))?
            .values()
            .as_ref();

        (bytemuck::cast_slice(srcs), vec![])
    };

    Ok((src_vids, gid_str_cache))
}

/// Resolves node GIDs and types using `NodeResolveCache`.
///
/// The returned list only contains GIDs that still need their id/type written to the segments:
/// newly created GID -> VID mappings, or existing nodes that do not yet have a stored type.
///
/// Cache hits and existing nodes that already have a matching type are skipped.
/// A GID that reappears with a different node type is rejected.
fn resolve_node_vids_and_types_with_cache<
    'a,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    cache: &mut NodeResolveCache<(VID, usize)>,
    node_col_resolved: &'a mut Vec<VID>,
    node_col: &'a NodeCol,
    node_types: &[usize],
    len: usize,
) -> Result<(&'a [VID], Vec<Resolved<'a>>), GraphError> {
    node_col_resolved.resize_with(len, Default::default);
    let atomic_node_col = atomic_vid_from_mut_slice(node_col_resolved);

    let new_gids_by_shard = cache
        .par_iter_mut()
        .map(|mut shard| {
            let mut new_gids = vec![];

            for (row, (gid, vid_slot)) in node_col.iter().zip(atomic_node_col.iter()).enumerate() {
                if !shard.is_in_shard(gid) {
                    continue;
                }

                let node_type = node_types[row];
                let mut should_store = false;

                let resolved = shard.resolve_with(gid, || {
                    let vid = unsafe { graph.bulk_load_resolve_node(gid).map_err(into_graph_err)? };

                    match vid {
                        MaybeNew::New(vid) => {
                            should_store = true;
                            Ok((vid, node_type))
                        }
                        MaybeNew::Existing(vid) => match existing_stored_node_type(graph, vid) {
                            None => {
                                should_store = true;
                                Ok((vid, node_type))
                            }
                            Some(existing) if existing == node_type => Ok((vid, existing)),
                            Some(existing) => Err(GraphError::LoadError {
                                source: LoadError::ConflictingNodeType {
                                    gid: gid.into(),
                                    existing: node_type_name(graph, existing),
                                    new: node_type_name(graph, node_type),
                                },
                            }),
                        },
                    }
                })?;

                let (vid, cached_node_type) = resolved.inner();

                if cached_node_type != node_type {
                    return Err(GraphError::LoadError {
                        source: LoadError::ConflictingNodeType {
                            gid: gid.into(),
                            existing: node_type_name(graph, cached_node_type),
                            new: node_type_name(graph, node_type),
                        },
                    });
                }

                if should_store {
                    new_gids.push((gid, (vid, cached_node_type)));
                }

                vid_slot.store(vid.0, Ordering::Relaxed);
            }

            Ok(new_gids)
        })
        .collect::<Result<Vec<_>, GraphError>>()?;

    // Shards own disjoint gids, so concatenating is already deduplicated.
    Ok((node_col_resolved.as_slice(), new_gids_by_shard.concat()))
}

fn existing_stored_node_type<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    graph: &G,
    vid: VID,
) -> Option<usize> {
    graph
        .node_metadata(vid, NODE_TYPE_IDX)
        .and_then(|prop| prop.into_u64())
        .map(|id| id as usize)
}

fn node_type_name<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    graph: &G,
    node_type: usize,
) -> String {
    graph
        .node_meta()
        .get_node_type_name_by_id(node_type)
        .map(|name| name.to_string())
        .unwrap_or_else(|| "no type".to_string())
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
    is_materializing: bool,
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
            is_materializing,
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
    is_materializing: bool,
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
            } else {
                *node_type_id = 0;
                last_node_type_id = Some(0);
            }
        } else if let Some(id) = last_node_type_id {
            *node_type_id = id;
        }

        // Create the node if it doesn't exist yet so metadata-only callers
        // (e.g. materialize loading node c_props before t_props) still
        // allocate a fresh VID in the target graph.
        let res_vid = if is_materializing {
            // Safe because load_node_props_from_df is called sequentially from the
            // materialize_impl consumer loop (one record batch at a time), and the resolve loop is serial
            // both here and in load_node_props_from_df, so no other thread resolves the same id concurrently.
            // Other future callers should make sure to utilize this pathway in single-threaded contexts only.
            unsafe {
                graph
                    .bulk_load_resolve_node(gid)
                    .map_err(into_graph_err)?
                    .inner()
            }
        } else {
            graph
                .internalise_node(gid.as_node_ref())
                .unwrap_or_default()
        };
        *vid = res_vid;
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
    gid_str_cache: &[Resolved<'_>],
    locked_page: &mut LockedNodePage<'_, NS>,
) {
    let mut writer = locked_page.writer();

    for (gid, (vid, node_type)) in gid_str_cache.iter() {
        if let Some(src_pos) = writer.resolve_pos(*vid) {
            writer.store_node_id_and_node_type(src_pos, Some(*gid), *node_type);
        }
    }
}

fn populate_node_type_index(gid_str_cache: &[Resolved<'_>], index: &MemNodeTypeIndex) {
    let mut by_type: HashMap<usize, Vec<VID>> = HashMap::new();

    for (_, (vid, node_type)) in gid_str_cache {
        // Nodes with default type don't need to be indexed.
        if *node_type == DEFAULT_NODE_TYPE_ID {
            continue;
        }

        by_type.entry(*node_type).or_default().push(*vid);
    }

    by_type.par_iter().for_each(|(node_type, vids)| {
        index.insert_batch(*node_type, vids.iter().copied());
    });
}
