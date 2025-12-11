use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::api::view::StaticGraphViewOps,
    errors::{into_graph_err, GraphError, LoadError},
    io::arrow::{
        dataframe::{DFChunk, DFView, SecondaryIndexCol},
        df_loaders::{build_progress_bar, process_shared_properties},
        layer_col::lift_layer_col,
        node_col::NodeCol,
        prop_handler::*,
    },
    prelude::*,
};
use arrow::{
    array::{AsArray, PrimitiveArray},
    datatypes::UInt64Type,
};
use bytemuck::checked::cast_slice_mut;
use db4_graph::WriteLockedGraph;
use itertools::izip;
use kdam::BarExt;
use raphtory_api::atomic_extra::atomic_vid_from_mut_slice;
use raphtory_api::{
    atomic_extra::atomic_usize_from_mut_slice,
    core::{
        entities::EID,
        storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry, FxDashMap},
    },
};
use raphtory_core::entities::{GidRef, VID};
use raphtory_storage::mutation::addition_ops::{InternalAdditionOps, SessionAdditionOps};
use rayon::prelude::*;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
        mpsc,
    },
};
use storage::{
    api::{edges::EdgeSegmentOps, nodes::NodeSegmentOps},
    pages::locked::{
        edges::{LockedEdgePage, WriteLockedEdgePages},
        nodes::LockedNodePage,
    },
    Extension,
};

#[derive(Debug, Copy, Clone)]
pub struct ColumnNames<'a> {
    pub time: &'a str,
    pub secondary_index: Option<&'a str>,
    pub src: &'a str,
    pub dst: &'a str,
    pub edge_id: Option<&'a str>,
    pub layer_col: Option<&'a str>,
}

#[allow(clippy::too_many_arguments)]
pub fn load_edges_from_df<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + Send>,
    column_names: ColumnNames,
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    if df_view.is_empty() {
        return Ok(());
    }

    let ColumnNames {
        time,
        secondary_index,
        src,
        dst,
        edge_id,
        layer_col,
    } = column_names;

    let properties_indices = properties
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;
    let metadata_indices = metadata
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;

    let src_index = df_view.get_index(src)?;
    let dst_index = df_view.get_index(dst)?;
    let time_index = df_view.get_index(time)?;
    let edge_id_col = edge_id.and_then(|name| df_view.get_index_opt(name));
    let secondary_index_index = secondary_index
        .map(|col| df_view.get_index(col))
        .transpose()?;
    let layer_index = if let Some(layer_col) = layer_col {
        Some(df_view.get_index(layer_col.as_ref())?)
    } else {
        None
    };
    let session = graph.write_session().map_err(into_graph_err)?;
    let shared_metadata = process_shared_properties(shared_metadata, |key, dtype| {
        session
            .resolve_edge_property(key, dtype, true)
            .map_err(into_graph_err)
    })?;

    // #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading edges".to_string(), df_view.num_rows)?;

    let mut src_col_resolved: Vec<VID> = vec![];
    let mut dst_col_resolved: Vec<VID> = vec![];
    let mut eid_col_resolved: Vec<EID> = vec![];
    let mut eids_exist: Vec<AtomicBool> = vec![]; // exists or needs to be created
    let mut layer_eids_exist: Vec<AtomicBool> = vec![]; // exists or needs to be created

    let resolve_ids = true; // todo add this to function params

    rayon::scope(|s| {
        let (tx, rx) = mpsc::sync_channel(2);

        s.spawn(move |_| {
            let sender = tx;
            for chunk in df_view.chunks {
                sender.send(chunk).unwrap()
            }
        });

        for chunk in rx.iter() {
            let df = chunk?;
            let prop_cols =
                combine_properties_arrow(properties, &properties_indices, &df, |key, dtype| {
                    session
                        .resolve_edge_property(key, dtype, false)
                        .map_err(into_graph_err)
                })?;
            let metadata_cols =
                combine_properties_arrow(metadata, &metadata_indices, &df, |key, dtype| {
                    session
                        .resolve_edge_property(key, dtype, true)
                        .map_err(into_graph_err)
                })?;
            // validate src and dst columns
            let src_col = df.node_col(src_index)?;
            src_col.validate(graph, LoadError::MissingSrcError)?;
            let dst_col = df.node_col(dst_index)?;
            dst_col.validate(graph, LoadError::MissingDstError)?;
            let layer = lift_layer_col(layer, layer_index, &df)?;
            let layer_col_resolved = layer.resolve(graph)?;

            let (src_vids, dst_vids, gid_str_cache) = get_or_resolve_node_vids(
                graph,
                src_index,
                dst_index,
                &mut src_col_resolved,
                &mut dst_col_resolved,
                resolve_ids,
                &df,
                &src_col,
                &dst_col,
            )?;

            let time_col = df.time_col(time_index)?;

            // Load the secondary index column if it exists, otherwise generate from start_id.
            let secondary_index_col =
                extract_secondary_index_col::<G>(secondary_index_index, &session, &df)?;

            let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;

            eid_col_resolved.resize_with(df.len(), Default::default);
            eids_exist.resize_with(df.len(), Default::default);
            layer_eids_exist.resize_with(df.len(), Default::default);
            let eid_col_shared = atomic_usize_from_mut_slice(cast_slice_mut(&mut eid_col_resolved));

            let edges = write_locked_graph.graph().storage().edges().clone();
            let next_edge_id = |row: usize| {
                let (page, pos) = edges.reserve_free_pos(row);
                pos.as_eid(page, edges.max_page_len())
            };

            let WriteLockedGraph {
                nodes, ref edges, ..
            } = &mut write_locked_graph;

            let eids = edge_id_col.and_then(|edge_id_col| {
                Some(
                    df.chunk[edge_id_col]
                        .as_primitive_opt::<UInt64Type>()?
                        .values()
                        .as_ref(),
                )
            });

            // Generate all edge_ids + add outbound edges
            nodes.par_iter_mut().for_each(|locked_page| {
                // Zip all columns for iteration.
                let zip = izip!(
                    src_vids.iter(),
                    dst_vids.iter(),
                    time_col.iter(),
                    secondary_index_col.iter(),
                    layer_col_resolved.iter()
                );

                if resolve_ids {
                    store_node_ids(&gid_str_cache, locked_page);
                }

                if resolve_ids {
                    add_and_resolve_outbound_edges(
                        &eids_exist,
                        &layer_eids_exist,
                        &eid_col_shared,
                        next_edge_id,
                        edges,
                        locked_page,
                        zip,
                    );
                } else if let Some(edge_ids) = eids {
                    add_and_resolve_outbound_edges(
                        &eids_exist,
                        &layer_eids_exist,
                        &eid_col_shared,
                        |row| EID(edge_ids[row] as usize),
                        edges,
                        locked_page,
                        zip,
                    );
                }
            });

            write_locked_graph.nodes.par_iter_mut().for_each(|shard| {
                let zip = izip!(
                    src_vids.iter(),
                    dst_vids.iter(),
                    eid_col_resolved.iter(),
                    time_col.iter(),
                    secondary_index_col.iter(),
                    layer_col_resolved.iter(),
                    layer_eids_exist.iter().map(|a| a.load(Ordering::Relaxed)),
                    eids_exist.iter().map(|b| b.load(Ordering::Relaxed))
                );

                add_outbound_edges(shard, zip);
            });

            drop(write_locked_graph);

            let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;

            write_locked_graph.edges.par_iter_mut().for_each(|shard| {
                let zip = izip!(
                    src_vids.iter(),
                    dst_vids.iter(),
                    time_col.iter(),
                    secondary_index_col.iter(),
                    eid_col_resolved.iter(),
                    layer_col_resolved.iter(),
                    eids_exist
                        .iter()
                        .map(|exists| exists.load(Ordering::Relaxed))
                );
                update_edge_properties(&shared_metadata, &prop_cols, &metadata_cols, shard, zip);
            });

            // #[cfg(feature = "python")]
            let _ = pb.update(df.len());
        }
        Ok::<_, GraphError>(())
    })?;
    // set the type of the resolver;

    Ok(())
}

fn get_or_resolve_node_vids<
    'a: 'c,
    'b: 'c,
    'c,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    src_index: usize,
    dst_index: usize,
    src_col_resolved: &'a mut Vec<VID>,
    dst_col_resolved: &'a mut Vec<VID>,
    resolve_nodes: bool,
    df: &'b DFChunk,
    src_col: &'a NodeCol,
    dst_col: &'a NodeCol,
) -> Result<
    (
        &'c [VID],
        &'c [VID],
        FxDashMap<GidRef<'a>, (Prop, MaybeNew<VID>)>,
    ),
    GraphError,
> {
    let (src_vids, dst_vids, gid_str_cache) = if resolve_nodes {
        src_col_resolved.resize_with(df.len(), Default::default);
        dst_col_resolved.resize_with(df.len(), Default::default);

        let atomic_src_col = atomic_vid_from_mut_slice(src_col_resolved);
        let atomic_dst_col = atomic_vid_from_mut_slice(dst_col_resolved);

        let gid_str_cache = resolve_nodes_with_cache::<G>(
            graph,
            [(src_col, atomic_src_col), (dst_col, atomic_dst_col)].as_ref(),
        )?;
        (
            src_col_resolved.as_slice(),
            dst_col_resolved.as_slice(),
            gid_str_cache,
        )
    } else {
        let srcs = df.chunk[src_index]
            .as_primitive_opt::<UInt64Type>()
            .ok_or_else(|| LoadError::InvalidNodeIdType(df.chunk[src_index].data_type().clone()))?
            .values()
            .as_ref();
        let dsts = df.chunk[dst_index]
            .as_primitive_opt::<UInt64Type>()
            .ok_or_else(|| LoadError::InvalidNodeIdType(df.chunk[dst_index].data_type().clone()))?
            .values()
            .as_ref();
        (
            bytemuck::cast_slice(srcs),
            bytemuck::cast_slice(dsts),
            FxDashMap::default(),
        )
    };
    Ok((src_vids, dst_vids, gid_str_cache))
}

#[inline(never)]
fn update_edge_properties<'a, ES: EdgeSegmentOps<Extension = Extension>>(
    shared_metadata: &[(usize, Prop)],
    prop_cols: &PropCols,
    metadata_cols: &PropCols,
    shard: &mut LockedEdgePage<'_, ES>,
    zip: impl Iterator<Item = (&'a VID, &'a VID, i64, usize, &'a EID, &'a usize, bool)>,
) {
    let mut t_props: Vec<(usize, Prop)> = vec![];
    let mut c_props: Vec<(usize, Prop)> = vec![];

    for (row, (src, dst, time, secondary_index, eid, layer, exists)) in zip.enumerate() {
        if let Some(eid_pos) = shard.resolve_pos(*eid) {
            let t = TimeIndexEntry(time, secondary_index);
            let mut writer = shard.writer();

            t_props.clear();
            t_props.extend(prop_cols.iter_row(row));

            c_props.clear();
            c_props.extend(metadata_cols.iter_row(row));
            c_props.extend_from_slice(shared_metadata);

            writer.bulk_add_edge(
                t,
                eid_pos,
                *src,
                *dst,
                exists,
                *layer,
                c_props.drain(..),
                t_props.drain(..),
                0,
            );
        }
    }
}

#[inline(never)]
fn add_outbound_edges<'a, NS: NodeSegmentOps<Extension = Extension>>(
    shard: &mut LockedNodePage<'_, NS>,
    zip: impl Iterator<Item = (&'a VID, &'a VID, &'a EID, i64, usize, &'a usize, bool, bool)>,
) {
    for (
        src,
        dst,
        eid,
        time,
        secondary_index,
        layer,
        edge_exists_in_layer,
        edge_exists_in_static_graph,
    ) in zip
    {
        if let Some(dst_pos) = shard.resolve_pos(*dst) {
            let t = TimeIndexEntry(time, secondary_index);
            let mut writer = shard.writer();

            if !edge_exists_in_static_graph {
                writer.add_static_inbound_edge(dst_pos, *src, *eid, 0);
            }

            if !edge_exists_in_layer {
                writer.add_inbound_edge(Some(t), dst_pos, *src, eid.with_layer(*layer), 0);
            } else {
                writer.update_timestamp(t, dst_pos, eid.with_layer(*layer), 0);
            }
        }
    }
}

#[inline(never)]
fn add_and_resolve_outbound_edges<
    'a,
    NS: NodeSegmentOps<Extension = Extension>,
    ES: EdgeSegmentOps<Extension = Extension>,
>(
    eids_exist: &[AtomicBool],
    layer_eids_exist: &[AtomicBool],
    eid_col_shared: &&mut [AtomicUsize],
    next_edge_id: impl Fn(usize) -> EID,
    edges: &WriteLockedEdgePages<'_, ES>,
    locked_page: &mut LockedNodePage<'_, NS>,
    zip: impl Iterator<Item = (&'a VID, &'a VID, i64, usize, &'a usize)>,
) {
    for (row, (src, dst, time, secondary_index, layer)) in zip.enumerate() {
        if let Some(src_pos) = locked_page.resolve_pos(*src) {
            let mut writer = locked_page.writer();
            let t = TimeIndexEntry(time, secondary_index);
            // find the original EID in the static graph if it exists
            // otherwise create a new one

            let edge_id = if let Some(edge_id) = writer.get_out_edge(src_pos, *dst, 0) {
                eid_col_shared[row].store(edge_id.0, Ordering::Relaxed);
                eids_exist[row].store(true, Ordering::Relaxed);
                edge_id.with_layer(*layer)
            } else {
                let edge_id = next_edge_id(row);
                writer.add_static_outbound_edge(src_pos, *dst, edge_id, 0);
                eid_col_shared[row].store(edge_id.0, Ordering::Relaxed);
                eids_exist[row].store(false, Ordering::Relaxed);
                edge_id.with_layer(*layer)
            };

            if edges.exists(edge_id) {
                layer_eids_exist[row].store(true, Ordering::Relaxed);
                // node additions
                writer.update_timestamp(t, src_pos, edge_id, 0);
            } else {
                layer_eids_exist[row].store(false, Ordering::Relaxed);
                // actually adds the edge
                writer.add_outbound_edge(Some(t), src_pos, *dst, edge_id, 0);
            }
        }
    }
}

#[inline(never)]
fn store_node_ids<NS: NodeSegmentOps<Extension = Extension>>(
    gid_str_cache: &FxDashMap<GidRef<'_>, (Prop, MaybeNew<VID>)>,
    locked_page: &mut LockedNodePage<'_, NS>,
) {
    for entry in gid_str_cache.iter() {
        let (src_gid, vid) = entry.value();

        if let Some(src_pos) = locked_page.resolve_pos(vid.inner()) {
            let mut writer = locked_page.writer();
            writer.store_node_id(src_pos, 0, src_gid.clone(), 0);
        }
    }
}

#[inline(never)]
fn resolve_nodes_with_cache<'a, G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    graph: &G,
    cols_to_resolve: &[(&'a NodeCol, &mut [AtomicUsize])],
) -> Result<FxDashMap<GidRef<'a>, (Prop, MaybeNew<VID>)>, GraphError> {
    let gid_str_cache: dashmap::DashMap<GidRef<'_>, (Prop, MaybeNew<VID>), _> =
        FxDashMap::default();
    let hasher_factory = gid_str_cache.hasher().clone();
    gid_str_cache
        .shards()
        .par_iter()
        .enumerate()
        .try_for_each(|(shard_idx, shard)| {
            let mut shard_guard = shard.write();
            use dashmap::SharedValue;
            use std::hash::BuildHasher;

            // Create hasher function for this shard
            let hash_key = |key: &GidRef<'_>| -> u64 { hasher_factory.hash_one(key) };

            let hasher_fn = |tuple: &(GidRef<'_>, SharedValue<(Prop, MaybeNew<VID>)>)| -> u64 {
                hasher_factory.hash_one(tuple.0)
            };

            for (col, atomic_col) in cols_to_resolve {
                // Process src_col sequentially for this shard
                for (idx, gid) in col.iter().enumerate() {
                    // Check if this key belongs to this shard
                    if gid_str_cache.determine_map(&gid) != shard_idx {
                        continue; // Skip, not our shard
                    }

                    let hash = hash_key(&gid);

                    // Check if exists in this shard
                    if let Some((_, value)) = shard_guard.get(hash, |(g, _)| g == &gid) {
                        let (_, vid) = value.get();
                        atomic_col[idx].store(vid.inner().index(), Ordering::Relaxed);
                    } else {
                        let vid = graph
                            .resolve_node(gid.as_node_ref())
                            .map_err(|_| LoadError::FatalError)?;

                        let data = (gid, SharedValue::new((Prop::from(gid), vid)));
                        shard_guard.insert(hash, data, hasher_fn);

                        atomic_col[idx].store(vid.inner().index(), Ordering::Relaxed);
                    }
                }
            }

            Ok::<(), LoadError>(())
        })?;
    Ok(gid_str_cache)
}

#[inline(never)]
fn extract_secondary_index_col<G: InternalAdditionOps + AdditionOps>(
    secondary_index_index: Option<usize>,
    session: &<G as InternalAdditionOps>::WS<'_>,
    df: &DFChunk,
) -> Result<SecondaryIndexCol, GraphError> {
    let secondary_index_col = match secondary_index_index {
        Some(col_index) => {
            // Update the event_id to reflect ingesting new secondary indices.
            let col = df.secondary_index_col(col_index)?;
            session
                .set_max_event_id(col.max())
                .map_err(into_graph_err)?;
            col
        }
        None => {
            let start_id = session
                .reserve_event_ids(df.len())
                .map_err(into_graph_err)?;
            SecondaryIndexCol::new_from_range(start_id, start_id + df.len())
        }
    };
    Ok(secondary_index_col)
}
