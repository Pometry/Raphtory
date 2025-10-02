use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, LayerIds},
    db::api::view::StaticGraphViewOps,
    errors::{into_graph_err, GraphError, LoadError},
    io::arrow::{
        dataframe::{DFChunk, DFView},
        layer_col::{lift_layer_col, lift_node_type_col},
        prop_handler::*,
    },
    prelude::*,
    serialise::incremental::InternalCache,
};
use bytemuck::checked::cast_slice_mut;
#[cfg(feature = "python")]
use kdam::{Bar, BarBuilder, BarExt};
use raphtory_api::{
    atomic_extra::atomic_usize_from_mut_slice,
    core::{
        entities::{properties::prop::PropType, EID},
        storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
        Direction,
    },
};
use rayon::prelude::*;
use std::{collections::HashMap, sync::atomic::Ordering};

#[cfg(feature = "python")]
fn build_progress_bar(des: String, num_rows: usize) -> Result<Bar, GraphError> {
    BarBuilder::default()
        .desc(des)
        .animation(kdam::Animation::FillUp)
        .total(num_rows)
        .unit_scale(true)
        .build()
        .map_err(|_| GraphError::TqdmError)
}

fn process_shared_properties(
    props: Option<&HashMap<String, Prop>>,
    resolver: impl Fn(&str, PropType) -> Result<MaybeNew<usize>, GraphError>,
) -> Result<Vec<(usize, Prop)>, GraphError> {
    match props {
        None => Ok(vec![]),
        Some(props) => props
            .iter()
            .map(|(key, prop)| Ok((resolver(key, prop.dtype())?.inner(), prop.clone())))
            .collect(),
    }
}

pub(crate) fn load_nodes_from_df<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    time: &str,
    node_id: &str,
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
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

    let shared_metadata = process_shared_properties(shared_metadata, |key, dtype| {
        graph
            .resolve_node_property(key, dtype, true)
            .map_err(into_graph_err)
    })?;

    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading nodes".to_string(), df_view.num_rows)?;

    let mut node_col_resolved = vec![];
    let mut node_type_col_resolved = vec![];

    let cache = graph.get_cache();
    let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;
    let cache_shards = cache.map(|cache| {
        (0..write_locked_graph.num_shards())
            .map(|_| cache.fork())
            .collect::<Vec<_>>()
    });

    let mut start_id = graph
        .reserve_event_ids(df_view.num_rows)
        .map_err(into_graph_err)?;
    for chunk in df_view.chunks {
        let df = chunk?;
        let prop_cols = combine_properties(properties, &properties_indices, &df, |key, dtype| {
            graph
                .resolve_node_property(key, dtype, false)
                .map_err(into_graph_err)
        })?;
        let metadata_cols = combine_properties(metadata, &metadata_indices, &df, |key, dtype| {
            graph
                .resolve_node_property(key, dtype, true)
                .map_err(into_graph_err)
        })?;
        let node_type_col = lift_node_type_col(node_type, node_type_index, &df)?;

        let time_col = df.time_col(time_index)?;
        let node_col = df.node_col(node_id_index)?;

        node_col_resolved.resize_with(df.len(), Default::default);
        node_type_col_resolved.resize_with(df.len(), Default::default);

        node_col
            .par_iter()
            .zip(node_col_resolved.par_iter_mut())
            .zip(node_type_col.par_iter())
            .zip(node_type_col_resolved.par_iter_mut())
            .try_for_each(|(((gid, resolved), node_type), node_type_resolved)| {
                let gid = gid.ok_or(LoadError::FatalError)?;
                let vid = write_locked_graph
                    .resolve_node(gid)
                    .map_err(|_| LoadError::FatalError)?;
                let node_type_res = write_locked_graph.resolve_node_type(node_type).inner();
                *node_type_resolved = node_type_res;
                if let Some(cache) = cache {
                    cache.resolve_node(vid, gid);
                }
                *resolved = vid.inner();
                Ok::<(), LoadError>(())
            })?;

        let g = write_locked_graph.graph;
        let update_time = |time| g.update_time(time);

        write_locked_graph
            .nodes
            .resize(write_locked_graph.num_nodes());

        write_locked_graph
            .nodes
            .par_iter_mut()
            .try_for_each(|mut shard| {
                let mut t_props = vec![];
                let mut c_props = vec![];

                for (idx, (((vid, time), node_type), gid)) in node_col_resolved
                    .iter()
                    .zip(time_col.iter())
                    .zip(node_type_col_resolved.iter())
                    .zip(node_col.iter())
                    .enumerate()
                {
                    let shard_id = shard.shard_id();
                    let node_exists = if let Some(mut_node) = shard.get_mut(*vid) {
                        mut_node.init(*vid, gid);
                        mut_node.node_type = *node_type;
                        t_props.clear();
                        t_props.extend(prop_cols.iter_row(idx));

                        c_props.clear();
                        c_props.extend(metadata_cols.iter_row(idx));
                        c_props.extend_from_slice(&shared_metadata);

                        if let Some(caches) = cache_shards.as_ref() {
                            let cache = &caches[shard_id];
                            cache.add_node_update(
                                TimeIndexEntry(time, start_id + idx),
                                *vid,
                                &t_props,
                            );
                            cache.add_node_cprops(*vid, &c_props);
                        }

                        for (id, prop) in c_props.drain(..) {
                            mut_node.add_metadata(id, prop)?;
                        }

                        true
                    } else {
                        false
                    };

                    if node_exists {
                        let t = TimeIndexEntry(time, start_id + idx);
                        update_time(t);
                        let prop_i = shard.t_prop_log_mut().push(t_props.drain(..))?;
                        if let Some(mut_node) = shard.get_mut(*vid) {
                            mut_node.update_t_prop_time(t, prop_i);
                        }
                    }
                }
                Ok::<_, GraphError>(())
            })?;

        #[cfg(feature = "python")]
        let _ = pb.update(df.len());
        start_id += df.len();
    }
    Ok(())
}

pub fn load_edges_from_df<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    time: &str,
    src: &str,
    dst: &str,
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
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
    let layer_index = if let Some(layer_col) = layer_col {
        Some(df_view.get_index(layer_col.as_ref())?)
    } else {
        None
    };
    let shared_metadata = process_shared_properties(shared_metadata, |key, dtype| {
        graph
            .resolve_edge_property(key, dtype, true)
            .map_err(into_graph_err)
    })?;

    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading edges".to_string(), df_view.num_rows)?;
    #[cfg(feature = "python")]
    let _ = pb.update(0);
    let mut start_idx = graph
        .reserve_event_ids(df_view.num_rows)
        .map_err(into_graph_err)?;

    let mut src_col_resolved = vec![];
    let mut dst_col_resolved = vec![];
    let mut eid_col_resolved: Vec<EID> = vec![];

    let cache = graph.get_cache();
    let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;
    let cache_shards = cache.map(|cache| {
        (0..write_locked_graph.num_shards())
            .map(|_| cache.fork())
            .collect::<Vec<_>>()
    });

    for chunk in df_view.chunks {
        let df = chunk?;
        let prop_cols = combine_properties(properties, &properties_indices, &df, |key, dtype| {
            graph
                .resolve_edge_property(key, dtype, false)
                .map_err(into_graph_err)
        })?;
        let metadata_cols = combine_properties(metadata, &metadata_indices, &df, |key, dtype| {
            graph
                .resolve_edge_property(key, dtype, true)
                .map_err(into_graph_err)
        })?;
        let layer = lift_layer_col(layer, layer_index, &df)?;
        let layer_col_resolved = layer.resolve(graph)?;

        let src_col = df.node_col(src_index)?;
        src_col.validate(graph, LoadError::MissingSrcError)?;

        let dst_col = df.node_col(dst_index)?;
        dst_col.validate(graph, LoadError::MissingDstError)?;

        let time_col = df.time_col(time_index)?;

        // It's our graph, no one else can change it
        src_col_resolved.resize_with(df.len(), Default::default);
        src_col
            .par_iter()
            .zip(src_col_resolved.par_iter_mut())
            .try_for_each(|(gid, resolved)| {
                let gid = gid.ok_or(LoadError::FatalError)?;
                let vid = write_locked_graph
                    .resolve_node(gid)
                    .map_err(|_| LoadError::FatalError)?;
                if let Some(cache) = cache {
                    cache.resolve_node(vid, gid);
                }
                *resolved = vid.inner();
                Ok::<(), LoadError>(())
            })?;

        dst_col_resolved.resize_with(df.len(), Default::default);
        dst_col
            .par_iter()
            .zip(dst_col_resolved.par_iter_mut())
            .try_for_each(|(gid, resolved)| {
                let gid = gid.ok_or(LoadError::FatalError)?;
                let vid = write_locked_graph
                    .resolve_node(gid)
                    .map_err(|_| LoadError::FatalError)?;
                if let Some(cache) = cache {
                    cache.resolve_node(vid, gid);
                }
                *resolved = vid.inner();
                Ok::<(), LoadError>(())
            })?;

        write_locked_graph
            .nodes
            .resize(write_locked_graph.num_nodes());

        // resolve all the edges
        eid_col_resolved.resize_with(df.len(), Default::default);
        let eid_col_shared = atomic_usize_from_mut_slice(cast_slice_mut(&mut eid_col_resolved));
        let g = write_locked_graph.graph;
        let next_edge_id = || g.storage.edges.next_id();
        let update_time = |time| g.update_time(time);
        write_locked_graph
            .nodes
            .par_iter_mut()
            .for_each(|mut shard| {
                for (row, ((((src, src_gid), dst), time), layer)) in src_col_resolved
                    .iter()
                    .zip(src_col.iter())
                    .zip(dst_col_resolved.iter())
                    .zip(time_col.iter())
                    .zip(layer_col_resolved.iter())
                    .enumerate()
                {
                    let shard_id = shard.shard_id();
                    if let Some(src_node) = shard.get_mut(*src) {
                        src_node.init(*src, src_gid);
                        update_time(TimeIndexEntry(time, start_idx + row));
                        let eid = match src_node.find_edge_eid(*dst, &LayerIds::All) {
                            None => {
                                let eid = next_edge_id();
                                if let Some(cache_shards) = cache_shards.as_ref() {
                                    cache_shards[shard_id].resolve_edge(
                                        MaybeNew::New(eid),
                                        *src,
                                        *dst,
                                    );
                                }
                                eid
                            }
                            Some(eid) => eid,
                        };
                        src_node.update_time(
                            TimeIndexEntry(time, start_idx + row),
                            eid.with_layer(*layer),
                        );
                        src_node.add_edge(*dst, Direction::OUT, *layer, eid);
                        eid_col_shared[row].store(eid.0, Ordering::Relaxed);
                    }
                }
            });

        // link the destinations
        write_locked_graph
            .nodes
            .par_iter_mut()
            .for_each(|mut shard| {
                for (row, ((((src, (dst, dst_gid)), eid), time), layer)) in src_col_resolved
                    .iter()
                    .zip(dst_col_resolved.iter().zip(dst_col.iter()))
                    .zip(eid_col_resolved.iter())
                    .zip(time_col.iter())
                    .zip(layer_col_resolved.iter())
                    .enumerate()
                {
                    if let Some(node) = shard.get_mut(*dst) {
                        node.init(*dst, dst_gid);
                        node.update_time(
                            TimeIndexEntry(time, row + start_idx),
                            eid.with_layer(*layer),
                        );
                        node.add_edge(*src, Direction::IN, *layer, *eid)
                    }
                }
            });

        write_locked_graph
            .edges
            .par_iter_mut()
            .try_for_each(|mut shard| {
                let mut t_props = vec![];
                let mut c_props = vec![];
                for (idx, ((((src, dst), time), eid), layer)) in src_col_resolved
                    .iter()
                    .zip(dst_col_resolved.iter())
                    .zip(time_col.iter())
                    .zip(eid_col_resolved.iter())
                    .zip(layer_col_resolved.iter())
                    .enumerate()
                {
                    let shard_id = shard.shard_id();
                    if let Some(mut edge) = shard.get_mut(*eid) {
                        let edge_store = edge.edge_store_mut();
                        if !edge_store.initialised() {
                            edge_store.src = *src;
                            edge_store.dst = *dst;
                            edge_store.eid = *eid;
                        }
                        let t = TimeIndexEntry(time, start_idx + idx);
                        edge.additions_mut(*layer).insert(t);
                        t_props.clear();
                        t_props.extend(prop_cols.iter_row(idx));

                        c_props.clear();
                        c_props.extend(metadata_cols.iter_row(idx));
                        c_props.extend_from_slice(&shared_metadata);

                        if let Some(caches) = cache_shards.as_ref() {
                            let cache = &caches[shard_id];
                            cache.add_edge_update(t, *eid, &t_props, *layer);
                            cache.add_edge_cprops(*eid, *layer, &c_props);
                        }

                        if !t_props.is_empty() || !c_props.is_empty() {
                            let edge_layer = edge.layer_mut(*layer);

                            for (id, prop) in t_props.drain(..) {
                                edge_layer.add_prop(t, id, prop)?;
                            }

                            for (id, prop) in c_props.drain(..) {
                                edge_layer.update_metadata(id, prop)?;
                            }
                        }
                    }
                }
                Ok::<(), GraphError>(())
            })?;
        if let Some(cache) = cache {
            cache.write()?;
        }
        if let Some(cache_shards) = cache_shards.as_ref() {
            for cache in cache_shards {
                cache.write()?;
            }
        }

        start_idx += df.len();
        #[cfg(feature = "python")]
        let _ = pb.update(df.len());
    }
    Ok(())
}

pub(crate) fn load_edge_deletions_from_df<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + DeletionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    time: &str,
    src: &str,
    dst: &str,
    layer: Option<&str>,
    layer_col: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    let src_index = df_view.get_index(src)?;
    let dst_index = df_view.get_index(dst)?;
    let time_index = df_view.get_index(time)?;
    let layer_index = layer_col.map(|layer_col| df_view.get_index(layer_col.as_ref()));
    let layer_index = layer_index.transpose()?;
    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading edge deletions".to_string(), df_view.num_rows)?;
    let mut start_idx = graph
        .reserve_event_ids(df_view.num_rows)
        .map_err(into_graph_err)?;

    for chunk in df_view.chunks {
        let df = chunk?;
        let layer = lift_layer_col(layer, layer_index, &df)?;
        let src_col = df.node_col(src_index)?;
        let dst_col = df.node_col(dst_index)?;
        let time_col = df.time_col(time_index)?;
        src_col
            .par_iter()
            .zip(dst_col.par_iter())
            .zip(time_col.par_iter())
            .zip(layer.par_iter())
            .enumerate()
            .try_for_each(|(idx, (((src, dst), time), layer))| {
                let src = src.ok_or(LoadError::MissingSrcError)?;
                let dst = dst.ok_or(LoadError::MissingDstError)?;
                let time = time.ok_or(LoadError::MissingTimeError)?;
                graph.delete_edge((time, start_idx + idx), src, dst, layer)?;
                Ok::<(), GraphError>(())
            })?;
        #[cfg(feature = "python")]
        let _ = pb.update(df.len());
        start_idx += df.len();
    }

    Ok(())
}

pub(crate) fn load_node_props_from_df<
    'a,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    node_id: &str,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    graph: &G,
) -> Result<(), GraphError> {
    let metadata_indices = metadata
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;

    let node_type_index =
        node_type_col.map(|node_type_col| df_view.get_index(node_type_col.as_ref()));
    let node_type_index = node_type_index.transpose()?;

    let node_id_index = df_view.get_index(node_id)?;

    let shared_metadata = process_shared_properties(shared_metadata, |key, dtype| {
        graph
            .resolve_node_property(key, dtype, true)
            .map_err(into_graph_err)
    })?;

    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading node properties".to_string(), df_view.num_rows)?;

    let mut node_col_resolved = vec![];
    let mut node_type_col_resolved = vec![];

    let cache = graph.get_cache();
    let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;
    let cache_shards = cache.map(|cache| {
        (0..write_locked_graph.num_shards())
            .map(|_| cache.fork())
            .collect::<Vec<_>>()
    });

    for chunk in df_view.chunks {
        let df = chunk?;
        let metadata_cols = combine_properties(metadata, &metadata_indices, &df, |key, dtype| {
            graph
                .resolve_node_property(key, dtype, true)
                .map_err(into_graph_err)
        })?;
        let node_type_col = lift_node_type_col(node_type, node_type_index, &df)?;
        let node_col = df.node_col(node_id_index)?;

        node_col_resolved.resize_with(df.len(), Default::default);
        node_type_col_resolved.resize_with(df.len(), Default::default);

        node_col
            .par_iter()
            .zip(node_col_resolved.par_iter_mut())
            .zip(node_type_col.par_iter())
            .zip(node_type_col_resolved.par_iter_mut())
            .try_for_each(|(((gid, resolved), node_type), node_type_resolved)| {
                let gid = gid.ok_or(LoadError::FatalError)?;
                let vid = write_locked_graph
                    .resolve_node(gid)
                    .map_err(|_| LoadError::FatalError)?;
                let node_type_res = write_locked_graph.resolve_node_type(node_type).inner();
                *node_type_resolved = node_type_res;
                if let Some(cache) = cache {
                    cache.resolve_node(vid, gid);
                }
                *resolved = vid.inner();
                Ok::<(), LoadError>(())
            })?;

        write_locked_graph
            .nodes
            .resize(write_locked_graph.num_nodes());

        write_locked_graph
            .nodes
            .par_iter_mut()
            .try_for_each(|mut shard| {
                let mut c_props = vec![];

                for (idx, ((vid, node_type), gid)) in node_col_resolved
                    .iter()
                    .zip(node_type_col_resolved.iter())
                    .zip(node_col.iter())
                    .enumerate()
                {
                    let shard_id = shard.shard_id();
                    if let Some(mut_node) = shard.get_mut(*vid) {
                        mut_node.init(*vid, gid);
                        mut_node.node_type = *node_type;

                        c_props.clear();
                        c_props.extend(metadata_cols.iter_row(idx));
                        c_props.extend_from_slice(&shared_metadata);

                        if let Some(caches) = cache_shards.as_ref() {
                            let cache = &caches[shard_id];
                            cache.add_node_cprops(*vid, &c_props);
                        }

                        for (id, prop) in c_props.drain(..) {
                            mut_node.add_metadata(id, prop)?;
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

pub(crate) fn load_edges_props_from_df<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    src: &str,
    dst: &str,
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    let metadata_indices = metadata
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;

    let src_index = df_view.get_index(src)?;
    let dst_index = df_view.get_index(dst)?;
    let layer_index = if let Some(layer_col) = layer_col {
        Some(df_view.get_index(layer_col.as_ref())?)
    } else {
        None
    };
    let shared_metadata = process_shared_properties(shared_metadata, |key, dtype| {
        graph
            .resolve_edge_property(key, dtype, true)
            .map_err(into_graph_err)
    })?;

    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading edge properties".to_string(), df_view.num_rows)?;
    #[cfg(feature = "python")]
    let _ = pb.update(0);

    let mut src_col_resolved = vec![];
    let mut dst_col_resolved = vec![];
    let mut eid_col_resolved = vec![];

    let cache = graph.get_cache();
    let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;
    let cache_shards = cache.map(|cache| {
        (0..write_locked_graph.num_shards())
            .map(|_| cache.fork())
            .collect::<Vec<_>>()
    });

    let g = write_locked_graph.graph;

    for chunk in df_view.chunks {
        let df = chunk?;
        let metadata_cols = combine_properties(metadata, &metadata_indices, &df, |key, dtype| {
            graph
                .resolve_edge_property(key, dtype, true)
                .map_err(into_graph_err)
        })?;
        let layer = lift_layer_col(layer, layer_index, &df)?;
        let layer_col_resolved = layer.resolve(graph)?;

        let src_col = df.node_col(src_index)?;
        src_col.validate(graph, LoadError::MissingSrcError)?;

        let dst_col = df.node_col(dst_index)?;
        dst_col.validate(graph, LoadError::MissingDstError)?;

        // It's our graph, no one else can change it
        src_col_resolved.resize_with(df.len(), Default::default);
        src_col
            .par_iter()
            .zip(src_col_resolved.par_iter_mut())
            .try_for_each(|(gid, resolved)| {
                let gid = gid.ok_or(LoadError::FatalError)?;
                let vid = g
                    .resolve_node_ref(gid.as_node_ref())
                    .ok_or(LoadError::MissingNodeError)?;
                *resolved = vid;
                Ok::<(), LoadError>(())
            })?;

        dst_col_resolved.resize_with(df.len(), Default::default);
        dst_col
            .par_iter()
            .zip(dst_col_resolved.par_iter_mut())
            .try_for_each(|(gid, resolved)| {
                let gid = gid.ok_or(LoadError::FatalError)?;
                let vid = g
                    .resolve_node_ref(gid.as_node_ref())
                    .ok_or(LoadError::MissingNodeError)?;
                *resolved = vid;
                Ok::<(), LoadError>(())
            })?;

        // resolve all the edges
        eid_col_resolved.resize_with(df.len(), Default::default);
        let eid_col_shared = atomic_usize_from_mut_slice(cast_slice_mut(&mut eid_col_resolved));
        write_locked_graph
            .nodes
            .par_iter_mut()
            .try_for_each(|shard| {
                for (row, (src, dst)) in src_col_resolved
                    .iter()
                    .zip(dst_col_resolved.iter())
                    .enumerate()
                {
                    if let Some(src_node) = shard.get(*src) {
                        // we know this is here
                        let EID(eid) = src_node
                            .find_edge_eid(*dst, &LayerIds::All)
                            .ok_or(LoadError::MissingEdgeError(*src, *dst))?;
                        eid_col_shared[row].store(eid, Ordering::Relaxed);
                    }
                }
                Ok::<_, LoadError>(())
            })?;

        write_locked_graph
            .edges
            .par_iter_mut()
            .try_for_each(|mut shard| {
                let mut c_props = vec![];
                for (idx, (eid, layer)) in eid_col_resolved
                    .iter()
                    .zip(layer_col_resolved.iter())
                    .enumerate()
                {
                    let shard_id = shard.shard_id();
                    if let Some(mut edge) = shard.get_mut(*eid) {
                        c_props.clear();
                        c_props.extend(metadata_cols.iter_row(idx));
                        c_props.extend_from_slice(&shared_metadata);

                        if let Some(caches) = cache_shards.as_ref() {
                            let cache = &caches[shard_id];
                            cache.add_edge_cprops(*eid, *layer, &c_props);
                        }

                        if !c_props.is_empty() {
                            let edge_layer = edge.layer_mut(*layer);

                            for (id, prop) in c_props.drain(..) {
                                edge_layer.update_metadata(id, prop)?;
                            }
                        }
                    }
                }
                Ok::<(), GraphError>(())
            })?;

        if let Some(cache) = cache {
            cache.write()?;
        }
        if let Some(cache_shards) = cache_shards.as_ref() {
            for cache in cache_shards {
                cache.write()?;
            }
        }

        #[cfg(feature = "python")]
        let _ = pb.update(df.len());
    }
    Ok(())
}

pub(crate) fn load_graph_props_from_df<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    time: &str,
    properties: Option<&[&str]>,
    metadata: Option<&[&str]>,
    graph: &G,
) -> Result<(), GraphError> {
    let properties = properties.unwrap_or(&[]);
    let metadata = metadata.unwrap_or(&[]);

    let properties_indices = properties
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;
    let metadata_indices = metadata
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;

    let time_index = df_view.get_index(time)?;

    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading graph properties".to_string(), df_view.num_rows)?;

    let mut start_id = graph
        .reserve_event_ids(df_view.num_rows)
        .map_err(into_graph_err)?;

    for chunk in df_view.chunks {
        let df = chunk?;
        let prop_cols = combine_properties(properties, &properties_indices, &df, |key, dtype| {
            graph
                .resolve_graph_property(key, dtype, false)
                .map_err(into_graph_err)
        })?;
        let metadata_cols = combine_properties(metadata, &metadata_indices, &df, |key, dtype| {
            graph
                .resolve_graph_property(key, dtype, true)
                .map_err(into_graph_err)
        })?;
        let time_col = df.time_col(time_index)?;

        time_col
            .par_iter()
            .zip(prop_cols.par_rows())
            .zip(metadata_cols.par_rows())
            .enumerate()
            .try_for_each(|(id, ((time, t_props), c_props))| {
                let time = time.ok_or(LoadError::MissingTimeError)?;
                let t = TimeIndexEntry(time, start_id + id);
                let t_props: Vec<_> = t_props.collect();
                if !t_props.is_empty() {
                    graph
                        .internal_add_properties(t, &t_props)
                        .map_err(into_graph_err)?;
                }

                let c_props: Vec<_> = c_props.collect();

                if !c_props.is_empty() {
                    graph
                        .internal_add_metadata(&c_props)
                        .map_err(into_graph_err)?;
                }
                Ok::<(), GraphError>(())
            })?;
        #[cfg(feature = "python")]
        let _ = pb.update(df.len());
        start_id += df.len();
    }
    Ok(())
}
