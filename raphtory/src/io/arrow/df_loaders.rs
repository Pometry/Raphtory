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
use bzip2::write;
#[cfg(feature = "python")]
use kdam::{Bar, BarBuilder, BarExt};
use raphtory_api::{
    atomic_extra::atomic_usize_from_mut_slice,
    core::{
        entities::{properties::prop::PropType, EID},
        storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
    },
};
use raphtory_core::{entities::graph::timer::TimeCounterTrait, storage::timeindex::AsTime};
use raphtory_storage::mutation::addition_ops::SessionAdditionOps;
use rayon::prelude::*;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

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
    constant_properties: &[&str],
    shared_constant_properties: Option<&HashMap<String, Prop>>,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    let properties_indices = properties
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;
    let constant_properties_indices = constant_properties
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;

    let node_type_index =
        node_type_col.map(|node_type_col| df_view.get_index(node_type_col.as_ref()));
    let node_type_index = node_type_index.transpose()?;

    let node_id_index = df_view.get_index(node_id)?;
    let time_index = df_view.get_index(time)?;

    let session = graph.write_session().map_err(into_graph_err)?;
    let shared_constant_properties =
        process_shared_properties(shared_constant_properties, |key, dtype| {
            session
                .resolve_node_property(key, dtype, true)
                .map_err(into_graph_err)
        })?;

    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading nodes".to_string(), df_view.num_rows)?;

    let mut node_col_resolved = vec![];
    let mut node_type_col_resolved = vec![];

    let cache = graph.get_cache();
    let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;

    let mut start_id = session
        .reserve_event_ids(df_view.num_rows)
        .map_err(into_graph_err)?;
    for chunk in df_view.chunks {
        let df = chunk?;
        let prop_cols = combine_properties(properties, &properties_indices, &df, |key, dtype| {
            session
                .resolve_node_property(key, dtype, false)
                .map_err(into_graph_err)
        })?;
        let const_prop_cols = combine_properties(
            constant_properties,
            &constant_properties_indices,
            &df,
            |key, dtype| {
                session
                    .resolve_node_property(key, dtype, true)
                    .map_err(into_graph_err)
            },
        )?;
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


        let (earliest, latest) = write_locked_graph.earliest_latest();
        let update_time = |time:TimeIndexEntry| { let time = time.t(); earliest.update(time); latest.update(time); };

        write_locked_graph.resize_chunks_to_num_nodes();

        write_locked_graph
            .nodes
            .par_iter_mut()
            .try_for_each(|shard| {
                let mut t_props = vec![];
                let mut c_props = vec![];

                for (idx, (((vid, time), node_type), gid)) in node_col_resolved
                    .iter()
                    .zip(time_col.iter())
                    .zip(node_type_col_resolved.iter())
                    .zip(node_col.iter())
                    .enumerate()
                {
                    if let Some(mut_node) = shard.resolve_pos(*vid) {
                        let mut writer = shard.writer();
                        writer.store_node_id_and_node_type(mut_node, 0, gid, *node_type, 0);
                        t_props.clear();
                        t_props.extend(prop_cols.iter_row(idx));

                        c_props.clear();
                        c_props.extend(const_prop_cols.iter_row(idx));
                        c_props.extend_from_slice(&shared_constant_properties);
                        writer.update_c_props(mut_node, 0, c_props.drain(..), 0);
                        writer.add_props(
                            TimeIndexEntry(time, start_id + idx),
                            mut_node,
                            0,
                            t_props.drain(..),
                            0,
                        );
                    };
                }
                Ok::<_, GraphError>(())
            })?;

        #[cfg(feature = "python")]
        let _ = pb.update(df.len());
        start_id += df.len();
    }
    Ok(())
}

pub(crate) fn load_edges_from_df<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    time: &str,
    src: &str,
    dst: &str,
    properties: &[&str],
    constant_properties: &[&str],
    shared_constant_properties: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    let properties_indices = properties
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;
    let constant_properties_indices = constant_properties
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
    let session = graph.write_session().map_err(into_graph_err)?;
    let shared_constant_properties =
        process_shared_properties(shared_constant_properties, |key, dtype| {
            session
                .resolve_edge_property(key, dtype, true)
                .map_err(into_graph_err)
        })?;

    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading edges".to_string(), df_view.num_rows)?;
    #[cfg(feature = "python")]
    let _ = pb.update(0);
    let mut start_idx = session
        .reserve_event_ids(df_view.num_rows)
        .map_err(into_graph_err)?;

    let mut src_col_resolved = vec![];
    let mut dst_col_resolved = vec![];
    let mut eid_col_resolved: Vec<EID> = vec![];
    let mut eids_exist: Vec<AtomicBool> = vec![]; // exists or needs to be created

    let cache = graph.get_cache();
    let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;

    for chunk in df_view.chunks {
        let df = chunk?;
        let prop_cols = combine_properties(properties, &properties_indices, &df, |key, dtype| {
            session
                .resolve_edge_property(key, dtype, false)
                .map_err(into_graph_err)
        })?;
        let const_prop_cols = combine_properties(
            constant_properties,
            &constant_properties_indices,
            &df,
            |key, dtype| {
                session
                    .resolve_edge_property(key, dtype, true)
                    .map_err(into_graph_err)
            },
        )?;
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

        write_locked_graph.resize_chunks_to_num_nodes();

        // resolve all the edges
        eid_col_resolved.resize_with(df.len(), Default::default);
        eids_exist.resize_with(df.len(), Default::default);
        let eid_col_shared = atomic_usize_from_mut_slice(cast_slice_mut(&mut eid_col_resolved));

        let next_edge_id: Arc<AtomicUsize> = write_locked_graph.num_edges.clone();
        let next_edge_id = || {next_edge_id.fetch_add(1, Ordering::Relaxed)};

        let (earliest, latest) = write_locked_graph.earliest_latest();
        let update_time = |time:TimeIndexEntry| { let time = time.t(); earliest.update(time); latest.update(time); };
        write_locked_graph
            .nodes
            .par_iter_mut()
            .for_each(|locked_page| {
                for (row, ((((src, src_gid), dst), time), layer)) in src_col_resolved
                    .iter()
                    .zip(src_col.iter())
                    .zip(dst_col_resolved.iter())
                    .zip(time_col.iter())
                    .zip(layer_col_resolved.iter())
                    .enumerate()
                {
                    if let Some(src_pos) = locked_page.resolve_pos(*src) {
                        let t = TimeIndexEntry(time, start_idx + row);
                        update_time(t);
                        let mut writer = locked_page.writer();
                        writer.store_node_id(src_pos, 0, src_gid, 0);
                        if let Some(edge_id) = writer.get_out_edge(src_pos, *dst, 0) {
                            eid_col_shared[row].store(edge_id.0, Ordering::Relaxed);
                            eids_exist[row].store(true, Ordering::Relaxed);
                        } else {
                            let edge_id = EID(next_edge_id());
                            writer.add_static_outbound_edge(
                                src_pos,
                                *dst,
                                edge_id.with_layer(*layer),
                                0,
                            );
                            writer.add_outbound_edge(
                                t,
                                src_pos,
                                *dst,
                                edge_id.with_layer(*layer),
                                0,
                            ); // FIXME: when we update this to work with layers use the correct layer
                            eid_col_shared[row].store(edge_id.0, Ordering::Relaxed);
                            eids_exist[row].store(false, Ordering::Relaxed);
                        }
                    }
                }
            });

        // link the destinations
        write_locked_graph
            .nodes
            .par_iter_mut()
            .for_each(|shard| {
                for (row, ((((src, (dst, dst_gid)), eid), time), layer)) in src_col_resolved
                    .iter()
                    .zip(dst_col_resolved.iter().zip(dst_col.iter()))
                    .zip(eid_col_resolved.iter())
                    .zip(time_col.iter())
                    .zip(layer_col_resolved.iter())
                    .enumerate()
                {
                    if let Some(dst_pos) = shard.resolve_pos(*dst) {
                        let t = TimeIndexEntry(time, start_idx + row);
                        let mut writer = shard.writer();
                        writer.store_node_id(dst_pos, 0, dst_gid, 0);
                        writer.add_static_inbound_edge(dst_pos, *src, eid.with_layer(*layer), 0);
                        writer.add_inbound_edge(t, dst_pos, *src, eid.with_layer(*layer), 0);
                    }
                }
            });

        write_locked_graph.resize_chunks_to_num_edges();

        write_locked_graph
            .edges
            .par_iter_mut()
            .try_for_each(|shard| {
                let mut t_props = vec![];
                let mut c_props = vec![];
                for (idx, (((((src, dst), time), eid), layer), exists)) in src_col_resolved
                    .iter()
                    .zip(dst_col_resolved.iter())
                    .zip(time_col.iter())
                    .zip(eid_col_resolved.iter())
                    .zip(layer_col_resolved.iter()).zip(eids_exist.iter().map(|exists| exists.load(Ordering::Relaxed)))
                    .enumerate()
                {
                    if let Some(eid_pos) = shard.resolve_pos(*eid) {
                        let t = TimeIndexEntry(time, start_idx + idx);
                        let mut writer = shard.writer();

                        t_props.clear();
                        t_props.extend(prop_cols.iter_row(idx));

                        c_props.clear();
                        c_props.extend(const_prop_cols.iter_row(idx));
                        c_props.extend_from_slice(&shared_constant_properties);

                        writer.update_c_props(eid_pos, *src, *dst, *layer, c_props.drain(..));
                        writer.add_edge(t, Some(eid_pos), *src, *dst, t_props.drain(..), *layer, 0, Some(exists));

                    }
                }
                Ok::<(), GraphError>(())
            })?;
        if let Some(cache) = cache {
            cache.write()?;
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
    let session = graph.write_session().map_err(into_graph_err)?;
    let mut start_idx = session
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
    constant_properties: &[&str],
    shared_constant_properties: Option<&HashMap<String, Prop>>,
    graph: &G,
) -> Result<(), GraphError> {
    let constant_properties_indices = constant_properties
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;

    let node_type_index =
        node_type_col.map(|node_type_col| df_view.get_index(node_type_col.as_ref()));
    let node_type_index = node_type_index.transpose()?;

    let node_id_index = df_view.get_index(node_id)?;
    let session = graph.write_session().map_err(into_graph_err)?;

    let shared_constant_properties =
        process_shared_properties(shared_constant_properties, |key, dtype| {
            session
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
        let const_prop_cols = combine_properties(
            constant_properties,
            &constant_properties_indices,
            &df,
            |key, dtype| {
                session
                    .resolve_node_property(key, dtype, true)
                    .map_err(into_graph_err)
            },
        )?;
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
                        c_props.extend(const_prop_cols.iter_row(idx));
                        c_props.extend_from_slice(&shared_constant_properties);

                        if let Some(caches) = cache_shards.as_ref() {
                            let cache = &caches[shard_id];
                            cache.add_node_cprops(*vid, &c_props);
                        }

                        for (id, prop) in c_props.drain(..) {
                            mut_node.add_constant_prop(id, prop)?;
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
    constant_properties: &[&str],
    shared_const_properties: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    let constant_properties_indices = constant_properties
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
    let session = graph.write_session().map_err(into_graph_err)?;
    let shared_constant_properties =
        process_shared_properties(shared_const_properties, |key, dtype| {
            session
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
        let const_prop_cols = combine_properties(
            constant_properties,
            &constant_properties_indices,
            &df,
            |key, dtype| {
                session
                    .resolve_edge_property(key, dtype, true)
                    .map_err(into_graph_err)
            },
        )?;
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
                        c_props.extend(const_prop_cols.iter_row(idx));
                        c_props.extend_from_slice(&shared_constant_properties);

                        if let Some(caches) = cache_shards.as_ref() {
                            let cache = &caches[shard_id];
                            cache.add_edge_cprops(*eid, *layer, &c_props);
                        }

                        if !c_props.is_empty() {
                            let edge_layer = edge.layer_mut(*layer);

                            for (id, prop) in c_props.drain(..) {
                                edge_layer.update_constant_prop(id, prop)?;
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
    constant_properties: Option<&[&str]>,
    graph: &G,
) -> Result<(), GraphError> {
    let properties = properties.unwrap_or(&[]);
    let constant_properties = constant_properties.unwrap_or(&[]);

    let properties_indices = properties
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;
    let constant_properties_indices = constant_properties
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;

    let time_index = df_view.get_index(time)?;

    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading graph properties".to_string(), df_view.num_rows)?;
    let session = graph.write_session().map_err(into_graph_err)?;

    let mut start_id = session
        .reserve_event_ids(df_view.num_rows)
        .map_err(into_graph_err)?;

    for chunk in df_view.chunks {
        let df = chunk?;
        let prop_cols = combine_properties(properties, &properties_indices, &df, |key, dtype| {
            session
                .resolve_graph_property(key, dtype, false)
                .map_err(into_graph_err)
        })?;
        let const_prop_cols = combine_properties(
            constant_properties,
            &constant_properties_indices,
            &df,
            |key, dtype| {
                session
                    .resolve_graph_property(key, dtype, true)
                    .map_err(into_graph_err)
            },
        )?;
        let time_col = df.time_col(time_index)?;

        time_col
            .par_iter()
            .zip(prop_cols.par_rows())
            .zip(const_prop_cols.par_rows())
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
                        .internal_add_constant_properties(&c_props)
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

#[cfg(test)]
mod tests {
    use crate::{
        db::graph::graph::assert_graph_equal,
        errors::GraphError,
        io::arrow::{
            dataframe::{DFChunk, DFView},
            df_loaders::load_edges_from_df,
        },
        prelude::*,
        test_utils::build_edge_list,
    };
    use itertools::Itertools;
    use polars_arrow::array::{MutableArray, MutablePrimitiveArray, MutableUtf8Array};
    use proptest::proptest;
    use tempfile::TempDir;

    #[cfg(feature = "storage")]
    mod load_multi_layer {
        use std::{
            fs::File,
            path::{Path, PathBuf},
        };

        use crate::{
            db::graph::graph::assert_graph_equal, io::parquet_loaders::load_edges_from_parquet,
            prelude::Graph, test_utils::build_edge_list,
        };
        use polars_arrow::{
            array::{PrimitiveArray, Utf8Array},
            types::NativeType,
        };
        use polars_core::{frame::DataFrame, prelude::*};
        use polars_io::prelude::{ParquetCompression, ParquetWriter};
        use pometry_storage::{graph::TemporalGraph, load::ExternalEdgeList};
        use prop::sample::SizeRange;
        use proptest::prelude::*;
        use raphtory_storage::{disk::DiskGraphStorage, graph::graph::GraphStorage};
        use tempfile::TempDir;

        fn build_edge_list_df(
            len: usize,
            num_nodes: impl Strategy<Value = u64>,
            num_layers: impl Into<SizeRange>,
        ) -> impl Strategy<Value = Vec<DataFrame>> {
            let layer = num_nodes
                .prop_flat_map(move |num_nodes| {
                    build_edge_list(len, num_nodes)
                        .prop_filter("no empty edge lists", |el| !el.is_empty())
                })
                .prop_map(move |mut rows| {
                    rows.sort_by_key(|(src, dst, time, _, _)| (*src, *dst, *time));
                    new_df_from_rows(&rows)
                });
            proptest::collection::vec(layer, num_layers)
        }

        fn new_df_from_rows(rows: &[(u64, u64, i64, String, i64)]) -> DataFrame {
            let src = native_series("src", rows.iter().map(|(src, _, _, _, _)| *src));
            let dst = native_series("dst", rows.iter().map(|(_, dst, _, _, _)| *dst));
            let time = native_series("time", rows.iter().map(|(_, _, time, _, _)| *time));
            let int_prop = native_series(
                "int_prop",
                rows.iter().map(|(_, _, _, _, int_prop)| *int_prop),
            );

            let str_prop = Series::from_arrow(
                "str_prop",
                Utf8Array::<i64>::from_iter(
                    rows.iter()
                        .map(|(_, _, _, str_prop, _)| Some(str_prop.clone())),
                )
                .boxed(),
            )
            .unwrap();

            DataFrame::new(vec![src, dst, time, str_prop, int_prop]).unwrap()
        }

        fn native_series<T: NativeType>(name: &str, is: impl IntoIterator<Item = T>) -> Series {
            let is = PrimitiveArray::from_vec(is.into_iter().collect());
            Series::from_arrow(name, is.boxed()).unwrap()
        }

        fn check_layers_from_df(input: Vec<DataFrame>, num_threads: usize) {
            let root_dir = TempDir::new().unwrap();
            let graph_dir = TempDir::new().unwrap();
            let layers = input
                .into_iter()
                .enumerate()
                .map(|(i, df)| (i.to_string(), df))
                .collect::<Vec<_>>();
            let edge_lists = write_layers(&layers, root_dir.path());

            let expected = Graph::new();
            for edge_list in &edge_lists {
                load_edges_from_parquet(
                    &expected,
                    &edge_list.path,
                    "time",
                    "src",
                    "dst",
                    &["int_prop", "str_prop"],
                    &[],
                    None,
                    Some(edge_list.layer),
                    None,
                )
                .unwrap();
            }

            let g = TemporalGraph::from_parquets(
                num_threads,
                13,
                23,
                graph_dir.path(),
                edge_lists,
                &[],
                None,
                None,
                None,
            )
            .unwrap();
            let actual =
                Graph::from_internal_graph(GraphStorage::Disk(DiskGraphStorage::new(g).into()));

            assert_graph_equal(&expected, &actual);

            let g = TemporalGraph::new(graph_dir.path()).unwrap();

            for edge in g.edges_iter() {
                assert!(g.find_edge(edge.src_id(), edge.dst_id()).is_some());
            }

            let actual =
                Graph::from_internal_graph(GraphStorage::Disk(DiskGraphStorage::new(g).into()));
            assert_graph_equal(&expected, &actual);
        }

        #[test]
        fn load_from_multiple_layers() {
            proptest!(|(input in build_edge_list_df(50, 1u64..23, 1..10,  ), num_threads in 1usize..2)| {
                check_layers_from_df(input, num_threads)
            });
        }

        #[test]
        fn single_layer_single_edge() {
            let df = new_df_from_rows(&[(0, 0, 1, "".to_owned(), 2)]);
            check_layers_from_df(vec![df], 1)
        }

        fn write_layers<'a>(
            layers: &'a [(String, DataFrame)],
            root_dir: &Path,
        ) -> Vec<ExternalEdgeList<'a, PathBuf>> {
            let mut paths = vec![];
            for (name, df) in layers.iter() {
                let layer_dir = root_dir.join(name);
                std::fs::create_dir_all(&layer_dir).unwrap();
                let layer_path = layer_dir.join("edges.parquet");

                paths.push(
                    ExternalEdgeList::new(
                        name,
                        layer_path.to_path_buf(),
                        "src",
                        "dst",
                        "time",
                        vec![],
                    )
                    .unwrap(),
                );

                let file = File::create(layer_path).unwrap();
                let mut df = df.clone();
                ParquetWriter::new(file)
                    .with_compression(ParquetCompression::Snappy)
                    .finish(&mut df)
                    .unwrap();
            }
            paths
        }
    }

    fn build_df(
        chunk_size: usize,
        edges: &[(u64, u64, i64, String, i64)],
    ) -> DFView<impl Iterator<Item = Result<DFChunk, GraphError>>> {
        let chunks = edges.iter().chunks(chunk_size);
        let chunks = chunks
            .into_iter()
            .map(|chunk| {
                let mut src_col = MutablePrimitiveArray::new();
                let mut dst_col = MutablePrimitiveArray::new();
                let mut time_col = MutablePrimitiveArray::new();
                let mut str_prop_col = MutableUtf8Array::<i64>::new();
                let mut int_prop_col = MutablePrimitiveArray::new();
                for (src, dst, time, str_prop, int_prop) in chunk {
                    src_col.push_value(*src);
                    dst_col.push_value(*dst);
                    time_col.push_value(*time);
                    str_prop_col.push(Some(str_prop));
                    int_prop_col.push_value(*int_prop);
                }
                let chunk = vec![
                    src_col.as_box(),
                    dst_col.as_box(),
                    time_col.as_box(),
                    str_prop_col.as_box(),
                    int_prop_col.as_box(),
                ];
                Ok(DFChunk { chunk })
            })
            .collect_vec();
        DFView {
            names: vec![
                "src".to_owned(),
                "dst".to_owned(),
                "time".to_owned(),
                "str_prop".to_owned(),
                "int_prop".to_owned(),
            ],
            chunks: chunks.into_iter(),
            num_rows: edges.len(),
        }
    }
    #[test]
    fn test_load_edges() {
        proptest!(|(edges in build_edge_list(1000, 100), chunk_size in 1usize..=1000)| {
            let df_view = build_df(chunk_size, &edges);
            let g = Graph::new();
            let props = ["str_prop", "int_prop"];
            load_edges_from_df(df_view, "time", "src", "dst", &props, &[], None, None, None, &g).unwrap();
            let g2 = Graph::new();
            for (src, dst, time, str_prop, int_prop) in edges {
                g2.add_edge(time, src, dst, [("str_prop", str_prop.clone().into_prop()), ("int_prop", int_prop.into_prop())], None).unwrap();
                let edge = g.edge(src, dst).unwrap().at(time);
                assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
                assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);
            }
            assert_graph_equal(&g, &g2);
        })
    }

    #[test]
    fn test_load_edges_with_cache() {
        proptest!(|(edges in build_edge_list(100, 100), chunk_size in 1usize..=100)| {
            let df_view = build_df(chunk_size, &edges);
            let g = Graph::new();
            let cache_file = TempDir::new().unwrap();
            g.cache(cache_file.path()).unwrap();
            let props = ["str_prop", "int_prop"];
            load_edges_from_df(df_view, "time", "src", "dst", &props, &[], None, None, None, &g).unwrap();
            let g = Graph::load_cached(cache_file.path()).unwrap();
            let g2 = Graph::new();
            for (src, dst, time, str_prop, int_prop) in edges {
                g2.add_edge(time, src, dst, [("str_prop", str_prop.clone().into_prop()), ("int_prop", int_prop.into_prop())], None).unwrap();
                let edge = g.edge(src, dst).unwrap().at(time);
                assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
                assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);
            }
            assert_graph_equal(&g, &g2);
        })
    }

    #[test]
    fn load_single_edge_with_cache() {
        let edges = [(0, 0, 0, "".to_string(), 0)];
        let df_view = build_df(1, &edges);
        let g = Graph::new();
        let cache_file = TempDir::new().unwrap();
        g.cache(cache_file.path()).unwrap();
        let props = ["str_prop", "int_prop"];
        load_edges_from_df(
            df_view,
            "time",
            "src",
            "dst",
            &props,
            &[],
            None,
            None,
            None,
            &g,
        )
        .unwrap();
        let g = Graph::load_cached(cache_file.path()).unwrap();
        let g2 = Graph::new();
        for (src, dst, time, str_prop, int_prop) in edges {
            g2.add_edge(
                time,
                src,
                dst,
                [
                    ("str_prop", str_prop.clone().into_prop()),
                    ("int_prop", int_prop.into_prop()),
                ],
                None,
            )
            .unwrap();
            let edge = g.edge(src, dst).unwrap().at(time);
            assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
            assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);
        }
        assert_graph_equal(&g, &g2);
    }
}
