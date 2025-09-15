use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
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
use either::Either;
use kdam::{Bar, BarBuilder, BarExt};
use raphtory_api::{
    atomic_extra::atomic_usize_from_mut_slice,
    core::{
        entities::{properties::prop::PropType, EID},
        storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
    },
};
use raphtory_core::{
    entities::{graph::logical_to_physical::ResolverShardT, GidRef, VID},
    storage::timeindex::AsTime,
};
use raphtory_storage::mutation::addition_ops::{InternalAdditionOps, SessionAdditionOps};
use rayon::prelude::*;
use std::{
    borrow::{Borrow, Cow},
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

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
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache + std::fmt::Debug,
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
        let metadata_cols = combine_properties(metadata, &metadata_indices, &df, |key, dtype| {
            session
                .resolve_node_property(key, dtype, true)
                .map_err(into_graph_err)
        })?;
        let node_type_col = lift_node_type_col(node_type, node_type_index, &df)?;

        let time_col = df.time_col(time_index)?;
        let node_col = df.node_col(node_id_index)?;

        node_col_resolved.resize_with(df.len(), Default::default);
        node_type_col_resolved.resize_with(df.len(), Default::default);

        // TODO: Using parallel iterators results in a 5x speedup, but
        // needs to be implemented such that node VID order is preserved.
        // See: https://github.com/Pometry/pometry-storage/issues/81
        for (((gid, resolved), node_type), node_type_resolved) in node_col
            .iter()
            .zip(node_col_resolved.iter_mut())
            .zip(node_type_col.iter())
            .zip(node_type_col_resolved.iter_mut())
        {
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
                        let t = TimeIndexEntry(time, start_id + idx);
                        update_time(t);
                        let mut writer = shard.writer();
                        writer.store_node_id_and_node_type(mut_node, 0, gid, *node_type, 0);
                        t_props.clear();
                        t_props.extend(prop_cols.iter_row(idx));

                        c_props.clear();
                        c_props.extend(metadata_cols.iter_row(idx));
                        c_props.extend_from_slice(&shared_metadata);
                        writer.update_c_props(mut_node, 0, c_props.drain(..), 0);
                        writer.add_props(t, mut_node, 0, t_props.drain(..), 0);
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
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
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

    let src_index = df_view.get_index(src)?;
    let dst_index = df_view.get_index(dst)?;
    let time_index = df_view.get_index(time)?;
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

    let mut pb = build_progress_bar("Loading edges".to_string(), df_view.num_rows)?;
    let _ = pb.update(0);
    let mut start_idx = session
        .reserve_event_ids(df_view.num_rows)
        .map_err(into_graph_err)?;

    let mut src_col_resolved = vec![];
    let mut dst_col_resolved = vec![];
    let mut eid_col_resolved: Vec<EID> = vec![];
    let mut eids_exist: Vec<AtomicBool> = vec![]; // exists or needs to be created

    let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;

    // set the type of the resolver;
    let chunks = df_view.chunks.peekable();
    // let mapping = Mapping::new();
    // let mapping = write_locked_graph.graph().logical_to_physical.clone();

    // if let Some(chunk) = chunks.peek() {
    //     if let Ok(chunk) = chunk {
    //         let src_col = chunk.node_col(src_index)?;
    //         let dst_col = chunk.node_col(dst_index)?;
    //         src_col.validate(graph, LoadError::MissingSrcError)?;
    //         dst_col.validate(graph, LoadError::MissingDstError)?;
    //         let mut iter = src_col.iter();

    //         if let Some(id) = iter.next() {
    //             let vid = graph
    //                 .resolve_node(id.as_node_ref())
    //                 .map_err(|_| LoadError::FatalError)?; // initialize the type of the resolver
    //             mapping
    //                 .set(id, vid.inner())
    //                 .map_err(|_| LoadError::FatalError)?;
    //         } else {
    //             return Ok(());
    //         }
    //     }
    // } else {
    //     return Ok(());
    // }

    let num_nodes = AtomicUsize::new(write_locked_graph.graph().internal_num_nodes());

    for chunk in chunks {
        let df = chunk?;
        let prop_cols = combine_properties(properties, &properties_indices, &df, |key, dtype| {
            session
                .resolve_edge_property(key, dtype, false)
                .map_err(into_graph_err)
        })?;
        let metadata_cols = combine_properties(metadata, &metadata_indices, &df, |key, dtype| {
            session
                .resolve_edge_property(key, dtype, true)
                .map_err(into_graph_err)
        })?;

        src_col_resolved.resize_with(df.len(), Default::default);
        dst_col_resolved.resize_with(df.len(), Default::default);

        // let src_col_shared = atomic_usize_from_mut_slice(cast_slice_mut(&mut src_col_resolved));
        // let dst_col_shared = atomic_usize_from_mut_slice(cast_slice_mut(&mut dst_col_resolved));

        let layer = lift_layer_col(layer, layer_index, &df)?;
        let layer_col_resolved = layer.resolve(graph)?;

        let src_col = df.node_col(src_index)?;
        src_col.validate(graph, LoadError::MissingSrcError)?;

        let dst_col = df.node_col(dst_index)?;
        dst_col.validate(graph, LoadError::MissingDstError)?;

        // let gid_type = src_col.dtype();

        // let fallback_resolver = write_locked_graph.graph().logical_to_physical.clone();

        // mapping
        //     .mapping()
        //     .run_with_locked(|mut shard| match gid_type {
        //         GidType::Str => load_into_shard(
        //             src_col_shared,
        //             dst_col_shared,
        //             &src_col,
        //             &dst_col,
        //             &num_nodes,
        //             shard.as_str().unwrap(),
        //             |gid| Cow::Borrowed(gid.as_str().unwrap()),
        //             |id| fallback_resolver.get_str(id),
        //         ),
        //         GidType::U64 => load_into_shard(
        //             src_col_shared,
        //             dst_col_shared,
        //             &src_col,
        //             &dst_col,
        //             &num_nodes,
        //             shard.as_u64().unwrap(),
        //             |gid| Cow::Owned(gid.as_u64().unwrap()),
        //             |id| fallback_resolver.get_u64(*id),
        //         ),
        //     })?;

        // It's our graph, no one else can change it
        src_col
            .par_iter()
            .zip(src_col_resolved.par_iter_mut())
            .try_for_each(|(gid, resolved)| {
                let gid = gid.ok_or(LoadError::FatalError)?;
                let vid = write_locked_graph
                    .graph()
                    .resolve_node(gid.as_node_ref())
                    .map_err(|_| LoadError::FatalError)?;

                if vid.is_new() {
                    num_nodes.fetch_add(1, Ordering::Relaxed);
                }

                *resolved = vid.inner();
                Ok::<(), LoadError>(())
            })?;

        dst_col
            .par_iter()
            .zip(dst_col_resolved.par_iter_mut())
            .try_for_each(|(gid, resolved)| {
                let gid = gid.ok_or(LoadError::FatalError)?;
                let vid = write_locked_graph
                    .graph()
                    .resolve_node(gid.as_node_ref())
                    .map_err(|_| LoadError::FatalError)?;

                if vid.is_new() {
                    num_nodes.fetch_add(1, Ordering::Relaxed);
                }

                *resolved = vid.inner();
                Ok::<(), LoadError>(())
            })?;

        let time_col = df.time_col(time_index)?;

        write_locked_graph.resize_chunks_to_num_nodes(num_nodes.load(Ordering::Relaxed));

        eid_col_resolved.resize_with(df.len(), Default::default);
        eids_exist.resize_with(df.len(), Default::default);
        let eid_col_shared = atomic_usize_from_mut_slice(cast_slice_mut(&mut eid_col_resolved));

        let num_edges: Arc<AtomicUsize> =
            AtomicUsize::new(write_locked_graph.graph().internal_num_edges()).into();
        let next_edge_id = || num_edges.fetch_add(1, Ordering::Relaxed);

        let mut per_segment_edge_count = Vec::with_capacity(write_locked_graph.nodes.len());
        per_segment_edge_count.resize_with(write_locked_graph.nodes.len(), || AtomicUsize::new(0));

        // Generate all edge_ids + add outbound edges
        write_locked_graph
            .nodes
            .iter_mut() // TODO: change to par_iter_mut but preserve edge_id order
            .enumerate()
            .for_each(|(page_id, locked_page)| {
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
                        let mut writer = locked_page.writer();
                        writer.store_node_id(src_pos, 0, src_gid, 0);

                        let edge_id = if let Some(edge_id) = writer.get_out_edge(src_pos, *dst, 0) {
                            eid_col_shared[row].store(edge_id.0, Ordering::Relaxed);
                            eids_exist[row].store(true, Ordering::Relaxed);
                            edge_id
                        } else {
                            let edge_id = EID(next_edge_id());

                            writer.add_static_outbound_edge(src_pos, *dst, edge_id, 0);
                            eid_col_shared[row].store(edge_id.0, Ordering::Relaxed);
                            eids_exist[row].store(false, Ordering::Relaxed);
                            edge_id
                        };

                        writer.add_outbound_edge(
                            Some(t),
                            src_pos,
                            *dst,
                            edge_id.with_layer(*layer),
                            0,
                        ); // FIXME: when we update this to work with layers use the correct layer
                        per_segment_edge_count[page_id].fetch_add(1, Ordering::Relaxed);
                    }
                }
            });

        write_locked_graph.resize_chunks_to_num_edges(num_edges.load(Ordering::Relaxed));

        rayon::scope(|sc| {
            // Add inbound edges
            sc.spawn(|_| {
                write_locked_graph
                    .nodes
                    .par_iter_mut()
                    .enumerate()
                    .for_each(|(page_id, shard)| {
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
                                writer.add_static_inbound_edge(dst_pos, *src, *eid, 0);
                                writer.add_inbound_edge(
                                    Some(t),
                                    dst_pos,
                                    *src,
                                    eid.with_layer(*layer),
                                    0,
                                );

                                per_segment_edge_count[page_id].fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    });
            });

            // Add temporal & constant properties to edges
            sc.spawn(|_| {
                write_locked_graph.edges.par_iter_mut().for_each(|shard| {
                    let mut t_props = vec![];
                    let mut c_props = vec![];

                    for (idx, (((((src, dst), time), eid), layer), exists)) in src_col_resolved
                        .iter()
                        .zip(dst_col_resolved.iter())
                        .zip(time_col.iter())
                        .zip(eid_col_resolved.iter())
                        .zip(layer_col_resolved.iter())
                        .zip(
                            eids_exist
                                .iter()
                                .map(|exists| exists.load(Ordering::Relaxed)),
                        )
                        .enumerate()
                    {
                        if let Some(eid_pos) = shard.resolve_pos(*eid) {
                            let t = TimeIndexEntry(time, start_idx + idx);
                            let mut writer = shard.writer();

                            t_props.clear();
                            t_props.extend(prop_cols.iter_row(idx));

                            c_props.clear();
                            c_props.extend(metadata_cols.iter_row(idx));
                            c_props.extend_from_slice(&shared_metadata);

                            writer.add_static_edge(Some(eid_pos), *src, *dst, 0, Some(exists));
                            writer.update_c_props(eid_pos, *src, *dst, *layer, c_props.drain(..));
                            writer.add_edge(t, eid_pos, *src, *dst, t_props.drain(..), *layer, 0);
                        }
                    }
                });
            });
        });

        start_idx += df.len();
        let _ = pb.update(df.len());
    }

    // put the mapping into the fallback resolver
    // let fallback_resolver = &write_locked_graph.graph().logical_to_physical;
    // match fallback_resolver.dtype() {
    //     Some(GidType::Str) => {
    //         fallback_resolver
    //             .bulk_set_str(mapping.iter_str())
    //             .map_err(|_| LoadError::FatalError)?;
    //     }
    //     Some(GidType::U64) => {
    //         fallback_resolver
    //             .bulk_set_u64(mapping.iter_u64())
    //             .map_err(|_| LoadError::FatalError)?;
    //     }
    //     _ => {}
    // }

    Ok(())
}

fn load_into_shard<Q, T>(
    src_col_shared: &[AtomicUsize],
    dst_col_shared: &[AtomicUsize],
    src_col: &super::node_col::NodeCol,
    dst_col: &super::node_col::NodeCol,
    node_count: &AtomicUsize,
    shard: &mut ResolverShardT<'_, T>,
    mut mapper_fn: impl FnMut(GidRef<'_>) -> Cow<'_, Q>,
    mut fallback_fn: impl FnMut(&Q) -> Option<VID>,
) -> Result<(), LoadError>
where
    T: Clone + Eq + std::hash::Hash + Borrow<Q>,
    Q: Eq + std::hash::Hash + ToOwned<Owned = T> + ?Sized,
{
    let src_iter = src_col.iter().map(&mut mapper_fn).enumerate();

    for (id, gid) in src_iter {
        if let Some(vid) = shard.resolve_node(&gid, |id| {
            // fallback_fn(id).map(Either::Right).unwrap_or_else(|| {
            //     // If the node does not exist, create a new VID
            //     Either::Left(VID(node_count.fetch_add(1, Ordering::Relaxed)))
            // })
            Either::Left(VID(node_count.fetch_add(1, Ordering::Relaxed)))
        }) {
            src_col_shared[id].store(vid.0, Ordering::Relaxed);
        }
    }

    let dst_iter = dst_col.iter().map(mapper_fn).enumerate();
    for (id, gid) in dst_iter {
        if let Some(vid) = shard.resolve_node(&gid, |id| {
            // fallback_fn(id).map(Either::Right).unwrap_or_else(|| {
            //     // If the node does not exist, create a new VID
            //     Either::Left(VID(node_count.fetch_add(1, Ordering::Relaxed)))
            // })
            Either::Left(VID(node_count.fetch_add(1, Ordering::Relaxed)))
        }) {
            dst_col_shared[id].store(vid.0, Ordering::Relaxed);
        }
    }
    Ok::<_, LoadError>(())
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
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache + std::fmt::Debug,
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
        let metadata_cols = combine_properties(metadata, &metadata_indices, &df, |key, dtype| {
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
    let session = graph.write_session().map_err(into_graph_err)?;
    let shared_metadata = process_shared_properties(shared_metadata, |key, dtype| {
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

    let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;

    let g = write_locked_graph.graph;

    for chunk in df_view.chunks {
        let df = chunk?;
        let metadata_cols = combine_properties(metadata, &metadata_indices, &df, |key, dtype| {
            session
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

        write_locked_graph
            .resize_chunks_to_num_nodes(write_locked_graph.graph().internal_num_nodes());

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
                    if let Some(src_node) = shard.resolve_pos(*src) {
                        let writer = shard.writer();
                        let EID(eid) = writer
                            .get_out_edge(src_node, *dst, 0)
                            .ok_or(LoadError::MissingEdgeError(*src, *dst))?;
                        eid_col_shared[row].store(eid, Ordering::Relaxed);
                    }
                }
                Ok::<_, LoadError>(())
            })?;

        write_locked_graph
            .edges
            .par_iter_mut()
            .try_for_each(|shard| {
                let mut c_props = vec![];
                for (idx, (((eid, layer), src), dst)) in eid_col_resolved
                    .iter()
                    .zip(layer_col_resolved.iter())
                    .zip(&src_col_resolved)
                    .zip(&dst_col_resolved)
                    .enumerate()
                {
                    if let Some(eid_pos) = shard.resolve_pos(*eid) {
                        let mut writer = shard.writer();
                        c_props.clear();
                        c_props.extend(metadata_cols.iter_row(idx));
                        c_props.extend_from_slice(&shared_metadata);
                        writer.update_c_props(eid_pos, *src, *dst, *layer, c_props.drain(..));
                    }
                }
                Ok::<(), GraphError>(())
            })?;

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
        let metadata_cols = combine_properties(metadata, &metadata_indices, &df, |key, dtype| {
            session
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
    fn test_load_edges_1() {
        let edges = [(0, 1, 0, "a".to_string(), 0)];
        let chunk_size = 412;
        let df_view = build_df(chunk_size, &edges);
        let g = Graph::new();
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
