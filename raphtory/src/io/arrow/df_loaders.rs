use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::api::view::StaticGraphViewOps,
    errors::{into_graph_err, GraphError, LoadError},
    io::arrow::{
        dataframe::{DFChunk, DFView, SecondaryIndexCol},
        layer_col::{lift_layer_col, lift_node_type_col},
        prop_handler::*,
    },
    prelude::*,
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
use itertools::izip;

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
    let secondary_index_index = secondary_index.map(|col| df_view.get_index(col)).transpose()?;

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
        let secondary_index_col = match secondary_index_index {
            Some(col_index) => df.secondary_index_col(col_index)?,
            None => SecondaryIndexCol::new_from_range(start_id, start_id + df.len()),
        };

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
                        let t = TimeIndexEntry(time, secondary_index as usize);
                        let layer_id = 0;
                        let lsn = 0;

                        update_time(t);
                        writer.store_node_id_and_node_type(mut_node, layer_id, gid, *node_type, lsn);

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
        start_id += df.len();
    }

    Ok(())
}

pub(crate) fn load_edges_from_df<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    time: &str,
    secondary_index: Option<&str>,
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
    let secondary_index_index = secondary_index.map(|col| df_view.get_index(col)).transpose()?;
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

        // Load the secondary index column if it exists, otherwise generate from start_idx.
        let secondary_index_col = match secondary_index_index {
            Some(col_index) => df.secondary_index_col(col_index)?,
            None => SecondaryIndexCol::new_from_range(start_idx, start_idx + df.len()),
        };

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
                // Zip all columns for iteration.
                let zip = izip!(
                    src_col_resolved.iter(),
                    src_col.iter(),
                    dst_col_resolved.iter(),
                    time_col.iter(),
                    secondary_index_col.iter(),
                    layer_col_resolved.iter()
                );

                for (row, (src, src_gid, dst, time, secondary_index, layer)) in zip.enumerate() {
                    if let Some(src_pos) = locked_page.resolve_pos(*src) {
                        let t = TimeIndexEntry(time, secondary_index as usize);
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
                        let zip = izip!(
                            src_col_resolved.iter(),
                            dst_col_resolved.iter(),
                            dst_col.iter(),
                            eid_col_resolved.iter(),
                            time_col.iter(),
                            secondary_index_col.iter(),
                            layer_col_resolved.iter()
                        );

                        for (src, dst, dst_gid, eid, time, secondary_index, layer) in zip {
                            if let Some(dst_pos) = shard.resolve_pos(*dst) {
                                let t = TimeIndexEntry(time, secondary_index as usize);
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
                    let zip = izip!(
                        src_col_resolved.iter(),
                        dst_col_resolved.iter(),
                        time_col.iter(),
                        secondary_index_col.iter(),
                        eid_col_resolved.iter(),
                        layer_col_resolved.iter(),
                        eids_exist.iter().map(|exists| exists.load(Ordering::Relaxed))
                    );

                    for (row, (src, dst, time, secondary_index, eid, layer, exists)) in zip.enumerate() {
                        if let Some(eid_pos) = shard.resolve_pos(*eid) {
                            let t = TimeIndexEntry(time, secondary_index as usize);
                            let mut writer = shard.writer();

                            t_props.clear();
                            t_props.extend(prop_cols.iter_row(row));

                            c_props.clear();
                            c_props.extend(metadata_cols.iter_row(row));
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
    secondary_index: Option<&str>,
    src: &str,
    dst: &str,
    layer: Option<&str>,
    layer_col: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    let src_index = df_view.get_index(src)?;
    let dst_index = df_view.get_index(dst)?;
    let time_index = df_view.get_index(time)?;
    let secondary_index_index = secondary_index.map(|col| df_view.get_index(col)).transpose()?;
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

        // Load the secondary index column if it exists, otherwise generate from start_idx.
        let secondary_index_col = match secondary_index_index {
            Some(col_index) => df.secondary_index_col(col_index)?,
            None => SecondaryIndexCol::new_from_range(start_idx, start_idx + df.len()),
        };

        src_col
            .par_iter()
            .zip(dst_col.par_iter())
            .zip(time_col.par_iter())
            .zip(secondary_index_col.par_iter())
            .zip(layer.par_iter())
            .try_for_each(|((((src, dst), time), secondary_index), layer)| {
                let src = src.ok_or(LoadError::MissingSrcError)?;
                let dst = dst.ok_or(LoadError::MissingDstError)?;
                graph.delete_edge((time, secondary_index as usize), src, dst, layer)?;
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

pub(crate) fn load_edges_props_from_df<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
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
        let metadata_cols =
            combine_properties_arrow(metadata, &metadata_indices, &df, |key, dtype| {
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
    secondary_index: Option<&str>,
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
    let secondary_index_index = secondary_index.map(|col| df_view.get_index(col)).transpose()?;

    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading graph properties".to_string(), df_view.num_rows)?;
    let session = graph.write_session().map_err(into_graph_err)?;

    let mut start_id = session
        .reserve_event_ids(df_view.num_rows)
        .map_err(into_graph_err)?;

    for chunk in df_view.chunks {
        let df = chunk?;
        let prop_cols =
            combine_properties_arrow(properties, &properties_indices, &df, |key, dtype| {
                session
                    .resolve_graph_property(key, dtype, false)
                    .map_err(into_graph_err)
            })?;
        let metadata_cols =
            combine_properties_arrow(metadata, &metadata_indices, &df, |key, dtype| {
                session
                    .resolve_graph_property(key, dtype, true)
                    .map_err(into_graph_err)
            })?;
        let time_col = df.time_col(time_index)?;

        // Load the secondary index column if it exists, otherwise generate from start_id.
        let secondary_index_col = match secondary_index_index {
            Some(col_index) => df.secondary_index_col(col_index)?,
            None => SecondaryIndexCol::new_from_range(start_id, start_id + df.len()),
        };

        time_col
            .par_iter()
            .zip(secondary_index_col.par_iter())
            .zip(prop_cols.par_rows())
            .zip(metadata_cols.par_rows())
            .try_for_each(|(((time, secondary_index), t_props), c_props)| {
                let t = TimeIndexEntry(time, secondary_index as usize);
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
            df_loaders::{load_edges_from_df, load_nodes_from_df},
        },
        prelude::*,
        test_utils::{build_edge_list, build_edge_list_str},
    };
    use arrow_array::builder::{
        ArrayBuilder, Int64Builder, LargeStringBuilder, StringViewBuilder,
        UInt64Builder,
    };
    use itertools::Itertools;
    use proptest::proptest;
    use raphtory_core::storage::timeindex::TimeIndexEntry;
    use raphtory_storage::core_ops::CoreGraphOps;

    fn build_df(
        chunk_size: usize,
        edges: &[(u64, u64, i64, String, i64)],
    ) -> DFView<impl Iterator<Item = Result<DFChunk, GraphError>>> {
        let chunks = edges.iter().chunks(chunk_size);
        let mut src_col = UInt64Builder::new();
        let mut dst_col = UInt64Builder::new();
        let mut time_col = Int64Builder::new();
        let mut str_prop_col = LargeStringBuilder::new();
        let mut int_prop_col = Int64Builder::new();
        let chunks = chunks
            .into_iter()
            .map(|chunk| {
                for (src, dst, time, str_prop, int_prop) in chunk {
                    src_col.append_value(*src);
                    dst_col.append_value(*dst);
                    time_col.append_value(*time);
                    str_prop_col.append_value(str_prop);
                    int_prop_col.append_value(*int_prop);
                }
                let chunk = vec![
                    ArrayBuilder::finish(&mut src_col),
                    ArrayBuilder::finish(&mut dst_col),
                    ArrayBuilder::finish(&mut time_col),
                    ArrayBuilder::finish(&mut str_prop_col),
                    ArrayBuilder::finish(&mut int_prop_col),
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

    fn build_df_str(
        chunk_size: usize,
        edges: &[(String, String, i64, String, i64)],
    ) -> DFView<impl Iterator<Item = Result<DFChunk, GraphError>>> {
        let chunks = edges.iter().chunks(chunk_size);
        let mut src_col = LargeStringBuilder::new();
        let mut dst_col = StringViewBuilder::new();
        let mut time_col = Int64Builder::new();
        let mut str_prop_col = StringViewBuilder::new();
        let mut int_prop_col = Int64Builder::new();
        let chunks = chunks
            .into_iter()
            .map(|chunk| {
                for (src, dst, time, str_prop, int_prop) in chunk {
                    src_col.append_value(src);
                    dst_col.append_value(dst);
                    time_col.append_value(*time);
                    str_prop_col.append_value(str_prop);
                    int_prop_col.append_value(*int_prop);
                }
                let chunk = vec![
                    ArrayBuilder::finish(&mut src_col),
                    ArrayBuilder::finish(&mut dst_col),
                    ArrayBuilder::finish(&mut time_col),
                    ArrayBuilder::finish(&mut str_prop_col),
                    ArrayBuilder::finish(&mut int_prop_col),
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

    fn build_df_with_secondary_index(
        chunk_size: usize,
        edges: &[(u64, u64, i64, u64, String, i64)],
    ) -> DFView<impl Iterator<Item = Result<DFChunk, GraphError>>> {
        let chunks = edges.iter().chunks(chunk_size);
        let mut src_col = UInt64Builder::new();
        let mut dst_col = UInt64Builder::new();
        let mut time_col = Int64Builder::new();
        let mut secondary_index_col = UInt64Builder::new();
        let mut str_prop_col = LargeStringBuilder::new();
        let mut int_prop_col = Int64Builder::new();
        let chunks = chunks
            .into_iter()
            .map(|chunk| {
                for (src, dst, time, secondary_index, str_prop, int_prop) in chunk {
                    src_col.append_value(*src);
                    dst_col.append_value(*dst);
                    time_col.append_value(*time);
                    secondary_index_col.append_value(*secondary_index);
                    str_prop_col.append_value(str_prop);
                    int_prop_col.append_value(*int_prop);
                }
                let chunk = vec![
                    ArrayBuilder::finish(&mut src_col),
                    ArrayBuilder::finish(&mut dst_col),
                    ArrayBuilder::finish(&mut time_col),
                    ArrayBuilder::finish(&mut secondary_index_col),
                    ArrayBuilder::finish(&mut str_prop_col),
                    ArrayBuilder::finish(&mut int_prop_col),
                ];
                Ok(DFChunk { chunk })
            })
            .collect_vec();
        DFView {
            names: vec![
                "src".to_owned(),
                "dst".to_owned(),
                "time".to_owned(),
                "secondary_index".to_owned(),
                "str_prop".to_owned(),
                "int_prop".to_owned(),
            ],
            chunks: chunks.into_iter(),
            num_rows: edges.len(),
        }
    }

    fn build_nodes_df_with_secondary_index(
        chunk_size: usize,
        nodes: &[(u64, i64, u64, String, i64)],
    ) -> DFView<impl Iterator<Item = Result<DFChunk, GraphError>>> {
        let chunks = nodes.iter().chunks(chunk_size);
        let mut node_id_col = UInt64Builder::new();
        let mut time_col = Int64Builder::new();
        let mut secondary_index_col = UInt64Builder::new();
        let mut str_prop_col = LargeStringBuilder::new();
        let mut int_prop_col = Int64Builder::new();
        let chunks = chunks
            .into_iter()
            .map(|chunk| {
                for (node_id, time, secondary_index, str_prop, int_prop) in chunk {
                    node_id_col.append_value(*node_id);
                    time_col.append_value(*time);
                    secondary_index_col.append_value(*secondary_index);
                    str_prop_col.append_value(str_prop);
                    int_prop_col.append_value(*int_prop);
                }
                let chunk = vec![
                    ArrayBuilder::finish(&mut node_id_col),
                    ArrayBuilder::finish(&mut time_col),
                    ArrayBuilder::finish(&mut secondary_index_col),
                    ArrayBuilder::finish(&mut str_prop_col),
                    ArrayBuilder::finish(&mut int_prop_col),
                ];
                Ok(DFChunk { chunk })
            })
            .collect_vec();
        DFView {
            names: vec![
                "node_id".to_owned(),
                "time".to_owned(),
                "secondary_index".to_owned(),
                "str_prop".to_owned(),
                "int_prop".to_owned(),
            ],
            chunks: chunks.into_iter(),
            num_rows: nodes.len(),
        }
    }

    #[test]
    fn test_load_edges() {
        proptest!(|(edges in build_edge_list(1000, 100), chunk_size in 1usize..=1000)| {
            let distinct_edges = edges.iter().map(|(src, dst, _, _, _)| (src, dst)).collect::<std::collections::HashSet<_>>().len();
            let df_view = build_df(chunk_size, &edges);
            let g = Graph::new();
            let props = ["str_prop", "int_prop"];
            let secondary_index = None;

            load_edges_from_df(
                df_view,
                "time",
                secondary_index,
                "src",
                "dst",
                &props,
                &[],
                None,
                None,
                None,
            &g).unwrap();

            let g2 = Graph::new();

            for (src, dst, time, str_prop, int_prop) in edges {
                g2.add_edge(time, src, dst, [("str_prop", str_prop.clone().into_prop()), ("int_prop", int_prop.into_prop())], None).unwrap();
                let edge = g.edge(src, dst).unwrap().at(time);
                assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
                assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);
            }

            assert_eq!(g.unfiltered_num_edges(), distinct_edges);
            assert_eq!(g2.unfiltered_num_edges(), distinct_edges);
            assert_graph_equal(&g, &g2);
        })
    }

    #[test]
    fn test_load_edges_str() {
        proptest!(|(edges in build_edge_list_str(100, 100), chunk_size in 1usize..=100)| {
            let distinct_edges = edges.iter().map(|(src, dst, _, _, _)| (src, dst)).collect::<std::collections::HashSet<_>>().len();
            let df_view = build_df_str(chunk_size, &edges);
            let g = Graph::new();
            let props = ["str_prop", "int_prop"];
            let secondary_index = None;

            load_edges_from_df(
                df_view,
                "time",
                secondary_index,
                "src",
                "dst",
                &props,
                &[],
                None,
                None,
                None,
                &g,
            ).unwrap();

            let g2 = Graph::new();

            for (src, dst, time, str_prop, int_prop) in edges {
                g2.add_edge(time, &src, &dst, [("str_prop", str_prop.clone().into_prop()), ("int_prop", int_prop.into_prop())], None).unwrap();
                let edge = g.edge(&src, &dst).unwrap().at(time);
                assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
                assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);
            }

            assert_eq!(g.unfiltered_num_edges(), distinct_edges);
            assert_eq!(g2.unfiltered_num_edges(), distinct_edges);
            assert_graph_equal(&g, &g2);
        })
    }

    #[test]
    fn test_load_edges_str_fail() {
        let edges = [("0".to_string(), "1".to_string(), 0, "".to_string(), 0)];
        let df_view = build_df_str(1, &edges);
        let g = Graph::new();
        let props = ["str_prop", "int_prop"];
        let secondary_index = None;

        load_edges_from_df(
            df_view,
            "time",
            secondary_index,
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
        assert!(g.has_edge("0", "1"))
    }

    fn check_load_edges_layers(
        mut edges: Vec<(u64, u64, i64, String, i64, Option<String>)>,
        chunk_size: usize,
    ) {
        let distinct_edges = edges
            .iter()
            .map(|(src, dst, _, _, _, _)| (src, dst))
            .collect::<std::collections::HashSet<_>>()
            .len();
        edges.sort_by(|(_, _, _, _, _, l1), (_, _, _, _, _, l2)| l1.cmp(l2));
        let g = Graph::new();
        let g2 = Graph::new();

        for edges in edges.chunk_by(|(_, _, _, _, _, l1), (_, _, _, _, _, l2)| l1 < l2) {
            let layer = edges[0].5.clone();
            let edges = edges
                .iter()
                .map(|(src, dst, time, str_prop, int_prop, _)| {
                    (*src, *dst, *time, str_prop.clone(), *int_prop)
                })
                .collect_vec();
            let df_view = build_df(chunk_size, &edges);
            let props = ["str_prop", "int_prop"];
            let secondary_index = None;

            load_edges_from_df(
                df_view,
                "time",
                secondary_index,
                "src",
                "dst",
                &props,
                &[],
                None,
                layer.as_deref(),
                None,
                &g,
            )
            .unwrap();
            for (src, dst, time, str_prop, int_prop) in edges {
                g2.add_edge(
                    time,
                    src,
                    dst,
                    [
                        ("str_prop", str_prop.clone().into_prop()),
                        ("int_prop", int_prop.into_prop()),
                    ],
                    layer.as_deref(),
                )
                .unwrap();
                let edge = g.edge(src, dst).unwrap().at(time);
                assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
                assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);
                if let Some(layer) = &layer {
                    assert!(edge.has_layer(layer))
                }
            }
            assert_graph_equal(&g, &g2);
        }
    }

    #[test]
    fn test_load_edges_1() {
        let edges = [(0, 1, 0, "a".to_string(), 0)];
        let chunk_size = 412;
        let df_view = build_df(chunk_size, &edges);
        let g = Graph::new();
        let props = ["str_prop", "int_prop"];
        let secondary_index = None;

        load_edges_from_df(
            df_view,
            "time",
            secondary_index,
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

    #[test]
    fn test_load_edges_with_secondary_index() {
        // Create edges with the same timestamp but different secondary_index values
        // Edge format: (src, dst, time, secondary_index, str_prop, int_prop)
        let edges = [
            (1, 2, 100, 2, "secondary_index_2".to_string(), 1),
            (1, 2, 100, 0, "secondary_index_0".to_string(), 2),
            (1, 2, 100, 1, "secondary_index_1".to_string(), 3),

            (2, 3, 200, 1, "secondary_index_1".to_string(), 4),
            (2, 3, 200, 0, "secondary_index_0".to_string(), 5),

            (3, 4, 300, 10, "secondary_index_10".to_string(), 6),
            (3, 4, 300, 5, "secondary_index_5".to_string(), 7),

            (4, 5, 400, 0, "secondary_index_0".to_string(), 8),
            (4, 5, 500, 0, "secondary_index_0".to_string(), 9),
        ];

        let chunk_size = 50;
        let df_view = build_df_with_secondary_index(chunk_size, &edges);
        let g = Graph::new();
        let props = ["str_prop", "int_prop"];
        let secondary_index = Some("secondary_index");

        // Load edges from DataFrame with secondary_index
        load_edges_from_df(
            df_view,
            "time",
            secondary_index,
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

        for (src, dst, time, secondary_index, str_prop, int_prop) in edges {
            let time_with_secondary_index = TimeIndexEntry(
                time,
                secondary_index as usize,
            );

            g2.add_edge(
                time_with_secondary_index,
                src,
                dst,
                [
                    ("str_prop", str_prop.clone().into_prop()),
                    ("int_prop", int_prop.into_prop()),
                ],
                None,
            )
            .unwrap();
        }

        // Internally checks whether temporal props are sorted by
        // secondary index.
        assert_graph_equal(&g, &g2);
    }

    #[test]
    fn test_load_nodes_with_secondary_index() {
        // Create nodes with the same timestamp but different secondary_index values
        // Node format: (node_id, time, secondary_index, str_prop, int_prop)
        let nodes = [
            (1, 100, 2, "secondary_index_2".to_string(), 1),
            (1, 100, 0, "secondary_index_0".to_string(), 2),
            (1, 100, 1, "secondary_index_1".to_string(), 3),

            (2, 200, 1, "secondary_index_1".to_string(), 4),
            (2, 200, 0, "secondary_index_0".to_string(), 5),

            (3, 300, 10, "secondary_index_10".to_string(), 6),
            (3, 300, 5, "secondary_index_5".to_string(), 7),

            (4, 400, 0, "secondary_index_0".to_string(), 8),
            (4, 500, 0, "secondary_index_0".to_string(), 9),
        ];

        let chunk_size = 50;
        let df_view = build_nodes_df_with_secondary_index(chunk_size, &nodes);
        let g = Graph::new();
        let props = ["str_prop", "int_prop"];
        let secondary_index = Some("secondary_index");

        // Load nodes from DataFrame with secondary_index
        load_nodes_from_df(
            df_view,
            "time",
            secondary_index,
            "node_id",
            &props,
            &[],
            None,
            None,
            None,
            &g,
        )
        .unwrap();

        let g2 = Graph::new();

        for (node_id, time, secondary_index, str_prop, int_prop) in nodes {
            let time_with_secondary_index = TimeIndexEntry(
                time,
                secondary_index as usize,
            );

            g2.add_node(
                time_with_secondary_index,
                node_id,
                [
                    ("str_prop", str_prop.clone().into_prop()),
                    ("int_prop", int_prop.into_prop()),
                ],
                None,
            )
            .unwrap();
        }

        // Internally checks whether temporal props are sorted by
        // secondary index.
        assert_graph_equal(&g, &g2);
    }
}
