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
use arrow::array::BooleanArray;
use bytemuck::checked::cast_slice_mut;
use db4_graph::WriteLockedGraph;
use either::Either;
use itertools::izip;
use kdam::{Bar, BarBuilder, BarExt};
use raphtory_api::{
    atomic_extra::atomic_usize_from_mut_slice,
    core::{
        entities::{
            properties::{meta::STATIC_GRAPH_LAYER_ID, prop::PropType},
            EID,
        },
        storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
    },
};
use raphtory_core::{
    entities::{graph::logical_to_physical::ResolverShardT, GidRef, VID},
    storage::timeindex::AsTime,
};
use raphtory_storage::core_ops::CoreGraphOps;
use raphtory_storage::layer_ops::InternalLayerOps;
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
                let col = SecondaryIndexCol::new_from_range(start_id, start_id + df.len());
                col
            }
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

pub fn load_edges_from_df<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
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

    let mut pb = build_progress_bar("Loading edges".to_string(), df_view.num_rows)?;

    let mut src_col_resolved = vec![];
    let mut dst_col_resolved = vec![];
    let mut eid_col_resolved: Vec<EID> = vec![];
    let mut eids_exist: Vec<AtomicBool> = vec![]; // exists or needs to be created
    let mut layer_eids_exist: Vec<AtomicBool> = vec![]; // exists or needs to be created

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
    let all_graph_layers = graph.edge_meta().layer_meta().all_ids().collect::<Vec<_>>();

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

        // Load the secondary index column if it exists, otherwise generate from start_id.
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
                let col = SecondaryIndexCol::new_from_range(start_id, start_id + df.len());
                col
            }
        };

        write_locked_graph.resize_chunks_to_num_nodes(num_nodes.load(Ordering::Relaxed));

        eid_col_resolved.resize_with(df.len(), Default::default);
        eids_exist.resize_with(df.len(), Default::default);
        layer_eids_exist.resize_with(df.len(), Default::default);
        let eid_col_shared = atomic_usize_from_mut_slice(cast_slice_mut(&mut eid_col_resolved));

        let num_edges: Arc<AtomicUsize> =
            AtomicUsize::new(write_locked_graph.graph().internal_num_edges()).into();
        let next_edge_id = || num_edges.fetch_add(1, Ordering::Relaxed);

        let mut per_segment_edge_count = Vec::with_capacity(write_locked_graph.nodes.len());
        per_segment_edge_count.resize_with(write_locked_graph.nodes.len(), || AtomicUsize::new(0));

        let WriteLockedGraph {
            nodes, ref edges, ..
        } = &mut write_locked_graph;

        // Generate all edge_ids + add outbound edges
        nodes
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
                        let mut writer = locked_page.writer();
                        let t = TimeIndexEntry(time, secondary_index);
                        writer.store_node_id(src_pos, 0, src_gid, 0);
                        // find the original EID in the static graph if it exists
                        // otherwise create a new one
                        // find the edge id in the layer, first using the EID then using get_out_edge (more expensive)

                        let edge_id = if let Some(edge_id) = writer.get_out_edge(src_pos, *dst, 0) {
                            eid_col_shared[row].store(edge_id.0, Ordering::Relaxed);
                            eids_exist[row].store(true, Ordering::Relaxed);
                            edge_id.with_layer(*layer)
                        } else {
                            let edge_id = EID(next_edge_id());

                            writer.add_static_outbound_edge(src_pos, *dst, edge_id, 0);
                            eid_col_shared[row].store(edge_id.0, Ordering::Relaxed);
                            eids_exist[row].store(false, Ordering::Relaxed);
                            edge_id.with_layer(*layer)
                        };

                        if edges.exists(edge_id)
                            || writer.get_out_edge(src_pos, *dst, *layer).is_some()
                        {
                            layer_eids_exist[row].store(true, Ordering::Relaxed);
                            // node additions
                            writer.update_timestamp(t, src_pos, edge_id, 0);
                        } else {
                            layer_eids_exist[row].store(false, Ordering::Relaxed);
                            // actually adds the edge
                            writer.add_outbound_edge(Some(t), src_pos, *dst, edge_id, 0);
                        }

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
                            layer_col_resolved.iter(),
                            layer_eids_exist.iter().map(|a| a.load(Ordering::Relaxed)),
                            eids_exist.iter().map(|b| b.load(Ordering::Relaxed))
                        );

                        for (
                            src,
                            dst,
                            dst_gid,
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

                                writer.store_node_id(dst_pos, 0, dst_gid, 0);

                                if !edge_exists_in_static_graph {
                                    writer.add_static_inbound_edge(dst_pos, *src, *eid, 0);
                                }

                                if !edge_exists_in_layer {
                                    writer.add_inbound_edge(
                                        Some(t),
                                        dst_pos,
                                        *src,
                                        eid.with_layer(*layer),
                                        0,
                                    );
                                } else {
                                    writer.update_timestamp(t, dst_pos, eid.with_layer(*layer), 0);
                                }

                                per_segment_edge_count[page_id].fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    });
            });

            // Add temporal & constant properties to edges
            sc.spawn(|_| {
                write_locked_graph.edges.par_iter_mut().for_each(|shard| {
                    let zip = izip!(
                        src_col_resolved.iter(),
                        dst_col_resolved.iter(),
                        time_col.iter(),
                        secondary_index_col.iter(),
                        eid_col_resolved.iter(),
                        layer_col_resolved.iter(),
                        eids_exist
                            .iter()
                            .map(|exists| exists.load(Ordering::Relaxed))
                    );
                    let mut t_props: Vec<(usize, Prop)> = vec![];
                    let mut c_props: Vec<(usize, Prop)> = vec![];

                    // let temporal_prop_cols = prop_cols.cols();
                    // // let metadata_cols = metadata_cols.cols();

                    // let masks = if layer_col.is_some() {
                    //     let mut masks = vec![];
                    //     for l in &all_graph_layers {
                    //         let mut mask = BooleanArray::builder(src_col_resolved.len());
                    //         for (i, (eid, layer)) in
                    //             eid_col_resolved.iter().zip(&layer_col_resolved).enumerate()
                    //         {
                    //             let exists = shard.resolve_pos(*eid).is_some() && *layer == *l;
                    //             mask.append_value(exists);
                    //         }
                    //         let mask = mask.finish();
                    //         if !mask.is_empty() {
                    //             masks.push(mask);
                    //         }
                    //     }
                    //     masks
                    // } else {
                    //     let mut mask = BooleanArray::builder(src_col_resolved.len());
                    //     // all these go into a single layer
                    //     for (i, eid) in eid_col_resolved.iter().enumerate() {
                    //         let exists = shard.resolve_pos(*eid).is_some();
                    //         mask.append_value(exists);
                    //     }
                    //     let mask = mask.finish();
                    //     vec![mask]
                    // };

                    // let mut writer = shard.writer();

                    // for mask in masks {
                    //     writer.bulk_add_edges(
                    //         &mask,
                    //         time_col.values(),
                    //         start_idx,
                    //         &eid_col_resolved,
                    //         &src_col_resolved,
                    //         &dst_col_resolved,
                    //         0, // use the mask to select for layer
                    //         &temporal_prop_cols,
                    //         prop_cols.prop_ids(),
                    //     )
                    // }

                    for (row, (src, dst, time, secondary_index, eid, layer, exists)) in
                        zip.enumerate()
                    {
                        if let Some(eid_pos) = shard.resolve_pos(*eid) {
                            let t = TimeIndexEntry(time, secondary_index);
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

        #[cfg(feature = "python")]
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
    if df_view.is_empty() {
        return Ok(());
    }
    let src_index = df_view.get_index(src)?;
    let dst_index = df_view.get_index(dst)?;
    let time_index = df_view.get_index(time)?;
    let secondary_index_index = secondary_index
        .map(|col| df_view.get_index(col))
        .transpose()?;
    let layer_index = layer_col.map(|layer_col| df_view.get_index(layer_col.as_ref()));
    let layer_index = layer_index.transpose()?;
    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading edge deletions".to_string(), df_view.num_rows)?;
    let session = graph.write_session().map_err(into_graph_err)?;

    for chunk in df_view.chunks {
        let df = chunk?;
        let layer = lift_layer_col(layer, layer_index, &df)?;
        let src_col = df.node_col(src_index)?;
        let dst_col = df.node_col(dst_index)?;
        let time_col = df.time_col(time_index)?;

        // Load the secondary index column if it exists, otherwise generate from start_id.
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
                let col = SecondaryIndexCol::new_from_range(start_id, start_id + df.len());
                col
            }
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
                graph.delete_edge((time, secondary_index), src, dst, layer)?;
                Ok::<(), GraphError>(())
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
    if df_view.is_empty() {
        return Ok(());
    }
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
    if df_view.is_empty() {
        return Ok(());
    }
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
    let secondary_index_index = secondary_index
        .map(|col| df_view.get_index(col))
        .transpose()?;

    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading graph properties".to_string(), df_view.num_rows)?;
    let session = graph.write_session().map_err(into_graph_err)?;

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
                let col = SecondaryIndexCol::new_from_range(start_id, start_id + df.len());
                col
            }
        };

        time_col
            .par_iter()
            .zip(secondary_index_col.par_iter())
            .zip(prop_cols.par_rows())
            .zip(metadata_cols.par_rows())
            .try_for_each(|(((time, secondary_index), t_props), c_props)| {
                let t = TimeIndexEntry(time, secondary_index);
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
    }

    Ok(())
}
