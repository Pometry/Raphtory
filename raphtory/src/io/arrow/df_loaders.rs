use crate::{
    core::{
        entities::LayerIds,
        utils::errors::{GraphError, LoadError},
        PropType,
    },
    db::api::{mutation::internal::*, view::StaticGraphViewOps},
    io::arrow::{
        dataframe::{DFChunk, DFView},
        layer_col::{lift_layer_col, lift_node_type_col},
        node_col::lift_node_col,
        prop_handler::*,
    },
    prelude::*,
};
use bytemuck::checked::cast_slice_mut;
use kdam::{Bar, BarBuilder, BarExt};
use parking_lot::Mutex;
use raphtory_api::{
    atomic_extra::atomic_usize_from_mut_slice,
    core::{
        entities::EID,
        storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
        Direction,
    },
};
use rayon::prelude::*;
use std::{collections::HashMap, sync::atomic::Ordering};

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
    'a,
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    time: &str,
    node_id: &str,
    properties: Option<&[&str]>,
    constant_properties: Option<&[&str]>,
    shared_constant_properties: Option<&HashMap<String, Prop>>,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
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

    let node_type_index = if let Some(node_type_col) = node_type_col {
        Some(df_view.get_index(node_type_col.as_ref()))
    } else {
        None
    };
    let node_type_index = node_type_index.transpose()?;

    let node_id_index = df_view.get_index(node_id)?;
    let time_index = df_view.get_index(time)?;

    let shared_constant_properties =
        process_shared_properties(shared_constant_properties, |key, dtype| {
            graph.resolve_node_property(key, dtype, true)
        })?;

    let mut pb = build_progress_bar("Loading nodes".to_string(), df_view.num_rows)?;

    let mut start_id = graph.reserve_event_ids(df_view.num_rows)?;
    for chunk in df_view.chunks {
        let df = chunk?;
        let prop_cols = combine_properties(properties, &properties_indices, &df, |key, dtype| {
            graph.resolve_node_property(key, dtype, false)
        })?;
        let const_prop_cols = combine_properties(
            constant_properties,
            &constant_properties_indices,
            &df,
            |key, dtype| graph.resolve_node_property(key, dtype, true),
        )?;
        let node_type_col = lift_node_type_col(node_type, node_type_index, &df)?;
        let time_col = df.time_col(time_index)?;
        let node_col = df.node_col(node_id_index)?;

        node_col
            .par_iter()
            .zip(time_col.par_iter())
            .zip(node_type_col.par_iter())
            .zip(prop_cols.par_rows())
            .zip(const_prop_cols.par_rows())
            .enumerate()
            .try_for_each(|(id, ((((node, time), node_type), t_props), c_props))| {
                let node = node.ok_or(LoadError::MissingNodeError)?;
                let time = time.ok_or(LoadError::MissingTimeError)?;
                let node_id = match node_type {
                    None => graph.resolve_node(node)?.inner(),
                    Some(node_type) => graph
                        .resolve_node_and_type(node, node_type)?
                        .inner()
                        .0
                        .inner(),
                };
                let t = TimeIndexEntry(time, start_id + id);
                let t_props: Vec<_> = t_props.collect();
                graph.internal_add_node(t, node_id, &t_props)?;
                let c_props: Vec<_> = c_props
                    .chain(shared_constant_properties.iter().cloned())
                    .collect();
                if !c_props.is_empty() {
                    graph.internal_add_constant_node_properties(node_id, &c_props)?;
                }
                Ok::<(), GraphError>(())
            })?;
        let _ = pb.update(df.len());
        start_id += df.len();
    }
    Ok(())
}

pub(crate) fn load_edges_from_df<
    'a,
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    time: &str,
    src: &str,
    dst: &str,
    properties: Option<&[&str]>,
    constant_properties: Option<&[&str]>,
    shared_constant_properties: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
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

    let src_index = df_view.get_index(src)?;
    let dst_index = df_view.get_index(dst)?;
    let time_index = df_view.get_index(time)?;
    let layer_index = if let Some(layer_col) = layer_col {
        Some(df_view.get_index(layer_col.as_ref())?)
    } else {
        None
    };
    let shared_constant_properties =
        process_shared_properties(shared_constant_properties, |key, dtype| {
            graph.resolve_edge_property(key, dtype, true)
        })?;

    let mut pb = build_progress_bar("Loading edges".to_string(), df_view.num_rows)?;
    let _ = pb.update(0);
    let mut start_idx = graph.reserve_event_ids(df_view.num_rows)?;

    let mut src_col_resolved = vec![];
    let mut dst_col_resolved = vec![];
    let mut eid_col_resolved = vec![];

    let mut write_locked_graph = graph.write_lock()?;
    for chunk in df_view.chunks {
        let df = chunk?;
        let prop_cols = combine_properties(properties, &properties_indices, &df, |key, dtype| {
            graph.resolve_edge_property(key, dtype, false)
        })?;
        let const_prop_cols = combine_properties(
            constant_properties,
            &constant_properties_indices,
            &df,
            |key, dtype| graph.resolve_edge_property(key, dtype, true),
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
                    if let Some(src_node) = shard.get_mut(*src) {
                        src_node.init(*src, src_gid);
                        update_time(TimeIndexEntry(time, start_idx + row));
                        src_node.update_time(TimeIndexEntry(time, start_idx + row));
                        let EID(eid) = match src_node.find_edge_eid(*dst, &LayerIds::All) {
                            None => {
                                let eid = next_edge_id();
                                src_node.add_edge(*dst, Direction::OUT, *layer, eid);
                                eid
                            }
                            Some(eid) => eid,
                        };
                        eid_col_shared[row].store(eid, Ordering::Relaxed);
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
                        node.update_time(TimeIndexEntry(time, row + start_idx));
                        node.add_edge(*src, Direction::IN, *layer, *eid)
                    }
                }
            });

        let failures = Mutex::new(Vec::new());
        write_locked_graph
            .edges
            .par_iter_mut()
            .for_each(|mut shard| {
                for (idx, ((((src, dst), time), eid), layer)) in src_col_resolved
                    .iter()
                    .zip(dst_col_resolved.iter())
                    .zip(time_col.iter())
                    .zip(eid_col_resolved.iter())
                    .zip(layer_col_resolved.iter())
                    .enumerate()
                {
                    if let Some(mut edge) = shard.get_mut(*eid) {
                        let edge_store = edge.edge_store_mut();
                        if !edge_store.initialised() {
                            edge_store.src = *src;
                            edge_store.dst = *dst;
                            edge_store.eid = *eid;
                        }
                        let t = TimeIndexEntry(time, start_idx + idx);
                        edge.additions_mut(*layer).insert(t);
                        let mut t_props = prop_cols.iter_row(idx).peekable();
                        let mut c_props = const_prop_cols
                            .iter_row(idx)
                            .chain(shared_constant_properties.iter().cloned())
                            .peekable();

                        if t_props.peek().is_some() || c_props.peek().is_some() {
                            let edge_layer = edge.layer_mut(*layer);
                            for (id, prop) in t_props {
                                if let Err(err) = edge_layer.add_prop(t, id, prop) {
                                    failures.lock().push((idx, err));
                                }
                            }

                            for (id, prop) in c_props {
                                if let Err(err) = edge_layer.update_constant_prop(id, prop) {
                                    failures.lock().push((idx, err))
                                }
                            }
                        }
                    }
                }
            });

        start_idx += df.len();
        let _ = pb.update(df.len());
    }
    Ok(())
}

pub(crate) fn load_edge_deletions_from_df<
    'a,
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps + DeletionOps,
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
    let layer_index = if let Some(layer_col) = layer_col {
        Some(df_view.get_index(layer_col.as_ref()))
    } else {
        None
    };
    let layer_index = layer_index.transpose()?;
    let mut pb = build_progress_bar("Loading edge deletions".to_string(), df_view.num_rows)?;
    let mut start_idx = graph.reserve_event_ids(df_view.num_rows)?;

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
        let _ = pb.update(df.len());
        start_idx += df.len();
    }

    Ok(())
}

pub(crate) fn load_node_props_from_df<
    'a,
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    node_id: &str,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    constant_properties: Option<&[&str]>,
    shared_constant_properties: Option<&HashMap<String, Prop>>,
    graph: &G,
) -> Result<(), GraphError> {
    let constant_properties = constant_properties.unwrap_or(&[]);
    let constant_properties_indices = constant_properties
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;
    let node_id_index = df_view.get_index(node_id)?;
    let node_type_index = if let Some(node_type_col) = node_type_col {
        Some(df_view.get_index(node_type_col.as_ref())?)
    } else {
        None
    };
    let shared_constant_properties = match shared_constant_properties {
        Some(props) => props
            .iter()
            .map(|(name, prop)| {
                Ok((
                    graph
                        .resolve_node_property(name, prop.dtype(), true)?
                        .inner(),
                    prop.clone(),
                ))
            })
            .collect::<Result<Vec<_>, GraphError>>()?,
        None => vec![],
    };
    let mut pb = build_progress_bar("Loading node properties".to_string(), df_view.num_rows)?;
    for chunk in df_view.chunks {
        let df = chunk?;
        let const_props = combine_properties(
            constant_properties,
            &constant_properties_indices,
            &df,
            |name, dtype| graph.resolve_node_property(name, dtype, true),
        )?;
        let node_col = df.node_col(node_id_index)?;
        let node_type_col = lift_node_type_col(node_type, node_type_index, &df)?;

        node_col
            .par_iter()
            .zip(node_type_col.par_iter())
            .zip(const_props.par_rows())
            .try_for_each(|((node_id, node_type), cprops)| {
                let node_id = node_id.ok_or(LoadError::MissingNodeError)?;
                let node = graph
                    .node(node_id)
                    .ok_or_else(|| GraphError::NodeMissingError(node_id.to_owned()))?;
                if let Some(node_type) = node_type {
                    node.set_node_type(node_type)?;
                }
                let props = cprops
                    .chain(shared_constant_properties.iter().cloned())
                    .collect::<Vec<_>>();
                if !props.is_empty() {
                    graph.internal_add_constant_node_properties(node.node, &props)?;
                }
                Ok::<(), GraphError>(())
            })?;
        let _ = pb.update(df.len());
    }
    Ok(())
}

pub(crate) fn load_edges_props_from_df<
    'a,
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    src: &str,
    dst: &str,
    constant_properties: Option<&[&str]>,
    shared_constant_properties: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    let constant_properties = constant_properties.unwrap_or(&[]);
    let constant_properties_indices = constant_properties
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;
    let src_index = df_view.get_index(src)?;
    let dst_index = df_view.get_index(dst)?;
    let layer_index = if let Some(layer_col) = layer_col {
        Some(df_view.get_index(layer_col.as_ref()))
    } else {
        None
    };
    let layer_index = layer_index.transpose()?;
    let mut pb = build_progress_bar("Loading edge properties".to_string(), df_view.num_rows)?;
    let shared_constant_properties = match shared_constant_properties {
        None => {
            vec![]
        }
        Some(props) => props
            .iter()
            .map(|(key, prop)| {
                Ok((
                    graph
                        .resolve_edge_property(key, prop.dtype(), true)?
                        .inner(),
                    prop.clone(),
                ))
            })
            .collect::<Result<Vec<_>, GraphError>>()?,
    };

    for chunk in df_view.chunks {
        let df = chunk?;
        let const_prop_iter = combine_properties(
            constant_properties,
            &constant_properties_indices,
            &df,
            |name, dtype| graph.resolve_edge_property(name, dtype, true),
        )?;

        let layer = lift_layer_col(layer, layer_index, &df)?;
        let src_col = lift_node_col(src_index, &df)?;
        let dst_col = lift_node_col(dst_index, &df)?;
        src_col
            .par_iter()
            .zip(dst_col.par_iter())
            .zip(layer.par_iter())
            .zip(const_prop_iter.par_rows())
            .try_for_each(|(((src, dst), layer), cprops)| {
                let src = src.ok_or(LoadError::MissingSrcError)?;
                let dst = dst.ok_or(LoadError::MissingDstError)?;
                let e = graph
                    .edge(src, dst)
                    .ok_or_else(|| GraphError::EdgeMissingError {
                        src: src.to_owned(),
                        dst: dst.to_owned(),
                    })?;
                let layer_id = graph.resolve_layer(layer)?.inner();
                let props = cprops
                    .chain(shared_constant_properties.iter().cloned())
                    .collect::<Vec<_>>();
                if !props.is_empty() {
                    graph.internal_add_constant_edge_properties(e.edge.pid(), layer_id, &props)?;
                }
                Ok::<(), GraphError>(())
            })?;
        let _ = pb.update(df.len());
    }
    Ok(())
}
