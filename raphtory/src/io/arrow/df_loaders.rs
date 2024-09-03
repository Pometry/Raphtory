use crate::{
    core::{
        utils::errors::{GraphError, LoadError},
        PropType,
    },
    db::api::{
        mutation::internal::*,
        view::{internal::CoreGraphOps, StaticGraphViewOps},
    },
    io::arrow::{
        dataframe::{DFChunk, DFView},
        layer_col::{lift_layer_col, lift_node_type_col},
        node_col::lift_node_col,
        prop_handler::*,
    },
    prelude::*,
};
use kdam::{Bar, BarBuilder, BarExt};
use raphtory_api::core::{
    entities::VID,
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use rayon::prelude::*;
use std::collections::HashMap;

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

        let src_col = df.node_col(src_index)?;
        let mut src_col_resolved = vec![VID(0); df.len()];
        src_col
            .par_iter()
            .zip(src_col_resolved.par_iter_mut())
            .try_for_each(|(gid, entry)| {
                let gid = gid.ok_or(LoadError::MissingSrcError)?;
                let vid = graph.resolve_node_no_init(gid)?.inner();
                *entry = vid;
                Ok::<(), GraphError>(())
            })?;

        let dst_col = df.node_col(dst_index)?;
        let mut dst_col_resolved = vec![VID(0); df.len()];
        dst_col
            .par_iter()
            .zip(dst_col_resolved.par_iter_mut())
            .try_for_each(|(gid, entry)| {
                let gid = gid.ok_or(LoadError::MissingDstError)?;
                let vid = graph.resolve_node_no_init(gid)?.inner();
                *entry = vid;
                Ok::<(), GraphError>(())
            })?;

        let mut write_locked_graph = graph.write_lock()?;

        let mut eid_col_resolved = vec![0; df.len()];

        let time_col = df.time_col(time_index)?;
        src_col
            .par_iter()
            .zip(dst_col.par_iter())
            .zip(time_col.par_iter())
            .zip(layer.par_iter())
            .zip(prop_cols.par_rows())
            .zip(const_prop_cols.par_rows())
            .enumerate()
            .try_for_each(|(idx, (((((src, dst), time), layer), t_props), c_props))| {
                let src = src.ok_or(LoadError::MissingSrcError)?;
                let dst = dst.ok_or(LoadError::MissingDstError)?;
                let time = time.ok_or(LoadError::MissingTimeError)?;
                let time_idx = TimeIndexEntry(time, start_idx + idx);
                let src = graph.resolve_node(src)?.inner();
                let dst = graph.resolve_node(dst)?.inner();
                let layer = graph.resolve_layer(layer)?.inner();
                let t_props: Vec<_> = t_props.collect();
                let c_props: Vec<_> = c_props
                    .chain(shared_constant_properties.iter().cloned())
                    .collect();
                let eid = graph
                    .internal_add_edge(time_idx, src, dst, &t_props, layer)?
                    .inner();
                if !c_props.is_empty() {
                    graph.internal_add_constant_edge_properties(eid, layer, &c_props)?;
                }
                Ok::<(), GraphError>(())
            })?;
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
