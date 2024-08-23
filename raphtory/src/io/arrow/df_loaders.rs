use crate::{
    core::utils::errors::GraphError,
    db::api::{
        mutation::{internal::*, AdditionOps},
        view::StaticGraphViewOps,
    },
    io::arrow::{
        dataframe::{DFChunk, DFView},
        prop_handler::*,
    },
    prelude::*,
};

use crate::{
    core::PropType,
    db::api::view::internal::CoreGraphOps,
    io::arrow::{
        layer_col::{lift_layer_col, lift_node_type_col},
        node_col::lift_node_col,
    },
};
use kdam::{Bar, BarBuilder, BarExt};
use raphtory_api::core::storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry};
use rayon::prelude::*;
use std::{collections::HashMap, iter};

fn build_progress_bar(des: String, num_rows: usize) -> Result<Bar, GraphError> {
    BarBuilder::default()
        .desc(des)
        .animation(kdam::Animation::FillUp)
        .total(num_rows)
        .unit_scale(true)
        .build()
        .map_err(|_| GraphError::TqdmError)
}
fn extract_out_default_type(n_t: Option<&str>) -> Option<&str> {
    if n_t == Some("_default") {
        None
    } else {
        n_t
    }
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

    let mut start_id = graph.reserve_ids(df_view.num_rows)?;
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
                if let Some(node) = node {
                    if let Some(time) = time {
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
                        graph.internal_add_constant_node_properties(node_id, &c_props)?;
                    }
                }
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
    let mut start_idx = graph.reserve_ids(df_view.num_rows)?;

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
        let dst_col = df.node_col(dst_index)?;
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
                if let Some(src) = src {
                    if let Some(dst) = dst {
                        if let Some(time) = time {
                            let time_idx = TimeIndexEntry(time, start_idx + idx);
                            let src = graph.resolve_node(src)?.inner();
                            let dst = graph.resolve_node(dst)?.inner();
                            let layer = graph.resolve_layer(layer)?.inner();
                            let t_pros: Vec<_> = t_props.collect();
                            let c_props: Vec<_> =  c_props.chain(shared_constant_properties.iter().cloned()).collect();
                            let eid = graph.internal_add_edge(time_idx, src, dst, &t_props, layer)?.inner();
                            graph.internal_add_constant_edge_properties(eid, layer, &c_props)?;
                        }
                    }
                }
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

    for chunk in df_view.chunks {
        let df = chunk?;
        let layer = lift_layer(layer, layer_index, &df)?;

        if let (Some(src), Some(dst), Some(time)) = (
            df.iter_col::<u64>(src_index),
            df.iter_col::<u64>(dst_index),
            df.time_iter_col(time_index),
        ) {
            let triplets = src
                .map(|i| i.copied())
                .zip(dst.map(|i| i.copied()))
                .zip(time);

            for (((src, dst), time), layer) in triplets.zip(layer) {
                if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                    graph.delete_edge(time, src, dst, layer.as_deref())?;
                }
                let _ = pb.update(1);
            }
        } else if let (Some(src), Some(dst), Some(time)) = (
            df.iter_col::<i64>(src_index),
            df.iter_col::<i64>(dst_index),
            df.time_iter_col(time_index),
        ) {
            let triplets = src
                .map(i64_opt_into_u64_opt)
                .zip(dst.map(i64_opt_into_u64_opt))
                .zip(time);

            for (((src, dst), time), layer) in triplets.zip(layer) {
                if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                    graph.delete_edge(time, src, dst, layer.as_deref())?;
                }
                let _ = pb.update(1);
            }
        } else if let (Some(src), Some(dst), Some(time)) = (
            df.utf8::<i32>(src_index),
            df.utf8::<i32>(dst_index),
            df.time_iter_col(time_index),
        ) {
            let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());
            for (((src, dst), time), layer) in triplets.zip(layer) {
                if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                    graph.delete_edge(time, src, dst, layer.as_deref())?;
                }
                let _ = pb.update(1);
            }
        } else if let (Some(src), Some(dst), Some(time)) = (
            df.utf8::<i64>(src_index),
            df.utf8::<i64>(dst_index),
            df.time_iter_col(time_index),
        ) {
            let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());

            for (((src, dst), time), layer) in triplets.zip(layer) {
                if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                    graph.delete_edge(time, src, dst, layer.as_deref())?;
                }
                let _ = pb.update(1);
            }
        } else {
            return Err(GraphError::LoadFailure(
                "Source and Target columns must be either u64 or text, Time column must be i64. Ensure these contain no NaN, Null or None values."
                    .to_string(),
            ));
        };
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
            |name, dtype| Ok(graph.resolve_node_property(name, dtype, true)?.inner()),
        )?;
        let node_col = df.node_col(node_id_index)?;
        let node_type_col = lift_node_type_col(node_type, node_type_index, &df)?;

        node_col
            .par_iter()
            .zip(node_type_col.par_iter())
            .zip(const_props.par_rows())
            .try_for_each(|((node_id, node_type), cprops)| {
                if let Some(node_id) = node_id {
                    let node = graph
                        .node(node_id)
                        .ok_or_else(|| GraphError::NodeMissingError(node_id.to_owned()))?;
                    if let Some(node_type) = node_type {
                        node.set_node_type(node_type)?;
                    }
                    let props = cprops
                        .chain(shared_constant_properties.iter().cloned())
                        .collect::<Vec<_>>();
                    graph.internal_add_constant_node_properties(node.node, &props)?;
                }
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
            |name, dtype| {
                graph
                    .resolve_edge_property(name, dtype, true)
                    .map(|id| id.inner())
            },
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
                if let Some(src) = src {
                    if let Some(dst) = dst {
                        let e =
                            graph
                                .edge(src, dst)
                                .ok_or_else(|| GraphError::EdgeMissingError {
                                    src: src.to_owned(),
                                    dst: dst.to_owned(),
                                })?;
                        let layer_id = graph.resolve_layer(layer)?.inner();
                        let props = cprops
                            .chain(shared_constant_properties.iter().cloned())
                            .collect::<Vec<_>>();
                        graph.internal_add_constant_edge_properties(
                            e.edge.pid(),
                            layer_id,
                            &props,
                        )?;
                    }
                }
                Ok::<(), GraphError>(())
            })?;
        let _ = pb.update(df.len());
    }
    Ok(())
}

fn i64_opt_into_u64_opt(x: Option<&i64>) -> Option<u64> {
    x.map(|x| (*x).try_into().unwrap())
}

fn load_edges_from_num_iter<
    'a,
    S: AsRef<str>,
    I: Iterator<Item = ((Option<u64>, Option<u64>), Option<i64>)>,
    PI: Iterator<Item = Vec<(S, Prop)>>,
    IL: Iterator<Item = Option<String>>,
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps,
>(
    graph: &G,
    pb: &mut Bar,
    edges: I,
    properties: PI,
    constant_properties: PI,
    shared_constant_properties: Option<&HashMap<String, Prop>>,
    layer: IL,
) -> Result<(), GraphError> {
    for (((((src, dst), time), edge_props), const_props), layer) in
        edges.zip(properties).zip(constant_properties).zip(layer)
    {
        if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
            let e = graph.add_edge(time, src, dst, edge_props, layer.as_deref())?;
            e.add_constant_properties(const_props, layer.as_deref())?;
            if let Some(shared_const_props) = &shared_constant_properties {
                e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
            }
        }
        let _ = pb.update(1);
    }
    Ok(())
}

fn load_nodes_from_num_iter<
    'a,
    S: AsRef<str>,
    I: Iterator<Item = (Option<u64>, Option<i64>, Option<&'a str>)>,
    PI: Iterator<Item = Vec<(S, Prop)>>,
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps,
>(
    graph: &G,
    pb: &mut Bar,
    nodes: I,
    properties: PI,
    constant_properties: PI,
    shared_constant_properties: Option<&HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    for (((node, time, node_type), props), const_props) in
        nodes.zip(properties).zip(constant_properties)
    {
        if let (Some(v), Some(t), n_t, props, const_props) =
            (node, time, node_type, props, const_props)
        {
            let actual_node_type = extract_out_default_type(n_t);
            let v = graph.add_node(t, v, props, actual_node_type)?;
            v.add_constant_properties(const_props)?;

            if let Some(shared_const_props) = &shared_constant_properties {
                v.add_constant_properties(shared_const_props.iter())?;
            }
            let _ = pb.update(1);
        }
    }
    Ok(())
}
