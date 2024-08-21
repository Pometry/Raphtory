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

use kdam::{Bar, BarBuilder, BarExt};
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
    let mut pb = build_progress_bar("Loading nodes".to_string(), df_view.num_rows)?;

    for chunk in df_view.chunks {
        let df = chunk?;
        let prop_iter = combine_properties(properties, &properties_indices, &df)?;
        let const_prop_iter =
            combine_properties(constant_properties, &constant_properties_indices, &df)?;

        let node_type: Result<Box<dyn Iterator<Item = Option<&str>>>, GraphError> =
            match (node_type, node_type_index) {
                (None, None) => Ok(Box::new(iter::repeat(None))),
                (Some(node_type), None) => Ok(Box::new(iter::repeat(Some(node_type)))),
                (None, Some(node_type_index)) => {
                    let iter_res: Result<Box<dyn Iterator<Item = Option<&str>>>, GraphError> =
                        if let Some(node_types) = df.utf8::<i32>(node_type_index) {
                            Ok(Box::new(node_types))
                        } else if let Some(node_types) = df.utf8::<i64>(node_type_index) {
                            Ok(Box::new(node_types))
                        } else {
                            Err(GraphError::LoadFailure(
                                "Unable to convert / find node_type column in dataframe."
                                    .to_string(),
                            ))
                        };
                    iter_res
                }
                _ => Err(GraphError::WrongNumOfArgs(
                    "node_type".to_string(),
                    "node_type_col".to_string(),
                )),
            };
        let node_type = node_type?;

        if let (Some(node_id), Some(time)) = (
            df.iter_col::<u64>(node_id_index),
            df.time_iter_col(time_index),
        ) {
            let iter = node_id
                .map(|i| i.copied())
                .zip(time)
                .zip(node_type)
                .map(|((node_id, time), n_t)| (node_id, time, n_t));
            load_nodes_from_num_iter(
                graph,
                &mut pb,
                iter,
                prop_iter,
                const_prop_iter,
                shared_constant_properties,
            )?;
        } else if let (Some(node_id), Some(time)) = (
            df.iter_col::<i64>(node_id_index),
            df.time_iter_col(time_index),
        ) {
            let iter = node_id.map(i64_opt_into_u64_opt).zip(time);
            let iter = iter
                .zip(node_type)
                .map(|((node_id, time), n_t)| (node_id, time, n_t));

            load_nodes_from_num_iter(
                graph,
                &mut pb,
                iter,
                prop_iter,
                const_prop_iter,
                shared_constant_properties,
            )?;
        } else if let (Some(node_id), Some(time)) =
            (df.utf8::<i32>(node_id_index), df.time_iter_col(time_index))
        {
            let iter = node_id.into_iter().zip(time);
            let iter = iter
                .zip(node_type)
                .map(|((node_id, time), n_t)| (node_id, time, n_t));

            for (((node_id, time, n_t), props), const_props) in
                iter.zip(prop_iter).zip(const_prop_iter)
            {
                if let (Some(node_id), Some(time), n_t) = (node_id, time, n_t) {
                    let actual_type = extract_out_default_type(n_t);
                    let v = graph.add_node(time, node_id, props, actual_type)?;
                    v.add_constant_properties(const_props)?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        v.add_constant_properties(shared_const_props.iter())?;
                    }
                }
                let _ = pb.update(1);
            }
        } else if let (Some(node_id), Some(time)) =
            (df.utf8::<i64>(node_id_index), df.time_iter_col(time_index))
        {
            let iter = node_id.into_iter().zip(time);
            let iter = iter
                .zip(node_type)
                .map(|((node_id, time), n_t)| (node_id, time, n_t));

            for (((node_id, time, n_t), props), const_props) in
                iter.zip(prop_iter).zip(const_prop_iter)
            {
                let actual_type = extract_out_default_type(n_t);
                if let (Some(node_id), Some(time), n_t) = (node_id, time, actual_type) {
                    let v = graph.add_node(time, node_id, props, n_t)?;
                    v.add_constant_properties(const_props)?;
                    if let Some(shared_const_props) = shared_constant_properties {
                        v.add_constant_properties(shared_const_props)?;
                    }
                }
                let _ = pb.update(1);
            }
        } else {
            return Err(GraphError::LoadFailure(
                "node id column must be either u64 or text, time column must be i64. Ensure these contain no NaN, Null or None values.".to_string(),
            ));
        };
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
        Some(df_view.get_index(layer_col.as_ref()))
    } else {
        None
    };
    let layer_index = layer_index.transpose()?;
    let mut pb = build_progress_bar("Loading edges".to_string(), df_view.num_rows)?;

    for chunk in df_view.chunks {
        let df = chunk?;
        let prop_iter = combine_properties(properties, &properties_indices, &df)?;
        let const_prop_iter =
            combine_properties(constant_properties, &constant_properties_indices, &df)?;

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
            load_edges_from_num_iter(
                graph,
                &mut pb,
                triplets,
                prop_iter,
                const_prop_iter,
                shared_constant_properties,
                layer,
            )?;
        } else if let (Some(src), Some(dst), Some(time)) = (
            df.iter_col::<i64>(src_index),
            df.iter_col::<i64>(dst_index),
            df.time_iter_col(time_index),
        ) {
            let triplets = src
                .map(i64_opt_into_u64_opt)
                .zip(dst.map(i64_opt_into_u64_opt))
                .zip(time);
            load_edges_from_num_iter(
                graph,
                &mut pb,
                triplets,
                prop_iter,
                const_prop_iter,
                shared_constant_properties,
                layer,
            )?;
        } else if let (Some(src), Some(dst), Some(time)) = (
            df.utf8::<i32>(src_index),
            df.utf8::<i32>(dst_index),
            df.time_iter_col(time_index),
        ) {
            let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());

            for (((((src, dst), time), props), const_props), layer) in
                triplets.zip(prop_iter).zip(const_prop_iter).zip(layer)
            {
                if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                    let e = graph.add_edge(time, src, dst, props, layer.as_deref())?;
                    e.add_constant_properties(const_props, layer.as_deref())?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                    }
                }
                let _ = pb.update(1);
            }
        } else if let (Some(src), Some(dst), Some(time)) = (
            df.utf8::<i64>(src_index),
            df.utf8::<i64>(dst_index),
            df.time_iter_col(time_index),
        ) {
            let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());
            for (((((src, dst), time), props), const_props), layer) in
                triplets.zip(prop_iter).zip(const_prop_iter).zip(layer)
            {
                if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                    let e = graph.add_edge(time, src, dst, props, layer.as_deref())?;
                    e.add_constant_properties(const_props, layer.as_deref())?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                    }
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
        Some(df_view.get_index(node_type_col.as_ref()))
    } else {
        None
    };
    let node_type_index = node_type_index.transpose()?;
    let mut pb = build_progress_bar("Loading node properties".to_string(), df_view.num_rows)?;
    for chunk in df_view.chunks {
        let df = chunk?;
        let const_prop_iter =
            combine_properties(constant_properties, &constant_properties_indices, &df)?;

        let node_type: Result<Box<dyn Iterator<Item = Option<&str>>>, GraphError> =
            match (node_type, node_type_index) {
                (None, None) => Ok(Box::new(iter::repeat(None))),
                (Some(node_type), None) => Ok(Box::new(iter::repeat(Some(node_type)))),
                (None, Some(node_type_index)) => {
                    let iter_res: Result<Box<dyn Iterator<Item = Option<&str>>>, GraphError> =
                        if let Some(node_types) = df.utf8::<i32>(node_type_index) {
                            Ok(Box::new(node_types))
                        } else if let Some(node_types) = df.utf8::<i64>(node_type_index) {
                            Ok(Box::new(node_types))
                        } else {
                            Err(GraphError::LoadFailure(
                                "Unable to convert / find node_type column in dataframe."
                                    .to_string(),
                            ))
                        };
                    iter_res
                }
                _ => Err(GraphError::WrongNumOfArgs(
                    "node_type".to_string(),
                    "node_type_col".to_string(),
                )),
            };
        let node_type = node_type?;

        if let Some(node_id) = df.iter_col::<u64>(node_id_index) {
            let iter = node_id.map(|i| i.copied());
            for ((node_id, const_props), node_type) in iter.zip(const_prop_iter).zip(node_type) {
                if let Some(node_id) = node_id {
                    let v = graph
                        .node(node_id)
                        .ok_or(GraphError::NodeIdError(node_id))?;
                    v.add_constant_properties(const_props)?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        v.add_constant_properties(shared_const_props.iter())?;
                    }
                    if let Some(node_type) = node_type {
                        v.set_node_type(node_type)?;
                    }
                }
                let _ = pb.update(1);
            }
        } else if let Some(node_id) = df.iter_col::<i64>(node_id_index) {
            let iter = node_id.map(i64_opt_into_u64_opt);
            for ((node_id, const_props), node_type) in iter.zip(const_prop_iter).zip(node_type) {
                if let Some(node_id) = node_id {
                    let v = graph
                        .node(node_id)
                        .ok_or(GraphError::NodeIdError(node_id))?;
                    v.add_constant_properties(const_props)?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        v.add_constant_properties(shared_const_props.iter())?;
                    }
                    if let Some(node_type) = node_type {
                        v.set_node_type(node_type)?;
                    }
                }
                let _ = pb.update(1);
            }
        } else if let Some(node_id) = df.utf8::<i32>(node_id_index) {
            let iter = node_id.into_iter();
            for ((node_id, const_props), node_type) in iter.zip(const_prop_iter).zip(node_type) {
                if let Some(node_id) = node_id {
                    let v = graph
                        .node(node_id)
                        .ok_or_else(|| GraphError::NodeNameError(node_id.to_owned()))?;
                    v.add_constant_properties(const_props)?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        v.add_constant_properties(shared_const_props.iter())?;
                    }
                    if let Some(node_type) = node_type {
                        v.set_node_type(node_type)?;
                    }
                }
                let _ = pb.update(1);
            }
        } else if let Some(node_id) = df.utf8::<i64>(node_id_index) {
            let iter = node_id.into_iter();
            for ((node_id, const_props), node_type) in iter.zip(const_prop_iter).zip(node_type) {
                if let Some(node_id) = node_id {
                    let v = graph
                        .node(node_id)
                        .ok_or_else(|| GraphError::NodeNameError(node_id.to_owned()))?;
                    v.add_constant_properties(const_props)?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        v.add_constant_properties(shared_const_props.iter())?;
                    }
                    if let Some(node_type) = node_type {
                        v.set_node_type(node_type)?;
                    }
                }
                let _ = pb.update(1);
            }
        } else {
            return Err(GraphError::LoadFailure(
                "node id column must be either u64 or text, time column must be i64. Ensure these contain no NaN, Null or None values.".to_string(),
            ));
        };
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

    for chunk in df_view.chunks {
        let df = chunk?;
        let const_prop_iter =
            combine_properties(constant_properties, &constant_properties_indices, &df)?;

        let layer = lift_layer(layer, layer_index, &df)?;

        if let (Some(src), Some(dst)) =
            (df.iter_col::<u64>(src_index), df.iter_col::<u64>(dst_index))
        {
            let triplets = src.map(|i| i.copied()).zip(dst.map(|i| i.copied()));

            for (((src, dst), const_props), layer) in triplets.zip(const_prop_iter).zip(layer) {
                if let (Some(src), Some(dst)) = (src, dst) {
                    let e = graph
                        .edge(src, dst)
                        .ok_or(GraphError::EdgeIdError { src, dst })?;
                    e.add_constant_properties(const_props, layer.as_deref())?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                    }
                }
                let _ = pb.update(1);
            }
        } else if let (Some(src), Some(dst)) =
            (df.iter_col::<i64>(src_index), df.iter_col::<i64>(dst_index))
        {
            let triplets = src
                .map(i64_opt_into_u64_opt)
                .zip(dst.map(i64_opt_into_u64_opt));

            for (((src, dst), const_props), layer) in triplets.zip(const_prop_iter).zip(layer) {
                if let (Some(src), Some(dst)) = (src, dst) {
                    let e = graph
                        .edge(src, dst)
                        .ok_or(GraphError::EdgeIdError { src, dst })?;
                    e.add_constant_properties(const_props, layer.as_deref())?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                    }
                }
                let _ = pb.update(1);
            }
        } else if let (Some(src), Some(dst)) =
            (df.utf8::<i32>(src_index), df.utf8::<i32>(dst_index))
        {
            let triplets = src.into_iter().zip(dst.into_iter());
            for (((src, dst), const_props), layer) in triplets.zip(const_prop_iter).zip(layer) {
                if let (Some(src), Some(dst)) = (src, dst) {
                    let e = graph
                        .edge(src, dst)
                        .ok_or_else(|| GraphError::EdgeNameError {
                            src: src.to_owned(),
                            dst: dst.to_owned(),
                        })?;
                    e.add_constant_properties(const_props, layer.as_deref())?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                    }
                }
                let _ = pb.update(1);
            }
        } else if let (Some(src), Some(dst)) =
            (df.utf8::<i64>(src_index), df.utf8::<i64>(dst_index))
        {
            let triplets = src.into_iter().zip(dst.into_iter());

            for (((src, dst), const_props), layer) in triplets.zip(const_prop_iter).zip(layer) {
                if let (Some(src), Some(dst)) = (src, dst) {
                    let e = graph
                        .edge(src, dst)
                        .ok_or_else(|| GraphError::EdgeNameError {
                            src: src.to_owned(),
                            dst: dst.to_owned(),
                        })?;
                    e.add_constant_properties(const_props, layer.as_deref())?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                    }
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
