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
#[cfg(feature = "python")]
use kdam::tqdm;
use std::{collections::HashMap, iter};

#[cfg(feature = "python")]
macro_rules! maybe_tqdm {
    ($iter:expr, $size:expr, $desc:literal) => {
        tqdm!(
            $iter,
            desc = "Loading nodes",
            total = $size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        )
    };
}

#[cfg(not(feature = "python"))]
macro_rules! maybe_tqdm {
    ($iter:expr, $size:expr, $desc:literal) => {
        $iter
    };
}

pub(crate) fn load_nodes_from_df<
    'a,
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    node_id: &str,
    time: &str,
    properties: Option<&[&str]>,
    constant_properties: Option<&[&str]>,
    shared_constant_properties: Option<&HashMap<String, Prop>>,
    node_type: Option<&str>,
    node_type_in_df: bool,
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

    let node_type_index = node_type
        .filter(|_| node_type_in_df)
        .map(|node_type| df_view.get_index(node_type))
        .transpose()?;
    let node_id_index = df_view.get_index(node_id)?;
    let time_index = df_view.get_index(time)?;

    for chunk in df_view.chunks {
        let df = chunk?;
        let size = df.get_inner_size();
        let prop_iter = combine_properties(properties, &properties_indices, &df)?;
        let const_prop_iter =
            combine_properties(constant_properties, &constant_properties_indices, &df)?;

        let node_type: Box<dyn Iterator<Item = Option<&str>>> = match node_type {
            Some(node_type) => match node_type_index {
                Some(index) => {
                    let iter_res: Result<Box<dyn Iterator<Item = Option<&str>>>, GraphError> =
                        if let Some(node_types) = df.utf8::<i32>(index) {
                            Ok(Box::new(node_types))
                        } else if let Some(node_types) = df.utf8::<i64>(index) {
                            Ok(Box::new(node_types))
                        } else {
                            Err(GraphError::LoadFailure(
                                "Unable to convert / find node_type column in dataframe."
                                    .to_string(),
                            ))
                        };
                    iter_res?
                }
                None => Box::new(iter::repeat(Some(node_type))),
            },
            None => Box::new(iter::repeat(None)),
        };

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
                size,
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
                size,
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

            let iter = maybe_tqdm!(
                iter.zip(prop_iter).zip(const_prop_iter),
                size,
                "Loading nodes"
            );

            for (((node_id, time, n_t), props), const_props) in iter {
                if let (Some(node_id), Some(time), n_t) = (node_id, time, n_t) {
                    let actual_type = extract_out_default_type(n_t);
                    let v = graph.add_node(time, node_id, props, actual_type)?;
                    v.add_constant_properties(const_props)?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        v.add_constant_properties(shared_const_props.iter())?;
                    }
                }
            }
        } else if let (Some(node_id), Some(time)) =
            (df.utf8::<i64>(node_id_index), df.time_iter_col(time_index))
        {
            let iter = node_id.into_iter().zip(time);
            let iter = iter
                .zip(node_type)
                .map(|((node_id, time), n_t)| (node_id, time, n_t));

            let iter = maybe_tqdm!(
                iter.zip(prop_iter).zip(const_prop_iter),
                size,
                "Loading nodes"
            );

            for (((node_id, time, n_t), props), const_props) in iter {
                let actual_type = extract_out_default_type(n_t);
                if let (Some(node_id), Some(time), n_t) = (node_id, time, actual_type) {
                    let v = graph.add_node(time, node_id, props, n_t)?;
                    v.add_constant_properties(const_props)?;
                    if let Some(shared_const_props) = shared_constant_properties {
                        v.add_constant_properties(shared_const_props)?;
                    }
                }
            }
        } else {
            return Err(GraphError::LoadFailure(
                "node id column must be either u64 or text, time column must be i64. Ensure these contain no NaN, Null or None values.".to_string(),
            ));
        };
    }

    Ok(())
}

fn extract_out_default_type(n_t: Option<&str>) -> Option<&str> {
    if n_t == Some("_default") {
        None
    } else {
        n_t
    }
}

pub(crate) fn load_edges_from_df<
    'a,
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    size: usize,
    time: &str,
    src: &str,
    dst: &str,
    properties: Option<&[&str]>,
    constant_properties: Option<&[&str]>,
    shared_constant_properties: Option<&HashMap<String, Prop>>,
    layer_name: Option<&str>,
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

    for chunk in df_view.chunks {
        let df = chunk?;
        let prop_iter = combine_properties(properties, &properties_indices, &df)?;
        let const_prop_iter =
            combine_properties(constant_properties, &constant_properties_indices, &df)?;

        let layer = lift_layer(layer_name, layer_index, &df)?;

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
                size,
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
                size,
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

            let iter = maybe_tqdm!(
                triplets.zip(prop_iter).zip(const_prop_iter).zip(layer),
                size,
                "Loading edges"
            );

            for (((((src, dst), time), props), const_props), layer) in iter {
                if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                    let e = graph.add_edge(time, src, dst, props, layer.as_deref())?;
                    e.add_constant_properties(const_props, layer.as_deref())?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                    }
                }
            }
        } else if let (Some(src), Some(dst), Some(time)) = (
            df.utf8::<i64>(src_index),
            df.utf8::<i64>(dst_index),
            df.time_iter_col(time_index),
        ) {
            let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());
            let iter = maybe_tqdm!(
                triplets.zip(prop_iter).zip(const_prop_iter).zip(layer),
                size,
                "Loading edges"
            );

            for (((((src, dst), time), props), const_props), layer) in iter {
                if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                    let e = graph.add_edge(time, src, dst, props, layer.as_deref())?;
                    e.add_constant_properties(const_props, layer.as_deref())?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                    }
                }
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

pub(crate) fn load_edges_deletions_from_df<
    'a,
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps + DeletionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    size: usize,
    time: &str,
    src: &str,
    dst: &str,
    layer_name: Option<&str>,
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

    for chunk in df_view.chunks {
        let df = chunk?;
        let layer = lift_layer(layer_name, layer_index, &df)?;

        if let (Some(src), Some(dst), Some(time)) = (
            df.iter_col::<u64>(src_index),
            df.iter_col::<u64>(dst_index),
            df.time_iter_col(time_index),
        ) {
            let triplets = src
                .map(|i| i.copied())
                .zip(dst.map(|i| i.copied()))
                .zip(time);

            let iter = maybe_tqdm!(triplets.zip(layer), size, "Loading edges");

            for (((src, dst), time), layer) in iter {
                if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                    graph.delete_edge(time, src, dst, layer.as_deref())?;
                }
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

            let iter = maybe_tqdm!(triplets.zip(layer), size, "Loading edges");

            for (((src, dst), time), layer) in iter {
                if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                    graph.delete_edge(time, src, dst, layer.as_deref())?;
                }
            }
        } else if let (Some(src), Some(dst), Some(time)) = (
            df.utf8::<i32>(src_index),
            df.utf8::<i32>(dst_index),
            df.time_iter_col(time_index),
        ) {
            let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());
            let iter = maybe_tqdm!(triplets.zip(layer), size, "Loading edges");

            for (((src, dst), time), layer) in iter {
                if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                    graph.delete_edge(time, src, dst, layer.as_deref())?;
                }
            }
        } else if let (Some(src), Some(dst), Some(time)) = (
            df.utf8::<i64>(src_index),
            df.utf8::<i64>(dst_index),
            df.time_iter_col(time_index),
        ) {
            let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());
            let iter = maybe_tqdm!(triplets.zip(layer), size, "Loading edges");

            for (((src, dst), time), layer) in iter {
                if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                    graph.delete_edge(time, src, dst, layer.as_deref())?;
                }
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
    size: usize,
    node_id: &str,
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

    for chunk in df_view.chunks {
        let df = chunk?;
        let const_prop_iter =
            combine_properties(constant_properties, &constant_properties_indices, &df)?;

        if let Some(node_id) = df.iter_col::<u64>(node_id_index) {
            let iter = node_id.map(|i| i.copied());
            let iter = maybe_tqdm!(iter.zip(const_prop_iter), size, "Loading node properties");

            for (node_id, const_props) in iter {
                if let Some(node_id) = node_id {
                    let v = graph
                        .node(node_id)
                        .ok_or(GraphError::NodeIdError(node_id))?;
                    v.add_constant_properties(const_props)?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        v.add_constant_properties(shared_const_props.iter())?;
                    }
                }
            }
        } else if let Some(node_id) = df.iter_col::<i64>(node_id_index) {
            let iter = node_id.map(i64_opt_into_u64_opt);
            let iter = maybe_tqdm!(iter.zip(const_prop_iter), size, "Loading node properties");

            for (node_id, const_props) in iter {
                if let Some(node_id) = node_id {
                    let v = graph
                        .node(node_id)
                        .ok_or(GraphError::NodeIdError(node_id))?;
                    v.add_constant_properties(const_props)?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        v.add_constant_properties(shared_const_props.iter())?;
                    }
                }
            }
        } else if let Some(node_id) = df.utf8::<i32>(node_id_index) {
            let iter = node_id.into_iter();
            let iter = maybe_tqdm!(iter.zip(const_prop_iter), size, "Loading node properties");

            for (node_id, const_props) in iter {
                if let Some(node_id) = node_id {
                    let v = graph
                        .node(node_id)
                        .ok_or_else(|| GraphError::NodeNameError(node_id.to_owned()))?;
                    v.add_constant_properties(const_props)?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        v.add_constant_properties(shared_const_props.iter())?;
                    }
                }
            }
        } else if let Some(node_id) = df.utf8::<i64>(node_id_index) {
            let iter = node_id.into_iter();
            let iter = maybe_tqdm!(iter.zip(const_prop_iter), size, "Loading node properties");

            for (node_id, const_props) in iter {
                if let Some(node_id) = node_id {
                    let v = graph
                        .node(node_id)
                        .ok_or_else(|| GraphError::NodeNameError(node_id.to_owned()))?;
                    v.add_constant_properties(const_props)?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        v.add_constant_properties(shared_const_props.iter())?;
                    }
                }
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
    size: usize,
    src: &str,
    dst: &str,
    constant_properties: Option<&[&str]>,
    shared_constant_properties: Option<&HashMap<String, Prop>>,
    layer_name: Option<&str>,
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

    for chunk in df_view.chunks {
        let df = chunk?;
        let const_prop_iter =
            combine_properties(constant_properties, &constant_properties_indices, &df)?;

        let layer = lift_layer(layer_name, layer_index, &df)?;

        if let (Some(src), Some(dst)) =
            (df.iter_col::<u64>(src_index), df.iter_col::<u64>(dst_index))
        {
            let triplets = src.map(|i| i.copied()).zip(dst.map(|i| i.copied()));
            let iter = maybe_tqdm!(
                triplets.zip(const_prop_iter).zip(layer),
                size,
                "Loading edge properties"
            );

            for (((src, dst), const_props), layer) in iter {
                if let (Some(src), Some(dst)) = (src, dst) {
                    let e = graph
                        .edge(src, dst)
                        .ok_or(GraphError::EdgeIdError { src, dst })?;
                    e.add_constant_properties(const_props, layer.as_deref())?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                    }
                }
            }
        } else if let (Some(src), Some(dst)) =
            (df.iter_col::<i64>(src_index), df.iter_col::<i64>(dst_index))
        {
            let triplets = src
                .map(i64_opt_into_u64_opt)
                .zip(dst.map(i64_opt_into_u64_opt));
            let iter = maybe_tqdm!(
                triplets.zip(const_prop_iter).zip(layer),
                size,
                "Loading edge properties"
            );

            for (((src, dst), const_props), layer) in iter {
                if let (Some(src), Some(dst)) = (src, dst) {
                    let e = graph
                        .edge(src, dst)
                        .ok_or(GraphError::EdgeIdError { src, dst })?;
                    e.add_constant_properties(const_props, layer.as_deref())?;
                    if let Some(shared_const_props) = &shared_constant_properties {
                        e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                    }
                }
            }
        } else if let (Some(src), Some(dst)) =
            (df.utf8::<i32>(src_index), df.utf8::<i32>(dst_index))
        {
            let triplets = src.into_iter().zip(dst.into_iter());
            let iter = maybe_tqdm!(
                triplets.zip(const_prop_iter).zip(layer),
                size,
                "Loading edge properties"
            );

            for (((src, dst), const_props), layer) in iter {
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
            }
        } else if let (Some(src), Some(dst)) =
            (df.utf8::<i64>(src_index), df.utf8::<i64>(dst_index))
        {
            let triplets = src.into_iter().zip(dst.into_iter());
            let iter = maybe_tqdm!(
                triplets.zip(const_prop_iter).zip(layer),
                size,
                "Loading edge properties"
            );

            for (((src, dst), const_props), layer) in iter {
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
    size: usize,
    edges: I,
    properties: PI,
    constant_properties: PI,
    shared_constant_properties: Option<&HashMap<String, Prop>>,
    layer: IL,
) -> Result<(), GraphError> {
    let iter = maybe_tqdm!(
        edges.zip(properties).zip(constant_properties).zip(layer),
        size,
        "Loading edges"
    );
    for (((((src, dst), time), edge_props), const_props), layer) in iter {
        if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
            let e = graph.add_edge(time, src, dst, edge_props, layer.as_deref())?;
            e.add_constant_properties(const_props, layer.as_deref())?;
            if let Some(shared_const_props) = &shared_constant_properties {
                e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
            }
        }
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
    size: usize,
    nodes: I,
    properties: PI,
    constant_properties: PI,
    shared_constant_properties: Option<&HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    let iter = maybe_tqdm!(
        nodes.zip(properties).zip(constant_properties),
        size,
        "Loading nodes"
    );
    for (((node, time, node_type), props), const_props) in iter {
        if let (Some(v), Some(t), n_t, props, const_props) =
            (node, time, node_type, props, const_props)
        {
            let actual_node_type = extract_out_default_type(n_t);
            let v = graph.add_node(t, v, props, actual_node_type)?;
            v.add_constant_properties(const_props)?;

            if let Some(shared_const_props) = &shared_constant_properties {
                v.add_constant_properties(shared_const_props.iter())?;
            }
        }
    }
    Ok(())
}
