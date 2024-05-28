use crate::{
    core::{entities::graph::tgraph::InternalGraph, utils::errors::GraphError},
    db::api::mutation::AdditionOps,
    prelude::*,
    python::graph::pandas::{
        dataframe::PretendDF,
        prop_handler::{get_prop_rows, lift_layer},
    },
};
use kdam::tqdm;
use std::{collections::HashMap, iter};

pub(crate) fn load_nodes_from_df<'a>(
    df: &'a PretendDF,
    size: usize,
    node_id: &str,
    time: &str,
    properties: Option<Vec<&str>>,
    const_properties: Option<Vec<&str>>,
    shared_const_properties: Option<HashMap<String, Prop>>,
    node_type: Option<&str>,
    node_type_in_df: bool,
    graph: &InternalGraph,
) -> Result<(), GraphError> {
    let (prop_iter, const_prop_iter) = get_prop_rows(df, properties, const_properties)?;

    let node_type: Box<dyn Iterator<Item = Option<&str>>> = match node_type {
        Some(node_type) => {
            if node_type_in_df {
                let iter_res: Result<Box<dyn Iterator<Item = Option<&str>>>, GraphError> =
                    if let Some(node_types) = df.utf8::<i32>(node_type) {
                        Ok(Box::new(node_types))
                    } else if let Some(node_types) = df.utf8::<i64>(node_type) {
                        Ok(Box::new(node_types))
                    } else {
                        Err(GraphError::LoadFailure(
                            "Unable to convert / find node_type column in dataframe.".to_string(),
                        ))
                    };
                iter_res?
            } else {
                Box::new(std::iter::repeat(Some(node_type)))
            }
        }
        None => Box::new(iter::repeat(None)),
    };

    if let (Some(node_id), Some(time)) = (df.iter_col::<u64>(node_id), df.time_iter_col(time)) {
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
            shared_const_properties,
        )?;
    } else if let (Some(node_id), Some(time)) =
        (df.iter_col::<i64>(node_id), df.time_iter_col(time))
    {
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
            shared_const_properties,
        )?;
    } else if let (Some(node_id), Some(time)) = (df.utf8::<i32>(node_id), df.time_iter_col(time)) {
        let iter = node_id.into_iter().zip(time);
        let iter = iter
            .zip(node_type)
            .map(|((node_id, time), n_t)| (node_id, time, n_t));

        for (((node_id, time, n_t), props), const_props) in tqdm!(
            iter.zip(prop_iter).zip(const_prop_iter),
            desc = "Loading nodes",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let (Some(node_id), Some(time), n_t) = (node_id, time, n_t) {
                let actual_type = extract_out_default_type(n_t);
                let v = graph.add_node(time, node_id, props, actual_type)?;
                v.add_constant_properties(const_props)?;
                if let Some(shared_const_props) = &shared_const_properties {
                    v.add_constant_properties(shared_const_props.iter())?;
                }
            }
        }
    } else if let (Some(node_id), Some(time)) = (df.utf8::<i64>(node_id), df.time_iter_col(time)) {
        let iter = node_id.into_iter().zip(time);
        let iter = iter
            .zip(node_type)
            .map(|((node_id, time), n_t)| (node_id, time, n_t));

        for (((node_id, time, n_t), props), const_props) in tqdm!(
            iter.zip(prop_iter).zip(const_prop_iter),
            desc = "Loading nodes",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            let actual_type = extract_out_default_type(n_t);
            if let (Some(node_id), Some(time), n_t) = (node_id, time, actual_type) {
                let v = graph.add_node(time, node_id, props, n_t)?;
                v.add_constant_properties(const_props)?;
                if let Some(shared_const_props) = &shared_const_properties {
                    v.add_constant_properties(shared_const_props)?;
                }
            }
        }
    } else {
        return Err(GraphError::LoadFailure(
            "node id column must be either u64 or text, time column must be i64. Ensure these contain no NaN, Null or None values.".to_string(),
        ));
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

pub(crate) fn load_edges_from_df<'a, S: AsRef<str>>(
    df: &'a PretendDF,
    size: usize,
    src: &str,
    dst: &str,
    time: &str,
    properties: Option<Vec<&str>>,
    const_properties: Option<Vec<&str>>,
    shared_const_properties: Option<HashMap<String, Prop>>,
    layer: Option<S>,
    layer_in_df: bool,
    graph: &InternalGraph,
) -> Result<(), GraphError> {
    let (prop_iter, const_prop_iter) = get_prop_rows(df, properties, const_properties)?;
    let layer = lift_layer(layer, layer_in_df, df);

    if let (Some(src), Some(dst), Some(time)) = (
        df.iter_col::<u64>(src),
        df.iter_col::<u64>(dst),
        df.time_iter_col(time),
    ) {
        let triplets = src
            .map(|i| i.copied())
            .zip(dst.map(|i| i.copied()))
            .zip(time);
        load_edges_from_num_iter(
            &graph,
            size,
            triplets,
            prop_iter,
            const_prop_iter,
            shared_const_properties,
            layer,
        )?;
    } else if let (Some(src), Some(dst), Some(time)) = (
        df.iter_col::<i64>(src),
        df.iter_col::<i64>(dst),
        df.time_iter_col(time),
    ) {
        let triplets = src
            .map(i64_opt_into_u64_opt)
            .zip(dst.map(i64_opt_into_u64_opt))
            .zip(time);
        load_edges_from_num_iter(
            &graph,
            size,
            triplets,
            prop_iter,
            const_prop_iter,
            shared_const_properties,
            layer,
        )?;
    } else if let (Some(src), Some(dst), Some(time)) = (
        df.utf8::<i32>(src),
        df.utf8::<i32>(dst),
        df.time_iter_col(time),
    ) {
        let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());

        for (((((src, dst), time), props), const_props), layer) in tqdm!(
            triplets.zip(prop_iter).zip(const_prop_iter).zip(layer),
            desc = "Loading edges",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                let e = graph.add_edge(time, src, dst, props, layer.as_deref())?;
                e.add_constant_properties(const_props, layer.as_deref())?;
                if let Some(shared_const_props) = &shared_const_properties {
                    e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                }
            }
        }
    } else if let (Some(src), Some(dst), Some(time)) = (
        df.utf8::<i64>(src),
        df.utf8::<i64>(dst),
        df.time_iter_col(time),
    ) {
        let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());
        for (((((src, dst), time), props), const_props), layer) in tqdm!(
            triplets.zip(prop_iter).zip(const_prop_iter).zip(layer),
            desc = "Loading edges",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                let e = graph.add_edge(time, src, dst, props, layer.as_deref())?;
                e.add_constant_properties(const_props, layer.as_deref())?;
                if let Some(shared_const_props) = &shared_const_properties {
                    e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                }
            }
        }
    } else {
        return Err(GraphError::LoadFailure(
            "Source and Target columns must be either u64 or text, Time column must be i64. Ensure these contain no NaN, Null or None values."
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn load_edges_deletions_from_df<'a, S: AsRef<str>>(
    df: &'a PretendDF,
    size: usize,
    src: &str,
    dst: &str,
    time: &str,
    layer: Option<S>,
    layer_in_df: bool,
    graph: &InternalGraph,
) -> Result<(), GraphError> {
    let layer = lift_layer(layer, layer_in_df, df);

    if let (Some(src), Some(dst), Some(time)) = (
        df.iter_col::<u64>(src),
        df.iter_col::<u64>(dst),
        df.time_iter_col(time),
    ) {
        let triplets = src
            .map(|i| i.copied())
            .zip(dst.map(|i| i.copied()))
            .zip(time);
        for (((src, dst), time), layer) in tqdm!(
            triplets.zip(layer),
            desc = "Loading edges",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                graph.delete_edge(time, src, dst, layer.as_deref())?;
            }
        }
    } else if let (Some(src), Some(dst), Some(time)) = (
        df.iter_col::<i64>(src),
        df.iter_col::<i64>(dst),
        df.time_iter_col(time),
    ) {
        let triplets = src
            .map(i64_opt_into_u64_opt)
            .zip(dst.map(i64_opt_into_u64_opt))
            .zip(time);
        for (((src, dst), time), layer) in tqdm!(
            triplets.zip(layer),
            desc = "Loading edges",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                graph.delete_edge(time, src, dst, layer.as_deref())?;
            }
        }
    } else if let (Some(src), Some(dst), Some(time)) = (
        df.utf8::<i32>(src),
        df.utf8::<i32>(dst),
        df.time_iter_col(time),
    ) {
        let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());
        for (((src, dst), time), layer) in tqdm!(
            triplets.zip(layer),
            desc = "Loading edges",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                graph.delete_edge(time, src, dst, layer.as_deref())?;
            }
        }
    } else if let (Some(src), Some(dst), Some(time)) = (
        df.utf8::<i64>(src),
        df.utf8::<i64>(dst),
        df.time_iter_col(time),
    ) {
        let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());
        for (((src, dst), time), layer) in tqdm!(
            triplets.zip(layer),
            desc = "Loading edges",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                graph.delete_edge(time, src, dst, layer.as_deref())?;
            }
        }
    } else {
        return Err(GraphError::LoadFailure(
            "Source and Target columns must be either u64 or text, Time column must be i64. Ensure these contain no NaN, Null or None values."
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn load_node_props_from_df<'a>(
    df: &'a PretendDF,
    size: usize,
    node_id: &str,
    const_properties: Option<Vec<&str>>,
    shared_const_properties: Option<HashMap<String, Prop>>,
    graph: &InternalGraph,
) -> Result<(), GraphError> {
    let (_, const_prop_iter) = get_prop_rows(df, None, const_properties)?;

    if let Some(node_id) = df.iter_col::<u64>(node_id) {
        let iter = node_id.map(|i| i.copied());
        for (node_id, const_props) in tqdm!(
            iter.zip(const_prop_iter),
            desc = "Loading node properties",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let Some(node_id) = node_id {
                let v = graph
                    .node(node_id)
                    .ok_or(GraphError::NodeIdError(node_id))?;
                v.add_constant_properties(const_props)?;
                if let Some(shared_const_props) = &shared_const_properties {
                    v.add_constant_properties(shared_const_props.iter())?;
                }
            }
        }
    } else if let Some(node_id) = df.iter_col::<i64>(node_id) {
        let iter = node_id.map(i64_opt_into_u64_opt);
        for (node_id, const_props) in tqdm!(
            iter.zip(const_prop_iter),
            desc = "Loading node properties",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let Some(node_id) = node_id {
                let v = graph
                    .node(node_id)
                    .ok_or(GraphError::NodeIdError(node_id))?;
                v.add_constant_properties(const_props)?;
                if let Some(shared_const_props) = &shared_const_properties {
                    v.add_constant_properties(shared_const_props.iter())?;
                }
            }
        }
    } else if let Some(node_id) = df.utf8::<i32>(node_id) {
        let iter = node_id.into_iter();
        for (node_id, const_props) in tqdm!(
            iter.zip(const_prop_iter),
            desc = "Loading node properties",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let Some(node_id) = node_id {
                let v = graph
                    .node(node_id)
                    .ok_or_else(|| GraphError::NodeNameError(node_id.to_owned()))?;
                v.add_constant_properties(const_props)?;
                if let Some(shared_const_props) = &shared_const_properties {
                    v.add_constant_properties(shared_const_props.iter())?;
                }
            }
        }
    } else if let Some(node_id) = df.utf8::<i64>(node_id) {
        let iter = node_id.into_iter();
        for (node_id, const_props) in tqdm!(
            iter.zip(const_prop_iter),
            desc = "Loading node properties",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let Some(node_id) = node_id {
                let v = graph
                    .node(node_id)
                    .ok_or_else(|| GraphError::NodeNameError(node_id.to_owned()))?;
                v.add_constant_properties(const_props)?;
                if let Some(shared_const_props) = &shared_const_properties {
                    v.add_constant_properties(shared_const_props.iter())?;
                }
            }
        }
    } else {
        return Err(GraphError::LoadFailure(
            "node id column must be either u64 or text, time column must be i64. Ensure these contain no NaN, Null or None values.".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn load_edges_props_from_df<'a, S: AsRef<str>>(
    df: &'a PretendDF,
    size: usize,
    src: &str,
    dst: &str,
    const_properties: Option<Vec<&str>>,
    shared_const_properties: Option<HashMap<String, Prop>>,
    layer: Option<S>,
    layer_in_df: bool,
    graph: &InternalGraph,
) -> Result<(), GraphError> {
    let (_, const_prop_iter) = get_prop_rows(df, None, const_properties)?;
    let layer = lift_layer(layer, layer_in_df, df);

    if let (Some(src), Some(dst)) = (df.iter_col::<u64>(src), df.iter_col::<u64>(dst)) {
        let triplets = src.map(|i| i.copied()).zip(dst.map(|i| i.copied()));

        for (((src, dst), const_props), layer) in tqdm!(
            triplets.zip(const_prop_iter).zip(layer),
            desc = "Loading edge properties",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let (Some(src), Some(dst)) = (src, dst) {
                let e = graph
                    .edge(src, dst)
                    .ok_or(GraphError::EdgeIdError { src, dst })?;
                e.add_constant_properties(const_props, layer.as_deref())?;
                if let Some(shared_const_props) = &shared_const_properties {
                    e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                }
            }
        }
    } else if let (Some(src), Some(dst)) = (df.iter_col::<i64>(src), df.iter_col::<i64>(dst)) {
        let triplets = src
            .map(i64_opt_into_u64_opt)
            .zip(dst.map(i64_opt_into_u64_opt));
        for (((src, dst), const_props), layer) in tqdm!(
            triplets.zip(const_prop_iter).zip(layer),
            desc = "Loading edge properties",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let (Some(src), Some(dst)) = (src, dst) {
                let e = graph
                    .edge(src, dst)
                    .ok_or(GraphError::EdgeIdError { src, dst })?;
                e.add_constant_properties(const_props, layer.as_deref())?;
                if let Some(shared_const_props) = &shared_const_properties {
                    e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                }
            }
        }
    } else if let (Some(src), Some(dst)) = (df.utf8::<i32>(src), df.utf8::<i32>(dst)) {
        let triplets = src.into_iter().zip(dst.into_iter());
        for (((src, dst), const_props), layer) in tqdm!(
            triplets.zip(const_prop_iter).zip(layer),
            desc = "Loading edge properties",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let (Some(src), Some(dst)) = (src, dst) {
                let e = graph
                    .edge(src, dst)
                    .ok_or_else(|| GraphError::EdgeNameError {
                        src: src.to_owned(),
                        dst: dst.to_owned(),
                    })?;
                e.add_constant_properties(const_props, layer.as_deref())?;
                if let Some(shared_const_props) = &shared_const_properties {
                    e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                }
            }
        }
    } else if let (Some(src), Some(dst)) = (df.utf8::<i64>(src), df.utf8::<i64>(dst)) {
        let triplets = src.into_iter().zip(dst.into_iter());
        for (((src, dst), const_props), layer) in tqdm!(
            triplets.zip(const_prop_iter).zip(layer),
            desc = "Loading edge properties",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let (Some(src), Some(dst)) = (src, dst) {
                let e = graph
                    .edge(src, dst)
                    .ok_or_else(|| GraphError::EdgeNameError {
                        src: src.to_owned(),
                        dst: dst.to_owned(),
                    })?;
                e.add_constant_properties(const_props, layer.as_deref())?;
                if let Some(shared_const_props) = &shared_const_properties {
                    e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                }
            }
        }
    } else {
        return Err(GraphError::LoadFailure(
            "Source and Target columns must be either u64 or text, Time column must be i64. Ensure these contain no NaN, Null or None values."
                .to_string(),
        ));
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
>(
    graph: &InternalGraph,
    size: usize,
    edges: I,
    properties: PI,
    const_properties: PI,
    shared_const_properties: Option<HashMap<String, Prop>>,
    layer: IL,
) -> Result<(), GraphError> {
    for (((((src, dst), time), edge_props), const_props), layer) in tqdm!(
        edges.zip(properties).zip(const_properties).zip(layer),
        desc = "Loading edges",
        total = size,
        animation = kdam::Animation::FillUp,
        unit_scale = true
    ) {
        if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
            let e = graph.add_edge(time, src, dst, edge_props, layer.as_deref())?;
            e.add_constant_properties(const_props, layer.as_deref())?;
            if let Some(shared_const_props) = &shared_const_properties {
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
>(
    graph: &InternalGraph,
    size: usize,
    nodes: I,
    properties: PI,
    const_properties: PI,
    shared_const_properties: Option<HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    for (((node, time, node_type), props), const_props) in tqdm!(
        nodes.zip(properties).zip(const_properties),
        desc = "Loading nodes",
        total = size,
        animation = kdam::Animation::FillUp,
        unit_scale = true
    ) {
        if let (Some(v), Some(t), n_t, props, const_props) =
            (node, time, node_type, props, const_props)
        {
            let actual_node_type = extract_out_default_type(n_t);
            let v = graph.add_node(t, v, props, actual_node_type)?;
            v.add_constant_properties(const_props)?;

            if let Some(shared_const_props) = &shared_const_properties {
                v.add_constant_properties(shared_const_props.iter())?;
            }
        }
    }
    Ok(())
}
