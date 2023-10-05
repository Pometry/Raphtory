use crate::{
    core::utils::errors::GraphError,
    prelude::*,
    python::graph::pandas::{
        dataframe::PretendDF,
        prop_handler::{get_prop_rows, lift_layer},
    },
};
use kdam::tqdm;
use std::collections::HashMap;
pub(crate) fn load_vertices_from_df<'a>(
    df: &'a PretendDF,
    size: usize,
    vertex_id: &str,
    time: &str,
    props: Option<Vec<&str>>,
    const_props: Option<Vec<&str>>,
    shared_const_props: Option<HashMap<String, Prop>>,
    graph: &Graph,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![vertex_id, time];
    cols_to_check.extend(props.as_ref().unwrap_or(&Vec::new()));
    cols_to_check.extend(const_props.as_ref().unwrap_or(&Vec::new()));
    df.check_cols_exist(&cols_to_check)?;

    let (prop_iter, const_prop_iter) = get_prop_rows(df, props, const_props)?;

    if let (Some(vertex_id), Some(time)) = (df.iter_col::<u64>(vertex_id), df.iter_col::<i64>(time))
    {
        let iter = vertex_id.map(|i| i.copied()).zip(time);
        load_vertices_from_num_iter(
            graph,
            size,
            iter,
            prop_iter,
            const_prop_iter,
            shared_const_props,
        )?;
    } else if let (Some(vertex_id), Some(time)) =
        (df.iter_col::<i64>(vertex_id), df.iter_col::<i64>(time))
    {
        let iter = vertex_id.map(i64_opt_into_u64_opt).zip(time);
        load_vertices_from_num_iter(
            graph,
            size,
            iter,
            prop_iter,
            const_prop_iter,
            shared_const_props,
        )?;
    } else if let (Some(vertex_id), Some(time)) =
        (df.utf8::<i32>(vertex_id), df.iter_col::<i64>(time))
    {
        let iter = vertex_id.into_iter().zip(time);
        for (((vertex_id, time), props), const_props) in tqdm!(
            iter.zip(prop_iter).zip(const_prop_iter),
            desc = "Loading vertices",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let (Some(vertex_id), Some(time)) = (vertex_id, time) {
                let v = graph.add_vertex(*time, vertex_id, props)?;
                v.add_constant_properties(const_props)?;
                if let Some(shared_const_props) = &shared_const_props {
                    v.add_constant_properties(shared_const_props.iter())?;
                }
            }
        }
    } else if let (Some(vertex_id), Some(time)) =
        (df.utf8::<i64>(vertex_id), df.iter_col::<i64>(time))
    {
        let iter = vertex_id.into_iter().zip(time);
        for (((vertex_id, time), props), const_props) in tqdm!(
            iter.zip(prop_iter).zip(const_prop_iter),
            desc = "Loading vertices",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let (Some(vertex_id), Some(time)) = (vertex_id, time) {
                let v = graph.add_vertex(*time, vertex_id, props)?;
                v.add_constant_properties(const_props)?;
                if let Some(shared_const_props) = &shared_const_props {
                    v.add_constant_properties(shared_const_props)?;
                }
            }
        }
    } else {
        return Err(GraphError::LoadFailure(
            "vertex id column must be either u64 or text, time column must be i64. Ensure these contain no NaN, Null or None values.".to_string(),
        ));
    }

    Ok(())
}

pub(crate) fn load_edges_from_df<'a, S: AsRef<str>>(
    df: &'a PretendDF,
    size: usize,
    src: &str,
    dst: &str,
    time: &str,
    props: Option<Vec<&str>>,
    const_props: Option<Vec<&str>>,
    shared_const_props: Option<HashMap<String, Prop>>,
    layer: Option<S>,
    layer_in_df: bool,
    graph: &Graph,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![src, dst, time];
    cols_to_check.extend(props.as_ref().unwrap_or(&Vec::new()));
    cols_to_check.extend(const_props.as_ref().unwrap_or(&Vec::new()));
    if layer_in_df {
        if let Some(ref layer) = layer {
            cols_to_check.push(layer.as_ref());
        }
    }
    df.check_cols_exist(&cols_to_check)?;

    let (prop_iter, const_prop_iter) = get_prop_rows(df, props, const_props)?;
    let layer = lift_layer(layer, layer_in_df, df);

    if let (Some(src), Some(dst), Some(time)) = (
        df.iter_col::<u64>(src),
        df.iter_col::<u64>(dst),
        df.iter_col::<i64>(time),
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
            shared_const_props,
            layer,
        )?;
    } else if let (Some(src), Some(dst), Some(time)) = (
        df.iter_col::<i64>(src),
        df.iter_col::<i64>(dst),
        df.iter_col::<i64>(time),
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
            shared_const_props,
            layer,
        )?;
    } else if let (Some(src), Some(dst), Some(time)) = (
        df.utf8::<i32>(src),
        df.utf8::<i32>(dst),
        df.iter_col::<i64>(time),
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
                let e = graph.add_edge(*time, src, dst, props, layer.as_deref())?;
                e.add_constant_properties(const_props, layer.as_deref())?;
                if let Some(shared_const_props) = &shared_const_props {
                    e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
                }
            }
        }
    } else if let (Some(src), Some(dst), Some(time)) = (
        df.utf8::<i64>(src),
        df.utf8::<i64>(dst),
        df.iter_col::<i64>(time),
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
                let e = graph.add_edge(*time, src, dst, props, layer.as_deref())?;
                e.add_constant_properties(const_props, layer.as_deref())?;
                if let Some(shared_const_props) = &shared_const_props {
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

pub(crate) fn load_vertex_props_from_df<'a>(
    df: &'a PretendDF,
    size: usize,
    vertex_id: &str,
    const_props: Option<Vec<&str>>,
    shared_const_props: Option<HashMap<String, Prop>>,
    graph: &Graph,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![vertex_id];
    cols_to_check.extend(const_props.as_ref().unwrap_or(&Vec::new()));
    df.check_cols_exist(&cols_to_check)?;

    let (_, const_prop_iter) = get_prop_rows(df, None, const_props)?;

    if let Some(vertex_id) = df.iter_col::<u64>(vertex_id) {
        let iter = vertex_id.map(|i| i.copied());
        for (vertex_id, const_props) in tqdm!(
            iter.zip(const_prop_iter),
            desc = "Loading vertex properties",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let Some(vertex_id) = vertex_id {
                let v = graph
                    .vertex(vertex_id)
                    .ok_or(GraphError::VertexIdError(vertex_id))?;
                v.add_constant_properties(const_props)?;
                if let Some(shared_const_props) = &shared_const_props {
                    v.add_constant_properties(shared_const_props.iter())?;
                }
            }
        }
    } else if let Some(vertex_id) = df.iter_col::<i64>(vertex_id) {
        let iter = vertex_id.map(i64_opt_into_u64_opt);
        for (vertex_id, const_props) in tqdm!(
            iter.zip(const_prop_iter),
            desc = "Loading vertex properties",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let Some(vertex_id) = vertex_id {
                let v = graph
                    .vertex(vertex_id)
                    .ok_or(GraphError::VertexIdError(vertex_id))?;
                v.add_constant_properties(const_props)?;
                if let Some(shared_const_props) = &shared_const_props {
                    v.add_constant_properties(shared_const_props.iter())?;
                }
            }
        }
    } else if let Some(vertex_id) = df.utf8::<i32>(vertex_id) {
        let iter = vertex_id.into_iter();
        for (vertex_id, const_props) in tqdm!(
            iter.zip(const_prop_iter),
            desc = "Loading vertex properties",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let Some(vertex_id) = vertex_id {
                let v = graph
                    .vertex(vertex_id)
                    .ok_or_else(|| GraphError::VertexNameError(vertex_id.to_owned()))?;
                v.add_constant_properties(const_props)?;
                if let Some(shared_const_props) = &shared_const_props {
                    v.add_constant_properties(shared_const_props.iter())?;
                }
            }
        }
    } else if let Some(vertex_id) = df.utf8::<i64>(vertex_id) {
        let iter = vertex_id.into_iter();
        for (vertex_id, const_props) in tqdm!(
            iter.zip(const_prop_iter),
            desc = "Loading vertex properties",
            total = size,
            animation = kdam::Animation::FillUp,
            unit_scale = true
        ) {
            if let Some(vertex_id) = vertex_id {
                let v = graph
                    .vertex(vertex_id)
                    .ok_or_else(|| GraphError::VertexNameError(vertex_id.to_owned()))?;
                v.add_constant_properties(const_props)?;
                if let Some(shared_const_props) = &shared_const_props {
                    v.add_constant_properties(shared_const_props.iter())?;
                }
            }
        }
    } else {
        return Err(GraphError::LoadFailure(
            "vertex id column must be either u64 or text, time column must be i64. Ensure these contain no NaN, Null or None values.".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn load_edges_props_from_df<'a, S: AsRef<str>>(
    df: &'a PretendDF,
    size: usize,
    src: &str,
    dst: &str,
    const_props: Option<Vec<&str>>,
    shared_const_props: Option<HashMap<String, Prop>>,
    layer: Option<S>,
    layer_in_df: bool,
    graph: &Graph,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![src, dst];
    if layer_in_df {
        if let Some(ref layer) = layer {
            cols_to_check.push(layer.as_ref());
        }
    }
    cols_to_check.extend(const_props.as_ref().unwrap_or(&Vec::new()));
    df.check_cols_exist(&cols_to_check)?;

    let (_, const_prop_iter) = get_prop_rows(df, None, const_props)?;
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
                if let Some(shared_const_props) = &shared_const_props {
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
                if let Some(shared_const_props) = &shared_const_props {
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
                if let Some(shared_const_props) = &shared_const_props {
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
                if let Some(shared_const_props) = &shared_const_props {
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
    I: Iterator<Item = ((Option<u64>, Option<u64>), Option<&'a i64>)>,
    PI: Iterator<Item = Vec<(S, Prop)>>,
    IL: Iterator<Item = Option<String>>,
>(
    graph: &Graph,
    size: usize,
    edges: I,
    props: PI,
    const_props: PI,
    shared_const_props: Option<HashMap<String, Prop>>,
    layer: IL,
) -> Result<(), GraphError> {
    for (((((src, dst), time), edge_props), const_props), layer) in tqdm!(
        edges.zip(props).zip(const_props).zip(layer),
        desc = "Loading edges",
        total = size,
        animation = kdam::Animation::FillUp,
        unit_scale = true
    ) {
        if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
            let e = graph.add_edge(*time, src, dst, edge_props, layer.as_deref())?;
            e.add_constant_properties(const_props, layer.as_deref())?;
            if let Some(shared_const_props) = &shared_const_props {
                e.add_constant_properties(shared_const_props.iter(), layer.as_deref())?;
            }
        }
    }
    Ok(())
}

fn load_vertices_from_num_iter<
    'a,
    S: AsRef<str>,
    I: Iterator<Item = (Option<u64>, Option<&'a i64>)>,
    PI: Iterator<Item = Vec<(S, Prop)>>,
>(
    graph: &Graph,
    size: usize,
    vertices: I,
    props: PI,
    const_props: PI,
    shared_const_props: Option<HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    for (((vertex, time), props), const_props) in tqdm!(
        vertices.zip(props).zip(const_props),
        desc = "Loading vertices",
        total = size,
        animation = kdam::Animation::FillUp,
        unit_scale = true
    ) {
        if let (Some(v), Some(t), props, const_props) = (vertex, time, props, const_props) {
            let v = graph.add_vertex(*t, v, props)?;
            v.add_constant_properties(const_props)?;

            if let Some(shared_const_props) = &shared_const_props {
                v.add_constant_properties(shared_const_props.iter())?;
            }
        }
    }
    Ok(())
}
