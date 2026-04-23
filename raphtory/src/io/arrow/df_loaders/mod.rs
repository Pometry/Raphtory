use crate::{
    db::api::view::StaticGraphViewOps,
    errors::{into_graph_err, GraphError},
    io::arrow::{
        dataframe::{DFChunk, DFView, SecondaryIndexCol},
        df_loaders::edges::ColumnNames,
        node_col::NodeCol,
        prop_handler::*,
    },
    prelude::*,
};
use kdam::{Bar, BarBuilder, BarExt};
use raphtory_api::core::{
    entities::properties::prop::PropType,
    storage::{dict_mapper::MaybeNew, timeindex::EventTime, FxDashMap},
};
use raphtory_core::entities::{GidRef, VID};
use raphtory_storage::mutation::addition_ops::{InternalAdditionOps, SessionAdditionOps};
use rayon::prelude::*;
use std::{
    collections::HashMap,
    env,
    sync::atomic::{AtomicUsize, Ordering},
};

pub mod edge_props;
pub mod edges;
pub mod nodes;

#[cfg(feature = "progress")]
fn progress_bars_enabled() -> bool {
    env::var("RAPHTORY_PROGRESS_BARS_ENABLED")
        .ok()
        .and_then(|value| {
            let value = value.trim().to_ascii_lowercase();
            match value.as_str() {
                "false" | "0" => Some(false),
                "true" | "1" => Some(true),
                _ => None,
            }
        })
        .unwrap_or(true)
}

#[cfg(feature = "progress")]
fn build_progress_bar(des: String, num_rows: Option<usize>) -> Result<Bar, GraphError> {
    let mut bar_builder = BarBuilder::default()
        .desc(des)
        .animation(kdam::Animation::FillUp)
        .unit_scale(true);

    if let Some(num_rows) = num_rows {
        bar_builder = bar_builder.total(num_rows);
    }

    if !progress_bars_enabled() {
        bar_builder = bar_builder.disable(true);
    }

    bar_builder.build().map_err(|_| GraphError::TqdmError)
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

pub(crate) fn load_edge_deletions_from_df_prefetch<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + DeletionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + Send>,
    column_names: ColumnNames,
    resolve_nodes: bool,
    layer: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    edges::load_edges_from_df_prefetch(
        df_view,
        column_names,
        resolve_nodes,
        &[],
        &[],
        None,
        layer,
        graph,
        true,
    )
}

pub(crate) fn load_edge_deletions_from_df<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + DeletionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    column_names: ColumnNames,
    resolve_nodes: bool,
    layer: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    edges::load_edges_from_df(
        df_view,
        column_names,
        resolve_nodes,
        &[],
        &[],
        None,
        layer,
        graph,
        true,
    )
}

pub(crate) fn load_edges_props_from_df_prefetch<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + Send>,
    src: &str,
    dst: &str,
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
    graph: &G,
    resolve_nodes: bool,
) -> Result<(), GraphError> {
    edge_props::load_edges_from_df_prefetch(
        df_view,
        ColumnNames::new("", None, src, dst, layer_col),
        resolve_nodes,
        metadata,
        shared_metadata,
        layer,
        graph,
    )
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

    #[cfg(feature = "progress")]
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
                SecondaryIndexCol::new_from_range(start_id, start_id + df.len())
            }
        };

        time_col
            .par_iter()
            .zip(secondary_index_col.par_iter())
            .zip(prop_cols.par_rows())
            .zip(metadata_cols.par_rows())
            .try_for_each(|(((time, secondary_index), t_props), c_props)| {
                let t = EventTime(time, secondary_index);
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

        #[cfg(feature = "progress")]
        let _ = pb.update(df.len());
    }

    Ok(())
}

pub(crate) fn extract_secondary_index_col<G: InternalAdditionOps + AdditionOps>(
    secondary_index_index: Option<usize>,
    session: &<G as InternalAdditionOps>::WS<'_>,
    df: &DFChunk,
) -> Result<SecondaryIndexCol, GraphError> {
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
            SecondaryIndexCol::new_from_range(start_id, start_id + df.len())
        }
    };
    Ok(secondary_index_col)
}

fn resolve_nodes_with_cache<'a, G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    graph: &G,
    cols_to_resolve: &[&'a NodeCol],
    resolved_cols: &[&mut [AtomicUsize]],
) -> Result<FxDashMap<GidRef<'a>, VID>, GraphError> {
    resolve_nodes_with_cache_generic(
        cols_to_resolve,
        |vid: &VID, idx, col_idx| {
            resolved_cols[col_idx][idx].store(vid.0, Ordering::Relaxed);
        },
        |gid, _, _| {
            let vid = unsafe { graph.bulk_load_resolve_node(gid).map_err(into_graph_err)? };
            Ok(vid)
        },
    )
}

fn resolve_nodes_and_type_with_cache<
    'a,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    cols_to_resolve: &[&'a NodeCol],
    node_types: &[&'a [usize]],
    resolved_cols: &[&mut [AtomicUsize]],
) -> Result<FxDashMap<GidRef<'a>, (VID, usize)>, GraphError> {
    resolve_nodes_with_cache_generic(
        cols_to_resolve,
        |vid: &(VID, usize), row, col_idx| {
            let (vid, _) = vid;
            resolved_cols[col_idx][row].store(vid.index(), Ordering::Relaxed);
        },
        |gid, row, col_idx| {
            let vid = unsafe { graph.bulk_load_resolve_node(gid).map_err(into_graph_err)? };
            let node_type = node_types[col_idx][row];
            Ok((vid, node_type))
        },
    )
}

fn resolve_nodes_with_cache_generic<'a, V: Send + Sync>(
    cols_to_resolve: &[&'a NodeCol],
    update_fn: impl Fn(&V, usize, usize) + Send + Sync,
    new_fn: impl Fn(GidRef<'a>, usize, usize) -> Result<V, GraphError> + Send + Sync,
) -> Result<FxDashMap<GidRef<'a>, V>, GraphError> {
    let gid_str_cache: dashmap::DashMap<GidRef<'_>, V, _> = FxDashMap::default();
    let hasher_factory = gid_str_cache.hasher().clone();
    gid_str_cache
        .shards()
        .par_iter()
        .enumerate()
        .try_for_each(|(shard_idx, shard)| {
            let mut shard_guard = shard.write();
            use dashmap::SharedValue;
            use std::hash::BuildHasher;

            // Create hasher function for this shard
            let hash_key = |key: &GidRef<'_>| -> u64 { hasher_factory.hash_one(key) };

            let hasher_fn =
                |tuple: &(GidRef<'_>, SharedValue<V>)| -> u64 { hasher_factory.hash_one(tuple.0) };

            for (col_id, node_col) in cols_to_resolve.iter().enumerate() {
                // Process src_col sequentially for this shard
                for (row, gid) in node_col.iter().enumerate() {
                    // Check if this key belongs to this shard
                    if gid_str_cache.determine_map(&gid) != shard_idx {
                        continue; // Skip, not our shard
                    }

                    let hash = hash_key(&gid);

                    // Check if exists in this shard
                    if let Some((_, value)) = shard_guard.get(hash, |(g, _)| g == &gid) {
                        let v = value.get();
                        update_fn(v, row, col_id);
                    } else {
                        let v = new_fn(gid, row, col_id)?;

                        update_fn(&v, row, col_id);
                        let data = (gid, SharedValue::new(v));
                        shard_guard.insert(hash, data, hasher_fn);
                    }
                }
            }

            Ok::<(), GraphError>(())
        })?;
    Ok(gid_str_cache)
}
