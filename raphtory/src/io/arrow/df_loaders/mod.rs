use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::api::view::StaticGraphViewOps,
    errors::{into_graph_err, GraphError},
    io::arrow::{
        dataframe::{DFChunk, DFView, SecondaryIndexCol},
        df_loaders::edges::ColumnNames,
        layer_col::LayerCol,
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
    sync::atomic::{AtomicUsize, Ordering},
};

pub mod edge_props;
pub mod edges;
pub mod nodes;
#[cfg(feature = "python")]
fn build_progress_bar(des: String, num_rows: Option<usize>) -> Result<Bar, GraphError> {
    if let Some(num_rows) = num_rows {
        BarBuilder::default()
            .desc(des)
            .animation(kdam::Animation::FillUp)
            .total(num_rows)
            .unit_scale(true)
            .build()
            .map_err(|_| GraphError::TqdmError)
    } else {
        BarBuilder::default()
            .desc(des)
            .animation(kdam::Animation::FillUp)
            .unit_scale(true)
            .build()
            .map_err(|_| GraphError::TqdmError)
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

pub(crate) fn load_edge_deletions_from_df<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + DeletionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + Send>,
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

pub(crate) fn load_edge_deletions_from_df_pandas<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + DeletionOps,
>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>,
    column_names: ColumnNames,
    resolve_nodes: bool,
    layer: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    edges::load_edges_from_df_pandas(
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

pub(crate) fn load_edges_props_from_df<
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
    edge_props::load_edges_from_df(
        df_view,
        ColumnNames::new("", None, src, dst, layer_col),
        resolve_nodes,
        metadata,
        shared_metadata,
        layer,
        graph,
    )
}

pub(crate) fn load_edges_props_from_df_pandas<
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
    resolve_nodes: bool,
) -> Result<(), GraphError> {
    edge_props::load_edges_from_df_pandas(
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

        #[cfg(feature = "python")]
        let _ = pb.update(df.len());
    }

    Ok(())
}

#[inline(never)]
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

#[inline(never)]
fn resolve_nodes_with_cache<'a, G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    graph: &G,
    cols_to_resolve: &[&'a NodeCol],
    resolved_cols: &[&mut [AtomicUsize]],
) -> Result<FxDashMap<GidKey<'a>, (GID, MaybeNew<VID>)>, GraphError> {
    let node_type_col = vec![None; cols_to_resolve.len()];
    resolve_nodes_with_cache_generic(
        cols_to_resolve,
        &node_type_col,
        |v: &(GID, MaybeNew<VID>), idx, col_idx| {
            let (_, vid) = v;
            resolved_cols[col_idx][idx].store(vid.inner().0, Ordering::Relaxed);
        },
        |gid, _idx| {
            let GidKey { gid, .. } = gid;
            let vid = graph
                .resolve_node(gid.as_node_ref())
                .map_err(into_graph_err)?;
            Ok((GID::from(gid), vid))
        },
    )
}

#[inline(never)]
fn resolve_nodes_and_type_with_cache<
    'a,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    cols_to_resolve: &[&'a NodeCol],
    resolved_cols: &[&mut [AtomicUsize]],
    node_type_col: LayerCol<'a>,
) -> Result<FxDashMap<GidKey<'a>, (VID, usize)>, GraphError> {
    let node_type_cols = vec![Some(node_type_col); cols_to_resolve.len()];
    resolve_nodes_with_cache_generic(
        cols_to_resolve,
        &node_type_cols,
        |v: &(VID, usize), row, col_idx| {
            let (vid, _) = v;
            resolved_cols[col_idx][row].store(vid.index(), Ordering::Relaxed);
        },
        |gid, _| {
            let GidKey { gid, node_type } = gid;
            let (vid, node_type) = graph
                .resolve_node_and_type(gid.as_node_ref(), node_type)
                .map_err(into_graph_err)?;
            Ok((vid, node_type))
        },
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)]
pub struct GidKey<'a> {
    gid: GidRef<'a>,
    node_type: Option<&'a str>,
}

impl<'a> GidKey<'a> {
    pub fn new(gid: GidRef<'a>, node_type: Option<&'a str>) -> Self {
        Self { gid, node_type }
    }
}

#[inline(always)]
fn resolve_nodes_with_cache_generic<'a, V: Send + Sync>(
    cols_to_resolve: &[&'a NodeCol],
    node_type_cols: &[Option<LayerCol<'a>>],
    update_fn: impl Fn(&V, usize, usize) + Send + Sync,
    new_fn: impl Fn(GidKey<'a>, usize) -> Result<V, GraphError> + Send + Sync,
) -> Result<FxDashMap<GidKey<'a>, V>, GraphError> {
    assert_eq!(cols_to_resolve.len(), node_type_cols.len());
    let gid_str_cache: dashmap::DashMap<GidKey<'_>, V, _> = FxDashMap::default();
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
            let hash_key = |key: &GidKey<'_>| -> u64 { hasher_factory.hash_one(key) };

            let hasher_fn =
                |tuple: &(GidKey<'_>, SharedValue<V>)| -> u64 { hasher_factory.hash_one(tuple.0) };

            for (col_id, (node_col, layer_col)) in
                cols_to_resolve.iter().zip(node_type_cols).enumerate()
            {
                // Process src_col sequentially for this shard
                for (idx, gid) in node_col.iter().enumerate() {
                    let node_type = layer_col.as_ref().and_then(|lc| lc.get(idx));
                    let gid = GidKey::new(gid, node_type);
                    // Check if this key belongs to this shard
                    if gid_str_cache.determine_map(&gid) != shard_idx {
                        continue; // Skip, not our shard
                    }

                    let hash = hash_key(&gid);

                    // Check if exists in this shard
                    if let Some((_, value)) = shard_guard.get(hash, |(g, _)| g == &gid) {
                        let v = value.get();
                        update_fn(&v, idx, col_id);
                    } else {
                        let v = new_fn(gid, idx)?;

                        update_fn(&v, idx, col_id);
                        let data = (gid, SharedValue::new(v));
                        shard_guard.insert(hash, data, hasher_fn);
                    }
                }
            }

            Ok::<(), GraphError>(())
        })?;
    Ok(gid_str_cache)
}
