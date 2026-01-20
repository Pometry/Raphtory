use crate::{
    db::api::view::StaticGraphViewOps,
    errors::{into_graph_err, GraphError, LoadError},
    io::arrow::{
        dataframe::{DFChunk, DFView},
        df_loaders::{
            build_progress_bar, extract_secondary_index_col, process_shared_properties,
            resolve_nodes_with_cache, GidKey,
        },
        layer_col::lift_layer_col,
        node_col::NodeCol,
        prop_handler::*,
    },
    prelude::*,
};
use arrow::{array::AsArray, datatypes::UInt64Type};
use bytemuck::checked::cast_slice_mut;
use db4_graph::WriteLockedGraph;
use itertools::izip;
use kdam::BarExt;
use raphtory_api::{
    atomic_extra::{atomic_usize_from_mut_slice, atomic_vid_from_mut_slice},
    core::{
        entities::{properties::meta::STATIC_GRAPH_LAYER_ID, EID},
        storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry, FxDashMap},
    },
};
use raphtory_core::entities::VID;
use raphtory_storage::mutation::addition_ops::SessionAdditionOps;
use rayon::prelude::*;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
};
use storage::{
    api::{edges::EdgeSegmentOps, nodes::NodeSegmentOps},
    pages::locked::{
        edges::{LockedEdgePage, WriteLockedEdgePages},
        nodes::LockedNodePage,
    },
    Extension,
};

#[derive(Debug, Copy, Clone)]
pub struct ColumnNames<'a> {
    pub time: &'a str,
    pub secondary_index: Option<&'a str>,
    pub src: &'a str,
    pub dst: &'a str,
    pub edge_id: Option<&'a str>,
    pub layer_col: Option<&'a str>,
    pub layer_id_col: Option<&'a str>,
}

impl<'a> ColumnNames<'a> {
    pub fn new(
        time: &'a str,
        secondary_index: Option<&'a str>,

        src: &'a str,
        dst: &'a str,

        layer_col: Option<&'a str>,
    ) -> Self {
        Self {
            time,
            secondary_index,
            src,
            dst,
            layer_col,
            edge_id: None,
            layer_id_col: None,
        }
    }

    pub fn with_layer_id_col(mut self, layer_id_col: &'a str) -> Self {
        self.layer_id_col = Some(layer_id_col);
        self
    }

    pub fn with_edge_id_col(mut self, edge_id: &'a str) -> Self {
        self.edge_id = Some(edge_id);
        self
    }
}

#[allow(clippy::too_many_arguments)]
pub fn load_edges_from_df_pandas<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
    I2: Iterator<Item = Result<DFChunk, GraphError>>,
>(
    df_view: DFView<I2>,
    column_names: ColumnNames,
    resolve_nodes: bool, // this is reserved for internal parquet encoders, this cannot be exposed to users
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    graph: &G,
    delete: bool, // whether to update edge deletions or additions
) -> Result<(), GraphError> {
    let DFView {
        names,
        chunks,
        num_rows,
    } = df_view;
    let df_view_meta = DFView {
        names,
        chunks: std::iter::empty(),
        num_rows,
    };
    load_edges_from_df_inner(
        chunks,
        df_view_meta,
        column_names,
        resolve_nodes,
        properties,
        metadata,
        shared_metadata,
        layer,
        graph,
        delete,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn load_edges_from_df<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
    I1: Iterator<Item = Result<DFChunk, GraphError>> + Send,
>(
    df_view: DFView<I1>,
    column_names: ColumnNames,
    resolve_nodes: bool, // this is reserved for internal parquet encoders, this cannot be exposed to users
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    graph: &G,
    delete: bool, // whether to update edge deletions or additions
) -> Result<(), GraphError> {
    let DFView {
        names,
        chunks,
        num_rows,
    } = df_view;
    let df_view_meta = DFView {
        names,
        chunks: std::iter::empty(),
        num_rows,
    };
    rayon::scope(|s| {
        let (tx, rx) = mpsc::sync_channel(2);

        s.spawn(move |_| {
            let sender = tx;
            for chunk in chunks {
                if let Err(e) = sender.send(chunk) {
                    eprintln!("Error sending chunk to loader: {}", e);
                    break;
                }
            }
        });

        load_edges_from_df_inner(
            rx,
            df_view_meta,
            column_names,
            resolve_nodes,
            properties,
            metadata,
            shared_metadata,
            layer,
            graph,
            delete,
        )?;
        Ok::<(), GraphError>(())
    })?;

    Ok(())
}

fn load_edges_from_df_inner<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    chunks: impl IntoIterator<Item = Result<DFChunk, GraphError>>,
    df_view: DFView<impl Iterator<Item = ()>>, // for metadata only

    column_names: ColumnNames,
    resolve_nodes: bool, // this is reserved for internal parquet encoders, this cannot be exposed to users
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    graph: &G,
    delete: bool, // whether to update edge deletions or additions
) -> Result<(), GraphError> {
    if df_view.is_empty() {
        return Ok(());
    }

    let ColumnNames {
        time,
        secondary_index,
        src,
        dst,
        edge_id,
        layer_col,
        layer_id_col,
    } = column_names;

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
    let edge_index = edge_id.and_then(|name| df_view.get_index_opt(name));
    let layer_id_index = layer_id_col.and_then(|name| df_view.get_index_opt(name));
    let secondary_index_index = secondary_index
        .map(|col| df_view.get_index(col))
        .transpose()?;
    let layer_index = layer_col.map(|name| df_view.get_index(name)).transpose()?;

    let session = graph.write_session().map_err(into_graph_err)?;
    let shared_metadata = process_shared_properties(shared_metadata, |key, dtype| {
        session
            .resolve_edge_property(key, dtype, true)
            .map_err(into_graph_err)
    })?;

    assert!(
        (resolve_nodes ^ edge_index.is_some()),
        "resolve_nodes must be false when edge_id is provided or true when edge_id is None, {{resolve_nodes:{resolve_nodes:?}, edge_id:{edge_index:?}}}"
    );

    assert!(
        (resolve_nodes ^ layer_id_index.is_some()),
        "resolve_nodes must be false when layer_id is provided or true when layer_id is None, {{resolve_nodes:{resolve_nodes:?}, layer_id:{layer_id_index:?}}}"
    );

    #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading edges".to_string(), df_view.num_rows)?;

    let mut src_col_resolved: Vec<VID> = vec![];
    let mut dst_col_resolved: Vec<VID> = vec![];
    let mut eid_col_resolved: Vec<EID> = vec![];
    let mut eids_exist: Vec<AtomicBool> = vec![]; // exists or needs to be created
    let mut layer_eids_exist: Vec<AtomicBool> = vec![]; // exists or needs to be created

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
        // validate src and dst columns
        let src_col = df.node_col(src_index)?;
        let dst_col = df.node_col(dst_index)?;
        if resolve_nodes {
            src_col.validate(graph, LoadError::MissingSrcError)?;
            dst_col.validate(graph, LoadError::MissingDstError)?;
        }
        let layer = lift_layer_col(layer, layer_index, &df)?;
        let layer_id_values = layer_id_index
            .map(|idx| {
                df.chunk[idx]
                    .as_primitive_opt::<UInt64Type>()
                    .ok_or_else(|| LoadError::InvalidLayerType(df.chunk[idx].data_type().clone()))
                    .map(|array| array.values().as_ref())
            })
            .transpose()?;
        let layer_col_resolved = layer.resolve_layer(layer_id_values, graph)?;

        let (src_vids, dst_vids, gid_str_cache) = get_or_resolve_node_vids(
            graph,
            src_index,
            dst_index,
            &mut src_col_resolved,
            &mut dst_col_resolved,
            resolve_nodes,
            &df,
            &src_col,
            &dst_col,
        )?;

        let time_col = df.time_col(time_index)?;

        // Load the secondary index column if it exists, otherwise generate from start_id.
        let secondary_index_col =
            extract_secondary_index_col::<G>(secondary_index_index, &session, &df)?;

        let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;

        eid_col_resolved.resize_with(df.len(), Default::default);
        eids_exist.resize_with(df.len(), Default::default);
        layer_eids_exist.resize_with(df.len(), Default::default);
        let eid_col_shared = atomic_usize_from_mut_slice(cast_slice_mut(&mut eid_col_resolved));

        let arc_edges = write_locked_graph.graph().storage().edges().clone();
        let next_edge_id = |row: usize| {
            let (page, pos) = arc_edges.reserve_free_pos(row);
            pos.as_eid(page, arc_edges.max_page_len())
        };

        let WriteLockedGraph {
            nodes, ref edges, ..
        } = &mut write_locked_graph;

        let eids = edge_index.and_then(|edge_id_col| {
            Some(
                df.chunk[edge_id_col]
                    .as_primitive_opt::<UInt64Type>()?
                    .values()
                    .as_ref(),
            )
        });

        // Generate all edge_ids + add outbound edges
        nodes.par_iter_mut().for_each(|locked_page| {
            // Zip all columns for iteration.
            let zip = izip!(
                src_vids.iter(),
                dst_vids.iter(),
                time_col.iter(),
                secondary_index_col.iter(),
                layer_col_resolved.iter()
            );

            // resolve_nodes=false
            // assumes we are loading our own graph, via the parquet loaders,
            // so previous calls have already stored the node ids and types
            if resolve_nodes {
                store_node_ids(&gid_str_cache, locked_page);
            }

            if resolve_nodes {
                add_and_resolve_outbound_edges(
                    &eids_exist,
                    &layer_eids_exist,
                    &eid_col_shared,
                    next_edge_id,
                    edges,
                    locked_page,
                    zip,
                    delete,
                );
            } else if let Some(edge_ids) = eids {
                add_and_resolve_outbound_edges(
                    &eids_exist,
                    &layer_eids_exist,
                    &eid_col_shared,
                    |row| {
                        let eid = EID(edge_ids[row] as usize);
                        arc_edges.increment_edge_segment_count(eid);
                        eid
                    },
                    edges,
                    locked_page,
                    zip,
                    delete,
                );
            }
        });

        write_locked_graph.nodes.par_iter_mut().for_each(|shard| {
            let zip = izip!(
                src_vids.iter(),
                dst_vids.iter(),
                eid_col_resolved.iter(),
                time_col.iter(),
                secondary_index_col.iter(),
                layer_col_resolved.iter(),
                layer_eids_exist.iter().map(|a| a.load(Ordering::Relaxed)),
                eids_exist.iter().map(|b| b.load(Ordering::Relaxed))
            );

            update_inbound_edges(shard, zip, delete);
        });

        drop(write_locked_graph);

        let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;

        write_locked_graph.edges.par_iter_mut().for_each(|shard| {
            let zip = izip!(
                src_vids.iter(),
                dst_vids.iter(),
                time_col.iter(),
                secondary_index_col.iter(),
                eid_col_resolved.iter(),
                layer_col_resolved.iter(),
                eids_exist
                    .iter()
                    .map(|exists| exists.load(Ordering::Relaxed))
            );
            update_edge_properties(
                &shared_metadata,
                &prop_cols,
                &metadata_cols,
                shard,
                zip,
                delete,
            );
        });

        #[cfg(feature = "python")]
        let _ = pb.update(df.len());
    }
    Ok::<_, GraphError>(())
}

#[inline(never)]
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn get_or_resolve_node_vids<
    'a: 'c,
    'b: 'c,
    'c,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    src_index: usize,
    dst_index: usize,
    src_col_resolved: &'a mut Vec<VID>,
    dst_col_resolved: &'a mut Vec<VID>,
    resolve_nodes: bool,
    df: &'b DFChunk,
    src_col: &'a NodeCol,
    dst_col: &'a NodeCol,
) -> Result<
    (
        &'c [VID],
        &'c [VID],
        FxDashMap<GidKey<'a>, (GID, MaybeNew<VID>)>,
    ),
    GraphError,
> {
    let (src_vids, dst_vids, gid_str_cache) = if resolve_nodes {
        src_col_resolved.resize_with(df.len(), Default::default);
        dst_col_resolved.resize_with(df.len(), Default::default);

        let atomic_src_col = atomic_vid_from_mut_slice(src_col_resolved);
        let atomic_dst_col = atomic_vid_from_mut_slice(dst_col_resolved);

        let gid_str_cache = resolve_nodes_with_cache::<G>(
            graph,
            [(src_col), (dst_col)].as_ref(),
            [atomic_src_col, atomic_dst_col].as_ref(),
        )?;
        (
            src_col_resolved.as_slice(),
            dst_col_resolved.as_slice(),
            gid_str_cache,
        )
    } else {
        let srcs = df.chunk[src_index]
            .as_primitive_opt::<UInt64Type>()
            .ok_or_else(|| LoadError::InvalidNodeIdType(df.chunk[src_index].data_type().clone()))?
            .values()
            .as_ref();
        let dsts = df.chunk[dst_index]
            .as_primitive_opt::<UInt64Type>()
            .ok_or_else(|| LoadError::InvalidNodeIdType(df.chunk[dst_index].data_type().clone()))?
            .values()
            .as_ref();
        (
            bytemuck::cast_slice(srcs),
            bytemuck::cast_slice(dsts),
            FxDashMap::default(),
        )
    };
    Ok((src_vids, dst_vids, gid_str_cache))
}

#[inline(never)]
fn update_edge_properties<'a, ES: EdgeSegmentOps<Extension = Extension>>(
    shared_metadata: &[(usize, Prop)],
    prop_cols: &PropCols,
    metadata_cols: &PropCols,
    shard: &mut LockedEdgePage<'_, ES>,
    zip: impl Iterator<Item = (&'a VID, &'a VID, i64, usize, &'a EID, &'a usize, bool)>,
    delete: bool,
) {
    let mut t_props: Vec<(usize, Prop)> = vec![];
    let mut c_props: Vec<(usize, Prop)> = vec![];

    for (row, (src, dst, time, secondary_index, eid, layer, exists)) in zip.enumerate() {
        if let Some(eid_pos) = shard.resolve_pos(*eid) {
            let t = TimeIndexEntry(time, secondary_index);
            let mut writer = shard.writer();

            t_props.clear();
            t_props.extend(prop_cols.iter_row(row));

            c_props.clear();
            c_props.extend(metadata_cols.iter_row(row));
            c_props.extend_from_slice(shared_metadata);

            if !delete {
                writer.bulk_add_edge(
                    t,
                    eid_pos,
                    *src,
                    *dst,
                    exists,
                    *layer,
                    c_props.drain(..),
                    t_props.drain(..),
                );
            } else {
                writer.bulk_delete_edge(t, eid_pos, *src, *dst, exists, *layer);
            }
        }
    }
}

#[inline(never)]
fn update_inbound_edges<'a, NS: NodeSegmentOps<Extension = Extension>>(
    shard: &mut LockedNodePage<'_, NS>,
    zip: impl Iterator<Item = (&'a VID, &'a VID, &'a EID, i64, usize, &'a usize, bool, bool)>,
    delete: bool,
) {
    for (
        src,
        dst,
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

            if !edge_exists_in_static_graph {
                writer.add_static_inbound_edge(dst_pos, *src, *eid);
            }
            let elid = if delete {
                eid.with_layer_deletion(*layer)
            } else {
                eid.with_layer(*layer)
            };

            if src != dst {
                if edge_exists_in_layer {
                    writer.update_timestamp(t, dst_pos, elid);
                } else {
                    writer.add_inbound_edge(Some(t), dst_pos, *src, elid);
                }
            } else {
                // self-loop edge, only add once
                if !edge_exists_in_layer {
                    writer.add_inbound_edge::<i64>(None, dst_pos, *src, elid);
                }
            }
        }
    }
}

#[inline(never)]
fn add_and_resolve_outbound_edges<
    'a,
    NS: NodeSegmentOps<Extension = Extension>,
    ES: EdgeSegmentOps<Extension = Extension>,
>(
    eids_exist: &[AtomicBool],
    layer_eids_exist: &[AtomicBool],
    eid_col_shared: &&mut [AtomicUsize],
    next_edge_id: impl Fn(usize) -> EID,
    edges: &WriteLockedEdgePages<'_, ES>,
    locked_page: &mut LockedNodePage<'_, NS>,
    zip: impl Iterator<Item = (&'a VID, &'a VID, i64, usize, &'a usize)>,
    delete: bool,
) {
    for (row, (src, dst, time, secondary_index, layer)) in zip.enumerate() {
        if let Some(src_pos) = locked_page.resolve_pos(*src) {
            let mut writer = locked_page.writer();
            let t = TimeIndexEntry(time, secondary_index);
            // find the original EID in the static graph if it exists
            // otherwise create a new one

            let edge_id = if let Some(edge_id) = writer.get_out_edge(src_pos, *dst, 0) {
                eid_col_shared[row].store(edge_id.0, Ordering::Relaxed);
                eids_exist[row].store(true, Ordering::Relaxed);
                MaybeNew::Existing(edge_id)
            } else {
                let edge_id = next_edge_id(row);
                writer.add_static_outbound_edge(src_pos, *dst, edge_id);
                eid_col_shared[row].store(edge_id.0, Ordering::Relaxed);
                eids_exist[row].store(false, Ordering::Relaxed);
                MaybeNew::New(edge_id)
            };

            let edge_id = edge_id.map(|eid| {
                if delete {
                    eid.with_layer_deletion(*layer)
                } else {
                    eid.with_layer(*layer)
                }
            });

            let exists = !edge_id.is_new()
                && (edges.exists(edge_id.inner())
                    || writer
                        .get_out_edge(src_pos, *dst, edge_id.inner().layer())
                        .is_some());

            layer_eids_exist[row].store(exists, Ordering::Relaxed);

            if exists {
                writer.update_timestamp(t, src_pos, edge_id.inner());
            } else {
                writer.add_outbound_edge(Some(t), src_pos, *dst, edge_id.inner());
            }
        }
    }
}

pub fn store_node_ids<K: Eq + std::hash::Hash, NS: NodeSegmentOps<Extension = Extension>>(
    gid_str_cache: &FxDashMap<K, (GID, MaybeNew<VID>)>,
    locked_page: &mut LockedNodePage<'_, NS>,
) {
    for entry in gid_str_cache.iter() {
        let (src_gid, vid) = entry.value();

        if let Some(src_pos) = locked_page.resolve_pos(vid.inner()) {
            let mut writer = locked_page.writer();
            writer.store_node_id(src_pos, STATIC_GRAPH_LAYER_ID, src_gid.clone());
        }
    }
}
