use crate::{
    db::api::view::StaticGraphViewOps,
    errors::{into_graph_err, GraphError, LoadError},
    io::arrow::{
        dataframe::{DFChunk, DFView},
        df_loaders::{
            build_progress_bar,
            edges::{get_or_resolve_node_vids, store_node_ids, ColumnNames},
            process_shared_properties,
        },
        layer_col::lift_layer_col,
        prop_handler::*,
    },
    prelude::*,
};
use arrow::{array::AsArray, datatypes::UInt64Type};
use bytemuck::checked::cast_slice_mut;
use db4_graph::WriteLockedGraph;
use itertools::izip;
use kdam::BarExt;
use raphtory_api::{atomic_extra::atomic_usize_from_mut_slice, core::entities::EID};
use raphtory_core::entities::VID;
use raphtory_storage::mutation::addition_ops::SessionAdditionOps;
use rayon::prelude::*;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
};
use storage::{
    api::{edges::EdgeSegmentOps, nodes::NodeSegmentOps},
    pages::locked::{edges::LockedEdgePage, nodes::LockedNodePage},
    Extension,
};

#[allow(clippy::too_many_arguments)]
pub fn load_edges_from_df<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + Send>,
    column_names: ColumnNames,
    resolve_nodes: bool,
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    if df_view.is_empty() {
        return Ok(());
    }

    let ColumnNames {
        src,
        dst,
        layer_col,
        layer_id_col,
        ..
    } = column_names;

    let metadata_indices = metadata
        .iter()
        .map(|name| df_view.get_index(name))
        .collect::<Result<Vec<_>, GraphError>>()?;

    let src_index = df_view.get_index(src)?;
    let dst_index = df_view.get_index(dst)?;
    let layer_id_index = layer_id_col.and_then(|name| df_view.get_index_opt(name));
    let layer_index = layer_col.map(|name| df_view.get_index(name)).transpose()?;

    let session = graph.write_session().map_err(into_graph_err)?;
    let shared_metadata = process_shared_properties(shared_metadata, |key, dtype| {
        session
            .resolve_edge_property(key, dtype, true)
            .map_err(into_graph_err)
    })?;

    // #[cfg(feature = "python")]
    let mut pb = build_progress_bar("Loading edges metadata".to_string(), df_view.num_rows)?;

    let mut src_col_resolved: Vec<VID> = vec![];
    let mut dst_col_resolved: Vec<VID> = vec![];
    let mut eid_col_resolved: Vec<EID> = vec![];

    rayon::scope(|s| {
        let (tx, rx) = mpsc::sync_channel(2);

        s.spawn(move |_| {
            let sender = tx;
            for chunk in df_view.chunks {
                if let Err(e) = sender.send(chunk) {
                    eprintln!("Error pre-fetching chunk for loading edges, possibly receiver has been dropped {e}");
                    break;
                }
            }
        });

        for chunk in rx.iter() {
            let df = chunk?;
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
                        .ok_or_else(|| {
                            LoadError::InvalidLayerType(df.chunk[idx].data_type().clone())
                        })
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

            let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;

            eid_col_resolved.resize_with(df.len(), Default::default);
            let eid_col_shared = atomic_usize_from_mut_slice(cast_slice_mut(&mut eid_col_resolved));

            let WriteLockedGraph { nodes, .. } = &mut write_locked_graph;

            // Generate all edge_ids + add outbound edges
            nodes.par_iter_mut().try_for_each(|locked_page| {
                // Zip all columns for iteration.
                let zip = izip!(src_vids.iter(), dst_vids.iter());
                add_and_resolve_outbound_edges(&eid_col_shared, locked_page, zip)?;
                // resolve_nodes=false
                // assumes we are loading our own graph, via the parquet loaders,
                // so previous calls have already stored the node ids and types
                if resolve_nodes {
                    store_node_ids(&gid_str_cache, locked_page);
                }
                Ok::<_, GraphError>(())
            })?;

            drop(write_locked_graph);

            let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;

            write_locked_graph.edges.par_iter_mut().for_each(|shard| {
                let zip = izip!(
                    src_vids.iter(),
                    dst_vids.iter(),
                    eid_col_resolved.iter(),
                    layer_col_resolved.iter(),
                );
                update_edge_metadata(&shared_metadata, &metadata_cols, shard, zip);
            });

            // #[cfg(feature = "python")]
            let _ = pb.update(df.len());
        }
        Ok::<_, GraphError>(())
    })?;
    // set the type of the resolver;

    Ok(())
}

#[inline(never)]
fn add_and_resolve_outbound_edges<'a, NS: NodeSegmentOps<Extension = Extension>>(
    eid_col_shared: &&mut [AtomicUsize],
    locked_page: &mut LockedNodePage<'_, NS>,
    zip: impl Iterator<Item = (&'a VID, &'a VID)>,
) -> Result<(), LoadError> {
    for (row, (src, dst)) in zip.enumerate() {
        if let Some(src_pos) = locked_page.resolve_pos(*src) {
            let writer = locked_page.writer();
            // find the original EID in the static graph if it exists
            // otherwise create a new one
            if let Some(edge_id) = writer.get_out_edge(src_pos, *dst, 0) {
                eid_col_shared[row].store(edge_id.0, Ordering::Relaxed);
            } else {
                return Err(LoadError::MissingEdgeError(*src, *dst));
            };
        }
    }
    Ok(())
}

#[inline(never)]
fn update_edge_metadata<'a, ES: EdgeSegmentOps<Extension = Extension>>(
    shared_metadata: &[(usize, Prop)],
    metadata_cols: &PropCols,
    shard: &mut LockedEdgePage<'_, ES>,
    zip: impl Iterator<Item = (&'a VID, &'a VID, &'a EID, &'a usize)>,
) {
    let mut c_props: Vec<(usize, Prop)> = Vec::new();
    for (row, (src, dst, eid, layer)) in zip.enumerate() {
        if let Some(eid_pos) = shard.resolve_pos(*eid) {
            let mut writer = shard.writer();

            c_props.clear();
            c_props.extend(metadata_cols.iter_row(row));
            c_props.extend_from_slice(shared_metadata);

            writer.update_c_props(eid_pos, *src, *dst, *layer, c_props.drain(..));
        }
    }
}
