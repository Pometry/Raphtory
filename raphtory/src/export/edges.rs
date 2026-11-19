use super::{exported_columns_idx, select_columns, ChunkCollector, ExportTable};
use crate::{
    core::entities::edges::edge_ref::EdgeRef,
    db::{
        api::view::internal::GraphView,
        graph::{
            edge::EdgeView,
            edges::{EdgeItem, Edges},
        },
    },
    errors::GraphError,
    parquet_encoder::{
        edge_tprop_fixed_fields, edge_tprop_schema, encode_edge_rows, DST_VID_COL, EDGE_COL_ID,
        LAYER_ID_COL, SRC_GID_COL, SRC_VID_COL,
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;
use rayon::prelude::*;

/// The internal ids among the edge encoder's fixed columns, which the exported tables leave out.
const EDGE_INTERNAL_COLS: [&str; 4] = [SRC_VID_COL, DST_VID_COL, EDGE_COL_ID, LAYER_ID_COL];

/// Builds the edge update table of `edges`: one row per update of each edge. With `latest_only`,
/// only the updates that hold the latest value of a property are kept, each with only those
/// properties.
///
/// The columns are the edge encoder's fixed columns, without the internal node, edge and layer
/// ids, followed by one column per temporal property visible in the view. The time column holds
/// epoch milliseconds, or UTC datetimes when `convert_datetime` is set.
pub fn edge_updates<'graph, G, K>(
    edges: &Edges<'graph, G, K>,
    latest_only: bool,
    convert_datetime: bool,
) -> Result<ExportTable, GraphError>
where
    G: GraphViewOps<'graph>,
    K: EdgeItem,
{
    let graph = &edges.base_graph;

    let encoded = edge_tprop_schema(graph)?;
    let num_fixed =
        edge_tprop_fixed_fields(encoded.field_with_name(SRC_GID_COL)?.data_type()).len();
    let visible = graph
        .edge_visible_temporal_prop_ids()
        .filter_map(|id| graph.edge_visible_temporal_prop_name(id))
        .collect();
    let columns = exported_columns_idx(&encoded, num_fixed, &EDGE_INTERNAL_COLS, &visible);

    // the collection can only be iterated, so its edges are listed to split them into chunks
    let edge_list = edges.iter().map(|edge| edge.edge).collect_vec();
    let collector = ChunkCollector::default();
    encode_edge_rows(
        graph,
        edge_chunks(graph, &edge_list),
        latest_only,
        |_, chunk, _| Ok(collector.sink(chunk)),
    )?;
    select_columns(
        &encoded,
        collector.into_batches(),
        &columns,
        convert_datetime,
    )
}

/// Splits the edges of a collection on `graph` into chunks for parallel encoding.
/// Chunks are numbered in the order of the collection, so rows come out in that order.
fn edge_chunks<'a, G: GraphView>(
    graph: &'a G,
    edges: &'a [EdgeRef],
) -> impl ParallelIterator<Item = (usize, impl Iterator<Item = EdgeView<&'a G>> + Send + 'a)> + 'a {
    let chunk_size = (edges.len() / rayon::current_num_threads().max(1)).max(1);
    edges
        .par_chunks(chunk_size)
        .enumerate()
        .map(move |(chunk, edges)| {
            (
                chunk,
                edges
                    .iter()
                    .map(move |edge| EdgeView::new_filtered(graph, *edge)),
            )
        })
}
