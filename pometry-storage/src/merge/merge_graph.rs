use crate::{
    chunked_array::{
        chunked_array::{ChunkedArray, NonNull},
        list_array::ChunkedListArray,
        mutable_chunked_array::{
            ChunkedOffsetsBuilder, MutChunkedStructArray, MutPrimitiveChunkedArray,
        },
    },
    disk_hmap::DiskHashMap,
    edge_list::EdgeList,
    edges::EdgeTemporalProps,
    file_prefix::GraphPaths,
    global_order::GIDArray,
    graph::{earliest_latest_times, fix_t_prop_offsets, TemporalGraph},
    graph_fragment::{Metadata, TempColGraphFragment},
    load::{persist_gid_map, persist_sorted_gids},
    merge::{
        merge_adj::merge_adj,
        merge_gids::{build_local_id_map, merge_gids},
        merge_inbound_adj::merge_inbounds,
        merge_node_additions::merge_additions,
        merge_node_types::{merge_node_type_ids, merge_node_type_names, reindex_node_type_ids},
        merge_props::merge_node_props::merge_properties,
        reindex_node_offsets,
    },
    nodes::{Adj, AdjTotal, Nodes},
    prelude::{ArrayOps, Chunked},
    prepare_graph_dir, prepare_layer_dir, RAError,
};
use ahash::HashMap;
use itertools::EitherOrBoth;
use polars_arrow::array::{Array, PrimitiveArray};
use raphtory_api::core::entities::{EID, VID};
use rayon::prelude::*;
use std::{collections::hash_map::Entry, fmt::Debug, path::Path};
use tracing::instrument;

use super::merge_edge_list::merge_edge_list;

// #[instrument(level = "debug", skip(left, right))]
pub fn merge_graphs(
    graph_dir: impl AsRef<Path> + Debug,
    left: &TemporalGraph,
    right: &TemporalGraph,
) -> Result<TemporalGraph, RAError> {
    let graph_dir = graph_dir.as_ref();
    prepare_graph_dir(graph_dir)?;
    let left_gids = left.global_ordering().clone().try_into()?;
    let right_gids = right.global_ordering().clone().try_into()?;
    let gids = merge_gids(&left_gids, &right_gids)?;
    persist_sorted_gids(graph_dir, gids.to_boxed())?;
    let gid_map = DiskHashMap::from_sorted_dedup(gids.to_boxed())?;
    persist_gid_map(graph_dir, &gid_map)?;
    let num_nodes = gids.len();
    let left_local_map = build_local_id_map(&gid_map, &left_gids);
    let right_local_map = build_local_id_map(&gid_map, &right_gids);

    let (global_nodes, edge_list) = merge_edge_list(
        graph_dir,
        num_nodes,
        &left_local_map,
        &left.edge_list,
        &right_local_map,
        &right.edge_list,
    )?;

    let (layer_names, layer_index) = merge_layer_names(left.layer_names(), right.layer_names());
    let layers = layer_names
        .iter()
        .zip(layer_index)
        .map(|(name, index)| {
            let layer_dir = prepare_layer_dir(graph_dir, name)?;
            match index {
                EitherOrBoth::Both(left_layer_id, right_layer_id) => merge_layer(
                    &layer_dir,
                    &global_nodes,
                    edge_list.len(),
                    &left,
                    left_layer_id,
                    &left_local_map,
                    &right,
                    right_layer_id,
                    &right_local_map,
                    &gids,
                ),
                EitherOrBoth::Left(layer_id) => reindex_and_copy_layer(
                    &layer_dir,
                    edge_list.len(),
                    &left.layer(layer_id),
                    &left.edge_list,
                    &global_nodes,
                    &left_local_map,
                    &gids,
                ),
                EitherOrBoth::Right(layer_id) => reindex_and_copy_layer(
                    &layer_dir,
                    edge_list.len(),
                    &right.layer(layer_id),
                    &right.edge_list,
                    &global_nodes,
                    &right_local_map,
                    &gids,
                ),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let node_props = merge_properties(
        graph_dir,
        left.node_properties(),
        &left_local_map,
        right.node_properties(),
        &right_local_map,
        num_nodes,
    )?;

    let (node_types, node_type_ids) = match (left.node_types.as_ref(), right.node_types.as_ref()) {
        (Some(left_types), Some(right_types)) => {
            let (node_types, right_type_map) = merge_node_type_names(left_types, right_types);
            let node_type_ids = merge_node_type_ids(
                graph_dir,
                left.node_type_ids().unwrap(),
                &left_local_map,
                right.node_type_ids().unwrap(),
                &right_local_map,
                &right_type_map,
            )?;
            (Some(node_types), Some(node_type_ids))
        }
        (Some(left_types), None) => {
            let node_type_ids = reindex_node_type_ids(
                graph_dir,
                left.node_type_ids().unwrap(),
                &left_local_map,
                num_nodes,
            )?;
            (Some(left_types.clone()), Some(node_type_ids))
        }
        (None, Some(right_types)) => {
            let node_type_ids = reindex_node_type_ids(
                graph_dir,
                right.node_type_ids().unwrap(),
                &right_local_map,
                num_nodes,
            )?;
            (Some(right_types.clone()), Some(node_type_ids))
        }
        (None, None) => (None, None),
    };

    let (earliest_time, latest_time) = earliest_latest_times(&layers);

    Ok(TemporalGraph::from_parts(
        graph_dir.to_path_buf(),
        gids.to_boxed(),
        gid_map.into(),
        edge_list,
        global_nodes,
        layers,
        node_props,
        node_type_ids,
        node_types,
        layer_names,
        earliest_time,
        latest_time,
    ))
}

#[instrument(level = "debug", skip_all)]
fn merge_layer(
    graph_dir: &Path,
    global_nodes: &Nodes<AdjTotal>,
    total_num_edges: usize,
    left_graph: &TemporalGraph,
    left_layer_id: usize,
    left_map: &[usize],
    right_graph: &TemporalGraph,
    right_layer_id: usize,
    right_map: &[usize],
    gids: &GIDArray,
) -> Result<TempColGraphFragment, RAError> {
    let left_layer = left_graph.layer(left_layer_id);
    let right_layer = right_graph.layer(right_layer_id);
    let (mut nodes, edges) = merge_adj(
        graph_dir,
        global_nodes,
        total_num_edges,
        gids,
        left_map,
        left_graph,
        left_layer_id,
        right_map,
        right_graph,
        right_layer_id,
    )?;
    let node_additions = merge_additions(
        graph_dir,
        left_layer.nodes.additions(),
        left_map,
        right_layer.nodes.additions(),
        right_map,
        gids.len(),
    )?;
    nodes.additions = node_additions;

    merge_inbounds(
        graph_dir,
        global_nodes,
        left_layer.nodes_storage(),
        left_map,
        right_layer.nodes_storage(),
        right_map,
        &mut nodes,
    )?;

    let earliest = left_layer
        .metadata
        .earliest_time()
        .min(right_layer.metadata.earliest_time());
    let latest = left_layer
        .metadata
        .latest_time()
        .max(right_layer.metadata.latest_time());
    let chunk_size = left_layer
        .metadata
        .chunk_size()
        .max(right_layer.metadata.chunk_size());
    let t_prop_chunk_size = left_layer
        .metadata
        .t_props_chunk_size()
        .max(right_layer.metadata.t_props_chunk_size());
    let layer = TempColGraphFragment {
        nodes,
        edges,
        graph_dir: graph_dir.into(),
        metadata: Metadata::new(earliest, latest, chunk_size, t_prop_chunk_size),
    };
    Ok(layer)
}

fn merge_layer_names(
    left_layers: &[String],
    right_layers: &[String],
) -> (Vec<String>, Vec<EitherOrBoth<usize>>) {
    let mut layer_mapper: HashMap<_, EitherOrBoth<usize>> = left_layers
        .iter()
        .enumerate()
        .map(|(id, name)| (name, EitherOrBoth::Left(id)))
        .collect();
    let mut names = left_layers.to_vec();
    for (id, name) in right_layers.iter().enumerate() {
        match layer_mapper.entry(name) {
            Entry::Occupied(entry) => {
                entry.into_mut().insert_right(id);
            }
            Entry::Vacant(entry) => {
                names.push(name.clone());
                entry.insert(EitherOrBoth::Right(id));
            }
        }
    }
    let ids = names
        .iter()
        .map(|name| layer_mapper.get(name).unwrap().clone())
        .collect();
    (names, ids)
}

fn reindex_and_copy_layer(
    graph_dir: &Path,
    total_num_edges: usize,
    old_layer: &TempColGraphFragment,
    old_edge_list: &EdgeList,
    global_nodes: &Nodes<AdjTotal>,
    node_map: &[usize],
    gids: &GIDArray,
) -> Result<TempColGraphFragment, RAError> {
    let new_nodes = reindex_and_copy_nodes(
        graph_dir,
        &old_layer.nodes,
        old_edge_list,
        global_nodes,
        node_map,
        gids.to_boxed(),
    )?;
    let new_edges = reindex_and_copy_edges(
        graph_dir,
        old_layer.edges_storage(),
        old_layer.nodes.outbound_edges().values(),
        new_nodes.outbound_edges().values(),
        total_num_edges,
    )?;
    let metadata = old_layer.metadata.clone();
    metadata.write_to_path(graph_dir)?;
    Ok(TempColGraphFragment {
        nodes: new_nodes,
        edges: new_edges,
        graph_dir: graph_dir.into(),
        metadata,
    })
}

fn reindex_and_copy_edges(
    graph_dir: &Path,
    old_edges: &EdgeTemporalProps,
    old_layer_edge_ids: &ChunkedArray<PrimitiveArray<u64>, NonNull>,
    layer_edge_ids: &ChunkedArray<PrimitiveArray<u64>, NonNull>,
    total_num_edges: usize,
) -> Result<EdgeTemporalProps, RAError> {
    let fields = old_edges.prop_dtypes();
    let mut new_props = MutChunkedStructArray::new_persisted(
        old_edges.temporal_props().values().chunk_size(),
        graph_dir,
        GraphPaths::EdgeTProps,
        fields.to_vec(),
    );
    for chunk in old_edges.temporal_props().values().iter_chunks() {
        new_props.push_chunk(chunk.values().to_vec())?;
    }

    let new_offsets = fix_t_prop_offsets(
        total_num_edges,
        old_edges.time_col().offsets().chunk_size(),
        graph_dir,
        old_edges.time_col().offsets(),
        old_layer_edge_ids.iter(),
        layer_edge_ids.iter(),
    )?;

    let new_props = new_props.finish()?;

    let edge_t_props = EdgeTemporalProps::from_structs(new_props, new_offsets);
    Ok(edge_t_props)
}

fn reindex_and_copy_nodes(
    graph_dir: &Path,
    old_nodes: &Nodes,
    old_edge_list: &EdgeList,
    global_nodes: &Nodes<AdjTotal>,
    node_map: &[usize],
    gids: Box<dyn Array>,
) -> Result<Nodes, RAError> {
    let new_adj_out_offsets = ChunkedOffsetsBuilder::new_persisted(
        old_nodes.adj_out.edges.offsets().chunk_size(),
        graph_dir,
        GraphPaths::AdjOutOffsets,
    );
    let new_adj_out_offsets = reindex_node_offsets(
        new_adj_out_offsets,
        node_map,
        old_nodes.adj_out.edges.offsets(),
        gids.len(),
    )?;

    let new_adj_in_offsets = ChunkedOffsetsBuilder::new_persisted(
        old_nodes.adj_in.edges.offsets().chunk_size(),
        graph_dir,
        GraphPaths::AdjInOffsets,
    );

    let new_adj_in_offsets = reindex_node_offsets(
        new_adj_in_offsets,
        node_map,
        old_nodes.adj_in.edges.offsets(),
        gids.len(),
    )?;

    let mut new_adj_in_src_ids = MutPrimitiveChunkedArray::new_persisted(
        old_nodes.adj_in.neighbours.values().chunk_size(),
        graph_dir,
        GraphPaths::AdjInSrcs,
    );

    for chunk in old_nodes.adj_in.neighbours.values().iter_chunks() {
        let new_chunk: Vec<_> = chunk
            .par_iter()
            .map(|id| node_map[*id as usize] as u64)
            .collect();
        new_adj_in_src_ids.push_chunk(new_chunk)?;
    }
    let new_adj_in_src_ids = new_adj_in_src_ids.finish()?;

    let mut new_adj_out_dst_ids = MutPrimitiveChunkedArray::new_persisted(
        old_nodes.adj_out.neighbours.values().chunk_size(),
        graph_dir,
        GraphPaths::AdjOutDsts,
    );

    for chunk in old_nodes.adj_out.neighbours.values().iter_chunks() {
        let new_chunk: Vec<_> = chunk
            .par_iter()
            .map(|id| node_map[*id as usize] as u64)
            .collect();
        new_adj_out_dst_ids.push_chunk(new_chunk)?;
    }

    let new_adj_out_dst_ids = new_adj_out_dst_ids.finish()?;

    let mut new_adj_in_edge_ids = MutPrimitiveChunkedArray::new_persisted(
        old_nodes.adj_in.edges.values().chunk_size(),
        graph_dir,
        GraphPaths::AdjInEdges,
    );
    for chunk in old_nodes.adj_in.edges.values().iter_chunks() {
        let new_chunk = chunk
            .iter()
            .map(|&old_eid| {
                let (VID(src), VID(dst)) = old_edge_list.get(EID(old_eid as usize));
                let src_id = VID(node_map[src]);
                let dst_id = VID(node_map[dst]);
                let eid = global_nodes.find_edge(src_id, dst_id).unwrap().0 as u64;
                eid
            })
            .collect::<Vec<_>>();
        new_adj_in_edge_ids.push_chunk(new_chunk)?;
    }
    let new_adj_in_edge_ids = new_adj_in_edge_ids.finish()?;

    let mut new_adj_out_edges_ids = MutPrimitiveChunkedArray::new_persisted(
        old_nodes.adj_out.edges.values().chunk_size(),
        graph_dir,
        GraphPaths::AdjOutEdges,
    );

    for chunk in old_nodes.adj_out.edges.values().iter_chunks() {
        let new_chunk = chunk
            .iter()
            .map(|&old_eid| {
                let (VID(src), VID(dst)) = old_edge_list.get(EID(old_eid as usize));
                let src_id = VID(node_map[src]);
                let dst_id = VID(node_map[dst]);
                let eid = global_nodes.find_edge(src_id, dst_id).unwrap().0 as u64;
                eid
            })
            .collect::<Vec<_>>();
        new_adj_out_edges_ids.push_chunk(new_chunk)?;
    }

    let mut new_additions = MutPrimitiveChunkedArray::new_persisted(
        old_nodes.additions.values().chunk_size(),
        graph_dir,
        GraphPaths::NodeAdditions,
    );
    for chunk in old_nodes.additions.values().iter_chunks() {
        let new_chunk = chunk.to_vec();
        new_additions.push_chunk(new_chunk)?;
    }
    let new_additions = new_additions.finish()?;
    let new_additions_offsets = ChunkedOffsetsBuilder::new_persisted(
        old_nodes.additions.offsets().chunk_size(),
        graph_dir,
        GraphPaths::NodeAdditionsOffsets,
    );
    let new_additions_offsets = reindex_node_offsets(
        new_additions_offsets,
        node_map,
        old_nodes.additions.offsets(),
        gids.len(),
    )?;

    let adj_edges = new_adj_out_edges_ids.finish()?;

    let new_nodes = Nodes {
        adj_out: Adj {
            edges: ChunkedListArray::new_from_parts(adj_edges, new_adj_out_offsets.clone()),
            neighbours: ChunkedListArray::new_from_parts(new_adj_out_dst_ids, new_adj_out_offsets),
        },
        adj_in: Adj {
            edges: ChunkedListArray::new_from_parts(
                new_adj_in_edge_ids,
                new_adj_in_offsets.clone(),
            ),
            neighbours: ChunkedListArray::new_from_parts(new_adj_in_src_ids, new_adj_in_offsets),
        },
        additions: ChunkedListArray::new_from_parts(new_additions, new_additions_offsets),
    };

    Ok(new_nodes)
}

#[cfg(test)]
pub mod test_merge {
    use crate::{
        graph::TemporalGraph,
        merge::merge_graph::merge_graphs,
        prelude::ArrayOps,
        properties::props_eq,
        test::{build_simple_edges, make_graph, make_multilayer_graph},
        GidRef,
    };
    use itertools::{multiunzip, multizip, EitherOrBoth, Itertools};
    use polars_arrow::{
        array::{Array, PrimitiveArray, StructArray, Utf8Array},
        datatypes::{ArrowDataType, Field},
    };
    use proptest::{
        prelude::*,
        sample::{subsequence, Index},
    };
    use raphtory_api::core::entities::EID;
    use rayon::prelude::*;
    use std::rc::Rc;
    use tempfile::TempDir;

    fn gen_node_ids(max_gid: impl Strategy<Value = u64>) -> impl Strategy<Value = Vec<u64>> {
        max_gid.prop_flat_map(|max_gid| {
            let values: Vec<_> = (0..=max_gid).collect();
            let max_num_nodes = (max_gid + 1) as usize;
            subsequence(values, 0..max_num_nodes)
        })
    }

    fn sample_edges(
        num_t_edges: impl Strategy<Value = usize>,
    ) -> impl Strategy<Value = (Vec<Index>, Vec<Index>, Vec<i64>)> {
        num_t_edges.prop_flat_map(|num_t_edges| {
            (
                prop::collection::vec(any::<Index>(), num_t_edges),
                prop::collection::vec(any::<Index>(), num_t_edges),
                prop::collection::vec(any::<i64>(), num_t_edges),
            )
        })
    }

    fn map_edges(
        nodes: &[u64],
        edge_index: (Vec<Index>, Vec<Index>, Vec<i64>),
    ) -> Rc<Vec<(u64, u64, i64)>> {
        if nodes.is_empty() {
            return Rc::new(vec![]);
        }
        let mut edges = multizip(edge_index)
            .map(|(src_idx, dst_idx, time)| (*src_idx.get(&nodes), *dst_idx.get(&nodes), time))
            .collect_vec();
        edges.sort();
        Rc::new(edges)
    }

    fn gen_multilayer_input(
        max_nodes: usize,
        max_t_edges: usize,
        max_layers: usize,
    ) -> impl Strategy<Value = MultilayerGraphInput> {
        let nodes = gen_node_ids(0..max_nodes as u64);
        let layers = subsequence(
            (0..max_layers).map(|i| i.to_string()).collect_vec(),
            0..=max_layers,
        );
        (nodes, layers).prop_flat_map(move |(nodes, layers)| {
            let nodes_rc = Rc::new(nodes);
            let nodes = nodes_rc.clone();
            let layer_edges = layers
                .into_iter()
                .map(move |layer| {
                    let nodes = nodes.clone();
                    let edge_index = sample_edges(0..max_t_edges)
                        .prop_map(move |edge_index| map_edges(&nodes, edge_index));
                    (Just(layer), edge_index)
                })
                .collect_vec();
            layer_edges.prop_map(move |layers| MultilayerGraphInput {
                nodes: nodes_rc.clone(),
                layers: layers.into(),
            })
        })
    }

    #[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
    enum PropType {
        Num,
        Str,
    }

    fn gen_prop_type() -> impl Strategy<Value = PropType> {
        prop_oneof![Just(PropType::Num), Just(PropType::Str)]
    }

    #[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq)]
    enum MergeCase {
        Neither,
        Left,
        Both,
        Right,
    }

    impl MergeCase {
        fn fix(self, left: usize, right: usize) -> MergeCase {
            match self {
                MergeCase::Neither => MergeCase::Neither,
                MergeCase::Left => {
                    if left == 0 {
                        MergeCase::Neither
                    } else {
                        MergeCase::Left
                    }
                }
                MergeCase::Right => {
                    if right == 0 {
                        MergeCase::Neither
                    } else {
                        MergeCase::Right
                    }
                }
                MergeCase::Both => {
                    if left == 0 && right == 0 {
                        MergeCase::Neither
                    } else if left == 0 {
                        MergeCase::Right
                    } else if right == 0 {
                        MergeCase::Left
                    } else {
                        MergeCase::Both
                    }
                }
            }
        }
    }

    fn gen_merge_case(
        has_values: bool,
        has_left: bool,
        has_right: bool,
    ) -> impl Strategy<Value = MergeCase> {
        if !has_values || (!has_left && !has_right) {
            return Just(MergeCase::Neither).boxed();
        }
        if !has_left {
            return prop_oneof![Just(MergeCase::Neither), Just(MergeCase::Right)].boxed();
        }
        if !has_right {
            return prop_oneof![Just(MergeCase::Neither), Just(MergeCase::Left)].boxed();
        }
        prop_oneof![
            Just(MergeCase::Neither),
            Just(MergeCase::Left),
            Just(MergeCase::Right),
            Just(MergeCase::Both),
        ]
        .boxed()
    }

    type TypeInput = Option<Box<dyn Array>>;

    fn gen_types_from_merged(
        max_types: usize,
        left_num_nodes: usize,
        right_num_nodes: usize,
        merged: Rc<Vec<EitherOrBoth<usize>>>,
    ) -> impl Strategy<Value = (TypeInput, TypeInput, TypeInput)> {
        gen_merge_case(max_types > 0, left_num_nodes > 0, right_num_nodes > 0).prop_flat_map(
            move |merge_case| {
                let merged = merged.clone();
                gen_merged_prop_column(
                    merge_case,
                    PropType::Str,
                    left_num_nodes,
                    right_num_nodes,
                    merged,
                )
            },
        )
    }

    fn gen_str_prop_column(num_values: usize) -> impl Strategy<Value = Vec<Option<String>>> {
        prop::collection::vec(any::<Option<String>>(), num_values)
    }

    fn gen_num_prop_column(num_values: usize) -> impl Strategy<Value = Vec<Option<i32>>> {
        prop::collection::vec(any::<Option<i32>>(), num_values)
    }

    fn gen_merged_prop_column(
        merge_case: MergeCase,
        prop_type: PropType,
        left_size: usize,
        right_size: usize,
        merge_idx: Rc<Vec<EitherOrBoth<usize>>>,
    ) -> impl Strategy<
        Value = (
            Option<Box<dyn Array>>,
            Option<Box<dyn Array>>,
            Option<Box<dyn Array>>,
        ),
    > {
        match merge_case {
            MergeCase::Neither => Just((None, None, None)).boxed(),
            MergeCase::Left => match prop_type {
                PropType::Str => gen_str_prop_column(left_size)
                    .prop_map(move |col| {
                        let merged_col = merge_idx
                            .iter()
                            .map(|idx| idx.as_ref().left().and_then(|&i| col[i].clone()))
                            .collect_vec();
                        (
                            Some(Utf8Array::<i64>::from(col).boxed()),
                            None,
                            Some(Utf8Array::<i64>::from(merged_col).boxed()),
                        )
                    })
                    .boxed(),
                PropType::Num => gen_num_prop_column(left_size)
                    .prop_map(move |col| {
                        let merged_col = merge_idx
                            .iter()
                            .map(|idx| idx.as_ref().left().and_then(|&i| col[i].clone()))
                            .collect_vec();
                        (
                            Some(PrimitiveArray::from(col).boxed()),
                            None,
                            Some(PrimitiveArray::from(merged_col).boxed()),
                        )
                    })
                    .boxed(),
            },
            MergeCase::Right => match prop_type {
                PropType::Str => gen_str_prop_column(right_size)
                    .prop_map(move |col| {
                        let merged_col = merge_idx
                            .iter()
                            .map(|idx| idx.as_ref().right().and_then(|&i| col[i].clone()))
                            .collect_vec();
                        (
                            None,
                            Some(Utf8Array::<i64>::from(col).boxed()),
                            Some(Utf8Array::<i64>::from(merged_col).boxed()),
                        )
                    })
                    .boxed(),
                PropType::Num => gen_num_prop_column(right_size)
                    .prop_map(move |col| {
                        let merged_col = merge_idx
                            .iter()
                            .map(|idx| idx.as_ref().right().and_then(|&i| col[i].clone()))
                            .collect_vec();
                        (
                            None,
                            Some(PrimitiveArray::from(col).boxed()),
                            Some(PrimitiveArray::from(merged_col).boxed()),
                        )
                    })
                    .boxed(),
            },
            MergeCase::Both => match prop_type {
                PropType::Str => (
                    gen_str_prop_column(left_size),
                    gen_str_prop_column(right_size),
                )
                    .prop_map(move |(left_col, right_col)| {
                        let merged_col = Utf8Array::<i64>::from_iter(merge_idx.iter().map(|idx| {
                            idx.as_ref()
                                .map_any(|&l| left_col[l].clone(), |&r| right_col[r].clone())
                                .reduce(|l, r| r.or(l))
                        }))
                        .boxed();
                        (
                            Some(Utf8Array::<i64>::from(left_col).boxed()),
                            Some(Utf8Array::<i64>::from(right_col).boxed()),
                            Some(merged_col),
                        )
                    })
                    .boxed(),
                PropType::Num => (
                    gen_num_prop_column(left_size),
                    gen_num_prop_column(right_size),
                )
                    .prop_map(move |(left_col, right_col)| {
                        let merged_col = PrimitiveArray::from_iter(merge_idx.iter().map(|idx| {
                            idx.as_ref()
                                .map_any(|&l| left_col[l], |&r| right_col[r])
                                .reduce(|l, r| r.or(l))
                        }))
                        .boxed();
                        (
                            Some(PrimitiveArray::from(left_col).boxed()),
                            Some(PrimitiveArray::from(right_col).boxed()),
                            Some(merged_col),
                        )
                    })
                    .boxed(),
            },
        }
    }

    fn make_props_struct(
        names: &[String],
        cols: Vec<Option<Box<dyn Array>>>,
    ) -> Option<StructArray> {
        let (fields, cols): (Vec<_>, Vec<_>) = names
            .iter()
            .zip(cols)
            .filter_map(|(n, c)| c.map(|c| (Field::new(n, c.data_type().clone(), false), c)))
            .unzip();
        if fields.is_empty() {
            None
        } else {
            Some(StructArray::new(ArrowDataType::Struct(fields), cols, None))
        }
    }

    fn gen_props_names(
        num_props: usize,
        has_values: bool,
        has_left: bool,
        has_right: bool,
    ) -> impl Strategy<Value = Vec<(MergeCase, String, PropType)>> {
        let props: Vec<_> = (0..num_props)
            .map(|i| {
                (
                    gen_merge_case(has_values, has_left, has_right),
                    Just(i.to_string()),
                    gen_prop_type(),
                )
            })
            .collect();
        props.prop_map(|mut props| {
            props.sort(); // Make sure this is in final merge order (left properties first, then right)
            props
        })
    }

    fn subsample_both(input: EitherOrBoth<usize>) -> impl Strategy<Value = EitherOrBoth<usize>> {
        match input {
            EitherOrBoth::Both(l, r) => {
                prop_oneof![Just(EitherOrBoth::Left(l)), Just(EitherOrBoth::Right(r)),].boxed()
            }
            v => Just(v).boxed(),
        }
    }

    fn gen_nodes_and_timestamps(
        node_idx: Rc<Vec<EitherOrBoth<usize>>>,
        max_ts: usize,
    ) -> impl Strategy<Value = (Vec<EitherOrBoth<usize>>, Vec<i64>)> {
        if node_idx.is_empty() {
            Just((vec![], vec![])).boxed()
        } else {
            prop::collection::vec(
                (0..node_idx.len()).prop_flat_map(move |i| {
                    (Just(i), subsample_both(node_idx[i].clone()), any::<i64>())
                }),
                0..=max_ts,
            )
            .prop_map(|mut v| {
                v.sort_by(|(li, lidx, lt), (ri, ridx, rt)| {
                    (li, lt, lidx.has_left()).cmp(&(ri, rt, ridx.has_left()))
                });
                v.into_iter().map(|(_, idx, t)| (idx, t)).unzip()
            })
            .boxed()
        }
    }

    fn gen_temporal_node_props_from_merged(
        num_props: usize,
        max_num_ts: usize,
        left_nodes: Rc<Vec<u64>>,
        right_nodes: Rc<Vec<u64>>,
        merged_idx: Rc<Vec<EitherOrBoth<usize>>>,
    ) -> impl Strategy<
        Value = (
            Option<StructArray>,
            Option<StructArray>,
            Option<StructArray>,
        ),
    > {
        let nodes_and_timestamps = gen_nodes_and_timestamps(merged_idx.clone(), max_num_ts);
        nodes_and_timestamps.prop_flat_map(move |(node_ids, time_stamps)| {
            let props = gen_props_names(
                num_props,
                num_props > 0,
                node_ids.iter().any(|id| id.has_left()),
                node_ids.iter().any(|id| id.has_right()),
            );
            let node_ids_rc = Rc::new(node_ids);
            let time_stamps = Rc::new(time_stamps);
            let node_ids = node_ids_rc.clone();
            let left_nodes = left_nodes.clone();
            let right_nodes = right_nodes.clone();
            props.prop_flat_map(move |prop_types| {
                let node_ids = node_ids.clone();
                let time_stamps = time_stamps.clone();
                let only_left = prop_types
                    .iter()
                    .all(|(c, _, _)| matches!(c, MergeCase::Left | MergeCase::Neither));
                let only_right = prop_types
                    .iter()
                    .all(|(c, _, _)| matches!(c, MergeCase::Right | MergeCase::Neither));
                let cols: Vec<_> = prop_types
                    .iter()
                    .map(move |(mt, _, pt)| {
                        let node_ids = node_ids.clone();
                        match mt {
                            MergeCase::Neither => Just((None, None, None)).boxed(),
                            MergeCase::Left => match pt {
                                PropType::Str => {
                                    let values = gen_str_prop_column(node_ids.len());
                                    values
                                        .prop_map(move |values| {
                                            let left = Utf8Array::<i64>::from_iter(
                                                values
                                                    .iter()
                                                    .zip(node_ids.iter())
                                                    .filter(|(_, id)| id.has_left())
                                                    .map(|(v, _)| v.as_ref()),
                                            )
                                            .boxed();
                                            let merged = if only_left {
                                                left.clone()
                                            } else {
                                                Utf8Array::<i64>::from_iter(
                                                    values.iter().zip(node_ids.iter()).map(
                                                        |(v, id)| {
                                                            v.as_ref().filter(|_| id.has_left())
                                                        },
                                                    ),
                                                )
                                                .boxed()
                                            };
                                            (Some(left), None, Some(merged))
                                        })
                                        .boxed()
                                }
                                PropType::Num => {
                                    let values = gen_num_prop_column(node_ids.len());
                                    values
                                        .prop_map(move |values| {
                                            let left = PrimitiveArray::from_iter(
                                                values
                                                    .iter()
                                                    .zip(node_ids.iter())
                                                    .filter(|(_, id)| id.has_left())
                                                    .map(|(v, _)| *v),
                                            )
                                            .boxed();
                                            let merged = if only_left {
                                                left.clone()
                                            } else {
                                                PrimitiveArray::from_iter(
                                                    values.iter().zip(node_ids.iter()).map(
                                                        |(&v, id)| v.filter(|_| id.has_left()),
                                                    ),
                                                )
                                                .boxed()
                                            };
                                            (Some(left), None, Some(merged))
                                        })
                                        .boxed()
                                }
                            },
                            MergeCase::Both => match pt {
                                PropType::Str => {
                                    let values = gen_str_prop_column(node_ids.len());
                                    values
                                        .prop_map(move |values| {
                                            let left = Utf8Array::<i64>::from_iter(
                                                values.iter().zip(node_ids.iter()).filter_map(
                                                    |(v, id)| id.has_left().then_some(v.as_ref()),
                                                ),
                                            )
                                            .boxed();
                                            let right = Utf8Array::<i64>::from_iter(
                                                values.iter().zip(node_ids.iter()).filter_map(
                                                    |(v, id)| id.has_right().then_some(v.as_ref()),
                                                ),
                                            )
                                            .boxed();
                                            let merged =
                                                Utf8Array::<i64>::from_iter(values).boxed();
                                            (Some(left), Some(right), Some(merged))
                                        })
                                        .boxed()
                                }
                                PropType::Num => {
                                    let values = gen_num_prop_column(node_ids.len());
                                    values
                                        .prop_map(move |values| {
                                            let left = PrimitiveArray::from_iter(
                                                values.iter().zip(node_ids.iter()).filter_map(
                                                    |(v, id)| id.has_left().then_some(*v),
                                                ),
                                            )
                                            .boxed();
                                            let right = PrimitiveArray::from_iter(
                                                values.iter().zip(node_ids.iter()).filter_map(
                                                    |(v, id)| id.has_right().then_some(*v),
                                                ),
                                            )
                                            .boxed();
                                            let merged = PrimitiveArray::from_iter(values).boxed();
                                            (Some(left), Some(right), Some(merged))
                                        })
                                        .boxed()
                                }
                            },
                            MergeCase::Right => match pt {
                                PropType::Str => {
                                    let values = gen_str_prop_column(node_ids.len());
                                    values
                                        .prop_map(move |values| {
                                            let right = Utf8Array::<i64>::from_iter(
                                                values
                                                    .iter()
                                                    .zip(node_ids.iter())
                                                    .filter(|(_, id)| id.has_right())
                                                    .map(|(v, _)| v.as_ref()),
                                            )
                                            .boxed();
                                            let merged = if only_right {
                                                right.clone()
                                            } else {
                                                Utf8Array::<i64>::from_iter(
                                                    values.iter().zip(node_ids.iter()).map(
                                                        |(v, id)| {
                                                            v.as_ref().filter(|_| id.has_right())
                                                        },
                                                    ),
                                                )
                                                .boxed()
                                            };
                                            (None, Some(right), Some(merged))
                                        })
                                        .boxed()
                                }
                                PropType::Num => {
                                    let values = gen_num_prop_column(node_ids.len());
                                    values
                                        .prop_map(move |values| {
                                            let right = PrimitiveArray::from_iter(
                                                values
                                                    .iter()
                                                    .zip(node_ids.iter())
                                                    .filter(|(_, id)| id.has_right())
                                                    .map(|(v, _)| *v),
                                            )
                                            .boxed();
                                            let merged = if only_right {
                                                right.clone()
                                            } else {
                                                PrimitiveArray::from_iter(
                                                    values.iter().zip(node_ids.iter()).map(
                                                        |(&v, id)| v.filter(|_| id.has_right()),
                                                    ),
                                                )
                                                .boxed()
                                            };
                                            (None, Some(right), Some(merged))
                                        })
                                        .boxed()
                                }
                            },
                        }
                    })
                    .collect();
                let mut all_names = vec!["node".to_string(), "time".to_string()];
                all_names.extend(prop_types.into_iter().map(|(_, name, _)| name));
                let left_nodes = left_nodes.clone();
                let right_nodes = right_nodes.clone();
                let node_ids = node_ids_rc.clone();
                cols.prop_map(move |cols| {
                    let (left, right, merged): (Vec<_>, Vec<_>, Vec<_>) = multiunzip(cols);
                    let left = if left.iter().any(|col| col.is_some()) {
                        let left_node_col = PrimitiveArray::from_values(
                            node_ids
                                .iter()
                                .filter_map(|id| id.as_ref().left().map(|&idx| left_nodes[idx])),
                        )
                        .boxed();
                        let left_ts = PrimitiveArray::from_values(
                            time_stamps
                                .iter()
                                .zip(node_ids.iter())
                                .filter_map(|(ts, id)| id.has_left().then_some(*ts)),
                        )
                        .boxed();
                        let mut all_cols = vec![Some(left_node_col), Some(left_ts)];
                        all_cols.extend(left);
                        make_props_struct(&all_names, all_cols)
                    } else {
                        None
                    };
                    let right = if right.iter().any(|col| col.is_some()) {
                        let right_node_col = PrimitiveArray::from_values(
                            node_ids
                                .iter()
                                .filter_map(|id| id.as_ref().right().map(|&idx| right_nodes[idx])),
                        )
                        .boxed();
                        let right_ts = PrimitiveArray::from_values(
                            time_stamps
                                .iter()
                                .zip(node_ids.iter())
                                .filter_map(|(ts, id)| id.has_right().then_some(*ts)),
                        )
                        .boxed();
                        let mut all_cols = vec![Some(right_node_col), Some(right_ts)];
                        all_cols.extend(right);
                        make_props_struct(&all_names, all_cols)
                    } else {
                        None
                    };
                    let merged =
                        if merged.iter().any(|col| col.is_some()) {
                            let merged_node_col =
                                if only_left {
                                    PrimitiveArray::from_values(node_ids.iter().filter_map(|id| {
                                        id.as_ref().left().map(|&i| left_nodes[i])
                                    }))
                                    .boxed()
                                } else if only_right {
                                    PrimitiveArray::from_values(node_ids.iter().filter_map(|id| {
                                        id.as_ref().right().map(|&i| right_nodes[i])
                                    }))
                                    .boxed()
                                } else {
                                    PrimitiveArray::from_values(node_ids.iter().map(|id| {
                                        id.as_ref()
                                            .map_any(|&i| left_nodes[i], |&i| right_nodes[i])
                                            .reduce(|l, _| l)
                                    }))
                                    .boxed()
                                };
                            let merged_ts_col = if only_left {
                                PrimitiveArray::from_values(
                                    time_stamps
                                        .iter()
                                        .copied()
                                        .zip(node_ids.iter())
                                        .filter(|(_, id)| id.has_left())
                                        .map(|(t, _)| t),
                                )
                                .boxed()
                            } else if only_right {
                                PrimitiveArray::from_values(
                                    time_stamps
                                        .iter()
                                        .copied()
                                        .zip(node_ids.iter())
                                        .filter(|(_, id)| id.has_right())
                                        .map(|(t, _)| t),
                                )
                                .boxed()
                            } else {
                                PrimitiveArray::from_values(time_stamps.iter().copied()).boxed()
                            };
                            let mut all_cols = vec![Some(merged_node_col), Some(merged_ts_col)];
                            all_cols.extend(merged);
                            make_props_struct(&all_names, all_cols)
                        } else {
                            None
                        };
                    (left, right, merged)
                })
            })
        })
    }

    fn gen_const_node_props_from_merged(
        num_props: usize,
        left_num_nodes: usize,
        right_num_nodes: usize,
        merged_idx: Rc<Vec<EitherOrBoth<usize>>>,
    ) -> impl Strategy<
        Value = (
            Option<StructArray>,
            Option<StructArray>,
            Option<StructArray>,
        ),
    > {
        let props = gen_props_names(num_props, true, left_num_nodes > 0, right_num_nodes > 0);
        props.prop_flat_map(move |named| {
            gen_properties(merged_idx.clone(), named, left_num_nodes, right_num_nodes)
        })
    }

    fn gen_properties(
        merged_idx: Rc<Vec<EitherOrBoth<usize>>>,
        named: Vec<(MergeCase, String, PropType)>,
        left_len: usize,
        right_len: usize,
    ) -> impl Strategy<
        Value = (
            Option<StructArray>,
            Option<StructArray>,
            Option<StructArray>,
        ),
    > {
        let merged = merged_idx.clone();
        let vals = named
            .iter()
            .map(move |(mt, _, pt)| {
                let merged = merged.clone();
                let mt = mt.fix(left_len, right_len);
                gen_merged_prop_column(mt, *pt, left_len, right_len, merged)
            })
            .collect_vec();
        let names: Vec<_> = named.into_iter().map(|(_, n, _)| n).collect();
        vals.prop_map(move |cols| {
            let (left, right, merged): (Vec<_>, Vec<_>, Vec<_>) = multiunzip(cols);
            (
                make_props_struct(&names, left),
                make_props_struct(&names, right),
                make_props_struct(&names, merged),
            )
        })
    }

    #[derive(Debug, Clone)]
    struct GraphTestInput {
        nodes: Vec<u64>,
        layers: Vec<(String, Option<StructArray>)>,
        types: Option<Box<dyn Array>>,
        node_c_props: Option<StructArray>,
        node_t_props: Option<StructArray>,
        chunk_size: usize,
        props_chunk_size: usize,
    }

    impl GraphTestInput {
        fn new_from_graph_input(
            graph: &MultilayerGraphInput,
            types: Option<Box<dyn Array>>,
            node_c_props: Option<StructArray>,
            node_t_props: Option<StructArray>,
            edge_t_props: impl IntoIterator<Item = Option<StructArray>>,
            chunk_size: usize,
            props_chunk_size: usize,
        ) -> Self {
            Self {
                nodes: graph.nodes.iter().copied().collect(),
                layers: graph
                    .layers
                    .iter()
                    .zip(edge_t_props)
                    .map(|((name, edges), props)| (name.clone(), make_edge_struct(edges, props)))
                    .collect(),
                types,
                node_c_props,
                node_t_props,
                chunk_size,
                props_chunk_size,
            }
        }

        fn new(
            nodes: Vec<u64>,
            layers: Vec<(String, Option<StructArray>)>,
            types: Option<Box<dyn Array>>,
            node_c_props: Option<StructArray>,
            node_t_props: Option<StructArray>,
            chunk_size: usize,
            props_chunk_size: usize,
        ) -> Self {
            Self {
                nodes,
                layers,
                types,
                node_c_props,
                node_t_props,
                chunk_size,
                props_chunk_size,
            }
        }
    }
    #[derive(Debug)]
    struct MergeTestInput {
        left: GraphTestInput,
        right: GraphTestInput,
        merged: GraphTestInput,
    }

    fn make_edge_struct(
        edges: &[(u64, u64, i64)],
        props: Option<StructArray>,
    ) -> Option<StructArray> {
        if edges.is_empty() {
            return None;
        }
        let (srcs, dsts, times): (Vec<_>, Vec<_>, Vec<_>) = multiunzip(edges.iter().copied());
        let srcs = PrimitiveArray::from_vec(srcs).boxed();
        let dsts = PrimitiveArray::from_vec(dsts).boxed();
        let times = PrimitiveArray::from_vec(times).boxed();

        let mut fields = vec![
            Field::new("time", times.data_type().clone(), false),
            Field::new("src", srcs.data_type().clone(), false),
            Field::new("dst", dsts.data_type().clone(), false),
        ];
        let mut values = vec![times, srcs, dsts];

        if let Some(props) = props {
            let (prop_fields, prop_values, _) = props.into_data();
            fields.extend(prop_fields);
            values.extend(prop_values);
        }

        Some(StructArray::new(
            ArrowDataType::Struct(fields),
            values,
            None,
        ))
    }

    fn gen_edge_props_from_merged(
        max_edge_props: usize,
        left_layers: Rc<Vec<(String, Rc<Vec<(u64, u64, i64)>>)>>,
        right_layers: Rc<Vec<(String, Rc<Vec<(u64, u64, i64)>>)>>,
        merged_layers: Rc<Vec<(EitherOrBoth<usize>, Rc<Vec<EitherOrBoth<usize, usize>>>)>>,
    ) -> impl Strategy<
        Value = (
            Vec<Option<StructArray>>,
            Vec<Option<StructArray>>,
            Vec<Option<StructArray>>,
        ),
    > {
        let left_num_layers = left_layers.len();
        let right_num_layers = right_layers.len();

        let has_left = left_layers.iter().any(|(_, edges)| !edges.is_empty());
        let has_right = right_layers.iter().any(|(_, edges)| !edges.is_empty());

        gen_props_names(max_edge_props, true, has_left, has_right).prop_flat_map(
            move |prop_names| {
                let left_layers = left_layers.clone();
                let right_layers = right_layers.clone();
                let merged_layers = merged_layers.clone();
                merged_layers
                    .iter()
                    .map(move |(l_index, e_index)| {
                        let left_num_edges = l_index
                            .as_ref()
                            .left()
                            .map(|&i| left_layers[i].1.len())
                            .unwrap_or(0);
                        let right_num_edges = l_index
                            .as_ref()
                            .right()
                            .map(|&i| right_layers[i].1.len())
                            .unwrap_or(0);
                        gen_properties(
                            e_index.clone(),
                            prop_names.clone(),
                            left_num_edges,
                            right_num_edges,
                        )
                    })
                    .collect_vec()
                    .prop_map(move |v| {
                        let mut left = vec![None; left_num_layers];
                        let mut right = vec![None; right_num_layers];
                        let merged = v
                            .into_iter()
                            .zip(merged_layers.iter())
                            .map(|((lv, rv, mv), (layer_idx, _))| {
                                if let Some(li) = layer_idx.as_ref().left() {
                                    left[*li] = lv;
                                }
                                if let Some(ri) = layer_idx.as_ref().right() {
                                    right[*ri] = rv;
                                }
                                mv
                            })
                            .collect_vec();
                        (left, right, merged)
                    })
            },
        )
    }

    fn gen_merge_test_input(
        max_nodes: usize,
        max_t_edges: usize,
        max_layers: usize,
        max_types: usize,
        max_node_cprops: usize,
        max_node_tprops: usize,
        max_node_ts: usize,
        max_edge_props: usize,
        l_c: impl Strategy<Value = usize>,
        l_t_c: impl Strategy<Value = usize>,
        r_c: impl Strategy<Value = usize>,
        r_t_c: impl Strategy<Value = usize>,
    ) -> impl Strategy<Value = MergeTestInput> {
        let left = gen_multilayer_input(max_nodes, max_t_edges, max_layers);
        let right = gen_multilayer_input(max_nodes, max_t_edges, max_layers);
        (left, right, l_c, l_t_c, r_c, r_t_c).prop_flat_map(
            move |(left, right, l_c, l_t_c, r_c, r_t_c)| {
                let merged = merge_nodes_and_edges(&left, &right);
                let types = gen_types_from_merged(
                    max_types,
                    left.nodes.len(),
                    right.nodes.len(),
                    merged.node_idx.clone(),
                );
                let node_c_props = gen_const_node_props_from_merged(
                    max_node_cprops,
                    left.nodes.len(),
                    right.nodes.len(),
                    merged.node_idx.clone(),
                );
                let node_t_props = gen_temporal_node_props_from_merged(
                    max_node_tprops,
                    max_node_ts,
                    left.nodes.clone(),
                    right.nodes.clone(),
                    merged.node_idx.clone(),
                );
                let edge_t_props = gen_edge_props_from_merged(
                    max_edge_props,
                    left.layers.clone(),
                    right.layers.clone(),
                    merged.layer_idx.clone(),
                );
                (types, node_c_props, node_t_props, edge_t_props).prop_map(
                    move |(
                        (l_types, r_types, m_types),
                        (l_props, r_props, m_props),
                        (l_t_props, r_t_props, m_t_props),
                        (l_e_props, r_e_props, m_e_props),
                    )| {
                        let left = GraphTestInput::new_from_graph_input(
                            &left, l_types, l_props, l_t_props, l_e_props, l_c, l_t_c,
                        );
                        let right = GraphTestInput::new_from_graph_input(
                            &right, r_types, r_props, r_t_props, r_e_props, r_c, r_t_c,
                        );
                        let merged = GraphTestInput::new_from_graph_input(
                            &merged.graph,
                            m_types,
                            m_props,
                            m_t_props,
                            m_e_props,
                            l_c.max(r_c),
                            l_t_c.max(r_t_c),
                        );
                        MergeTestInput {
                            left,
                            right,
                            merged,
                        }
                    },
                )
            },
        )
    }

    struct MergedInput {
        graph: MultilayerGraphInput,
        node_idx: Rc<Vec<EitherOrBoth<usize>>>,
        layer_idx: Rc<Vec<(EitherOrBoth<usize>, Rc<Vec<EitherOrBoth<usize>>>)>>,
    }

    fn merge_nodes_and_edges(
        left: &MultilayerGraphInput,
        right: &MultilayerGraphInput,
    ) -> MergedInput {
        let (nodes, node_idx): (Vec<_>, Vec<_>) = left
            .nodes
            .iter()
            .enumerate()
            .merge_join_by(right.nodes.iter().enumerate(), |(_, l), (_, r)| l.cmp(r))
            .map(|merged| match merged {
                EitherOrBoth::Both((l_i, l), (r_i, _)) => (*l, EitherOrBoth::Both(l_i, r_i)),
                EitherOrBoth::Left((l_i, l)) => (*l, EitherOrBoth::Left(l_i)),
                EitherOrBoth::Right((r_i, r)) => (*r, EitherOrBoth::Right(r_i)),
            })
            .unzip();

        let (mut layers, mut layer_idx): (Vec<_>, Vec<_>) = left
            .layers
            .iter()
            .enumerate()
            .map(|(id, (name, edges))| {
                (
                    (name.clone(), edges.clone()),
                    (
                        EitherOrBoth::Left(id),
                        Rc::new(
                            (0..edges.len())
                                .map(|i| EitherOrBoth::Left(i))
                                .collect_vec(),
                        ),
                    ),
                )
            })
            .unzip();

        for (li, (right_layer, r_e)) in right.layers.iter().enumerate() {
            match left
                .layers
                .binary_search_by(|(left_layer, _)| left_layer.cmp(right_layer))
            {
                Ok(i) => {
                    let (_, l_e) = &mut layers[i];
                    let (l_id, l_idx) = &mut layer_idx[i];
                    l_id.insert_right(li);
                    let (edges, e_idx): (Vec<_>, Vec<_>) = l_e
                        .iter()
                        .copied()
                        .zip(l_idx.iter().cloned())
                        .merge_by(
                            r_e.iter()
                                .copied()
                                .enumerate()
                                .map(|(i, e)| (e, EitherOrBoth::Right(i))),
                            |(l, _), (r, _)| l <= r,
                        )
                        .unzip();
                    *l_e = edges.into();
                    *l_idx = Rc::new(e_idx);
                }
                Err(_) => {
                    layers.push((right_layer.clone(), r_e.clone()));
                    layer_idx.push((
                        EitherOrBoth::Right(li),
                        Rc::new((0..r_e.len()).map(EitherOrBoth::Right).collect()),
                    ));
                }
            }
        }
        MergedInput {
            graph: MultilayerGraphInput {
                nodes: Rc::new(nodes),
                layers: Rc::new(layers),
            },
            node_idx: Rc::new(node_idx),
            layer_idx: Rc::new(layer_idx),
        }
    }

    #[derive(Debug)]
    struct MultilayerGraphInput {
        nodes: Rc<Vec<u64>>,
        layers: Rc<Vec<(String, Rc<Vec<(u64, u64, i64)>>)>>,
    }

    use pretty_assertions::assert_eq;

    fn assert_same_graph(left: &TemporalGraph, right: &TemporalGraph) {
        assert_eq!(left.num_nodes(), right.num_nodes());
        assert_eq!(left.layer_names(), right.layer_names());
        match (left.node_types(), right.node_types()) {
            (Some(lt), Some(rt)) => {
                assert!(
                    left.node_type_ids()
                        .unwrap()
                        .iter()
                        .map(|id| lt.value(id as usize))
                        .eq(right
                            .node_type_ids()
                            .unwrap()
                            .iter()
                            .map(|id| rt.value(id as usize))),
                    "Mismatched node types, left: {lt:?}, right: {rt:?}, left_ids: {:?}, right_ids: {:?}", left.node_type_ids(), right.node_type_ids());
            }
            (None, None) => {}
            (lt, rt) => panic!("Mismatched node types, left: {lt:?}, right: {rt:?}"),
        }
        assert_eq!(left.node_properties(), right.node_properties());
        assert_eq!(left.global_ordering(), right.global_ordering());
        assert_eq!(left.global_mapping(), right.global_mapping());
        assert_eq!(left.earliest(), right.earliest());
        assert_eq!(left.latest(), right.latest());

        for (layer_id, (l, r)) in left.layers().iter().zip_eq(right.layers()).enumerate() {
            let l_adj_out: Vec<_> = l
                .nodes_storage()
                .outbound_neighbours()
                .par_exploded_indexed()
                .collect();
            let r_adj_out: Vec<_> = r
                .nodes_storage()
                .outbound_neighbours()
                .par_exploded_indexed()
                .collect();
            assert_eq!(
                l_adj_out, r_adj_out,
                "Mismatched adj_out, left: {l_adj_out:?}, right: {r_adj_out:?}"
            );

            let l_adj_out_n: Vec<_> = l
                .nodes_storage()
                .outbound_edges()
                .par_exploded_indexed()
                .collect();
            let r_adj_out_n: Vec<_> = r
                .nodes_storage()
                .outbound_edges()
                .par_exploded_indexed()
                .collect();

            assert_eq!(
                l_adj_out_n, r_adj_out_n,
                "Mismatched outbound edges, left: {l_adj_out_n:?}, right: {r_adj_out_n:?}"
            );

            let l_adj_in_n: Vec<_> = l
                .nodes_storage()
                .inbound_neighbours()
                .par_exploded_indexed()
                .collect();
            let r_adj_in_n: Vec<_> = r
                .nodes_storage()
                .inbound_neighbours()
                .par_exploded_indexed()
                .collect();
            assert_eq!(
                l_adj_in_n, r_adj_in_n,
                "Mismatched adj_in_neighbours, left: {l_adj_in_n:?}, right: {r_adj_in_n:?}"
            );

            let l_adj_in_e: Vec<_> = l
                .nodes_storage()
                .inbound_edges()
                .par_exploded_indexed()
                .collect();
            let r_adj_in_e: Vec<_> = r
                .nodes_storage()
                .inbound_edges()
                .par_exploded_indexed()
                .collect();
            assert_eq!(
                l_adj_in_e, r_adj_in_e,
                "Mismatched [{layer_id}] adj_in_edges, left: {l_adj_in_e:?}, right: {r_adj_in_e:?}"
            );

            let l_add: Vec<_> = l
                .nodes_storage()
                .additions()
                .par_exploded_indexed()
                .collect();
            let r_add: Vec<_> = r
                .nodes_storage()
                .additions()
                .par_exploded_indexed()
                .collect();
            assert_eq!(
                l_add, r_add,
                "Mismatched additions, left: {l_add:?}, right: {r_add:?}"
            );

            let l_srcs: Vec<_> = l
                .nodes_storage()
                .outbound_edges()
                .values()
                .iter()
                .map(|eid| left.edge(EID(eid as usize)).src_id())
                .collect();
            let r_srcs: Vec<_> = r
                .nodes_storage()
                .outbound_edges()
                .values()
                .iter()
                .map(|eid| right.edge(EID(eid as usize)).src_id())
                .collect();
            assert_eq!(
                l_srcs, r_srcs,
                "Mismatched srcs, left: {l_srcs:?}, right: {r_srcs:?}"
            );

            let l_dsts: Vec<_> = l
                .nodes_storage()
                .outbound_neighbours()
                .values()
                .iter()
                .collect();
            let r_dsts: Vec<_> = r
                .nodes_storage()
                .outbound_neighbours()
                .values()
                .iter()
                .collect();
            assert_eq!(
                l_dsts, r_dsts,
                "Mismatched dsts, left: {l_dsts:?}, right: {r_dsts:?}"
            );

            let l_times: Vec<_> = l
                .edges_storage()
                .time_col()
                .par_exploded_indexed()
                .collect();
            let r_times: Vec<_> = r
                .edges_storage()
                .time_col()
                .par_exploded_indexed()
                .collect();
            assert_eq!(
                l_times,
                r_times,
                "Mismatched times [{layer_id}], left: {:?}, right; {:?}",
                l.edges_storage().time_col(),
                r.edges_storage().time_col()
            );

            assert_eq!(
                l.earliest_time(),
                r.earliest_time(),
                "Mismatched earliest_time, left: {}, right: {}",
                l.earliest_time(),
                r.earliest_time()
            );
            assert_eq!(
                l.latest_time(),
                r.latest_time(),
                "Mismatched latest_time, left: {}, right: {}",
                l.latest_time(),
                r.latest_time()
            );
            assert!(
                props_eq(
                    l.edges_storage().temporal_props().values(),
                    r.edges_storage().temporal_props().values(),
                ),
                "{layer_id } Mismatched edge property values, left: {:?}, right: {:?}",
                l.edges_storage().temporal_props().values(),
                r.edges_storage().temporal_props().values()
            );
            assert_eq!(
                l.edges_storage().temporal_props().offsets(),
                r.edges_storage().temporal_props().offsets()
            )
        }
    }

    fn merge_test_inner_multilayer(
        left_g: GraphTestInput,
        right_g: GraphTestInput,
        all_g: GraphTestInput,
    ) {
        let (left_g, _) = make_multilayer_graph(
            left_g.nodes,
            left_g.layers,
            left_g.types,
            left_g.node_c_props,
            left_g.node_t_props,
            left_g.chunk_size,
            left_g.props_chunk_size,
        );
        let (right_g, _) = make_multilayer_graph(
            right_g.nodes,
            right_g.layers,
            right_g.types,
            right_g.node_c_props,
            right_g.node_t_props,
            right_g.chunk_size,
            right_g.props_chunk_size,
        );
        let (all_g, _) = make_multilayer_graph(
            all_g.nodes,
            all_g.layers,
            all_g.types,
            all_g.node_c_props,
            all_g.node_t_props,
            all_g.chunk_size,
            all_g.props_chunk_size,
        );
        let test_dir = TempDir::new().unwrap();
        let merged_g = merge_graphs(test_dir.path(), &left_g, &right_g).unwrap();
        assert_same_graph(&merged_g, &all_g)
    }

    #[test]
    fn multilayer_merge_test() {
        proptest!(|(input in gen_merge_test_input(10, 10, 3, 3, 3, 3, 10, 3, 1usize..10, 1usize..10, 1usize..10, 1usize..10), num_threads in 1usize..10)| {
            let pool = rayon::ThreadPoolBuilder::new().num_threads(num_threads).build().unwrap();
            pool.install(|| {
                merge_test_inner_multilayer(input.left, input.right, input.merged)
            })
        })
    }

    #[test]
    fn test_edge_properties() {
        proptest!(|(input in gen_merge_test_input(10, 20, 3, 0, 0, 0, 0, 3, 1usize..10, 1usize..10, 1usize..10, 1usize..10), num_threads in 1usize..10)| {
            let pool = rayon::ThreadPoolBuilder::new().num_threads(num_threads).build().unwrap();
            pool.install(|| {
                merge_test_inner_multilayer(input.left, input.right, input.merged)
            })
        })
    }

    #[test]
    fn test_edge_properties_fail1() {
        let input = MergeTestInput {
            left: GraphTestInput {
                nodes: vec![0, 1, 2, 3],
                layers: vec![
                    (
                        "1".to_string(),
                        make_edge_struct(&[(0, 3, -5945314627683170009)], None),
                    ),
                    (
                        "2".to_string(),
                        make_edge_struct(
                            &[(0, 3, -3900827985801567991), (3, 0, -5365312088194817115)],
                            None,
                        ),
                    ),
                ],
                types: None,
                node_c_props: None,
                node_t_props: None,
                chunk_size: 7,
                props_chunk_size: 2,
            },
            right: GraphTestInput {
                nodes: vec![],
                layers: vec![],
                types: None,
                node_c_props: None,
                node_t_props: None,
                chunk_size: 2,
                props_chunk_size: 4,
            },
            merged: GraphTestInput {
                nodes: vec![0, 1, 2, 3],
                layers: vec![
                    (
                        "1".to_string(),
                        make_edge_struct(&[(0, 3, -5945314627683170009)], None),
                    ),
                    (
                        "2".to_string(),
                        make_edge_struct(
                            &[(0, 3, -3900827985801567991), (3, 0, -5365312088194817115)],
                            None,
                        ),
                    ),
                ],
                types: None,
                node_c_props: None,
                node_t_props: None,
                chunk_size: 7,
                props_chunk_size: 4,
            },
        };
        merge_test_inner_multilayer(input.left, input.right, input.merged);
    }

    #[test]
    fn test_edge_properties_fail2() {
        let input = MergeTestInput {
            left: GraphTestInput {
                nodes: vec![2, 3],
                layers: vec![
                    (
                        "0".to_string(),
                        make_edge_struct(
                            &[
                                (2, 2, -8790266959658037302),
                                (2, 3, -5863041061550971579),
                                (2, 3, -3860421863066847700),
                                (2, 3, 4718133631310592359),
                                (2, 3, 4718133631310592359),
                                (2, 3, 5108729448956548825),
                                (2, 3, 5274503720833175077),
                                (2, 3, 6969999048205749982),
                                (3, 2, -7665093246617648727),
                                (3, 2, 1212183696274971444),
                                (3, 2, 7459752815353423290),
                                (3, 3, -3652064179733234825),
                                (3, 3, -1813238502943490221),
                            ],
                            None,
                        ),
                    ),
                    (
                        "1".to_string(),
                        make_edge_struct(
                            &[
                                (2, 2, -4676480839564749383),
                                (2, 2, -796527581648319088),
                                (2, 2, 6599699557202409914),
                                (2, 2, 6713710443665300146),
                                (2, 2, 8548336610588223223),
                                (2, 3, -8625946798564243121),
                                (2, 3, -5978017797878982853),
                                (2, 3, 2643990301082762767),
                                (2, 3, 8508186570548576698),
                                (3, 2, -8974343865488786402),
                                (3, 2, -1151494688392905782),
                                (3, 2, 6678701855379434317),
                                (3, 3, 6105818091045437248),
                            ],
                            None,
                        ),
                    ),
                    (
                        "2".to_string(),
                        make_edge_struct(
                            &[
                                (2, 2, -3459410470056403076),
                                (2, 2, -1442058839372107461),
                                (2, 2, 1347048756055893080),
                                (2, 2, 4245753183956832294),
                                (2, 2, 5801119603067364603),
                                (2, 2, 6217278671828409931),
                                (2, 2, 7352129614553342396),
                                (2, 3, -8748395263211815480),
                                (3, 2, -7497908467785936036),
                                (3, 2, -3152312160890338183),
                                (3, 2, 2905216384453491612),
                                (3, 3, -3739101010679472286),
                                (3, 3, -2373047721065137989),
                                (3, 3, -867292776505532128),
                                (3, 3, 469038746769819232),
                                (3, 3, 2695973115316366469),
                                (3, 3, 5927099026687572943),
                                (3, 3, 7430691676735013103),
                            ],
                            None,
                        ),
                    ),
                ],
                types: None,
                node_c_props: None,
                node_t_props: None,
                chunk_size: 7,
                props_chunk_size: 4,
            },
            right: GraphTestInput {
                nodes: vec![0, 2, 3],
                layers: vec![
                    (
                        "1".to_string(),
                        make_edge_struct(
                            &[(3, 0, -4870647468080823253), (3, 2, -7974464349526214689)],
                            None,
                        ),
                    ),
                    (
                        "2".to_string(),
                        make_edge_struct(
                            &[(0, 3, 3173818553911961396), (0, 3, 4032189829297051356)],
                            None,
                        ),
                    ),
                ],
                types: None,
                node_c_props: None,
                node_t_props: None,
                chunk_size: 8,
                props_chunk_size: 1,
            },
            merged: GraphTestInput {
                nodes: vec![0, 2, 3],
                layers: vec![
                    (
                        "0".to_string(),
                        make_edge_struct(
                            &[
                                (2, 2, -8790266959658037302),
                                (2, 3, -5863041061550971579),
                                (2, 3, -3860421863066847700),
                                (2, 3, 4718133631310592359),
                                (2, 3, 4718133631310592359),
                                (2, 3, 5108729448956548825),
                                (2, 3, 5274503720833175077),
                                (2, 3, 6969999048205749982),
                                (3, 2, -7665093246617648727),
                                (3, 2, 1212183696274971444),
                                (3, 2, 7459752815353423290),
                                (3, 3, -3652064179733234825),
                                (3, 3, -1813238502943490221),
                            ],
                            None,
                        ),
                    ),
                    (
                        "1".to_string(),
                        make_edge_struct(
                            &[
                                (2, 2, -4676480839564749383),
                                (2, 2, -796527581648319088),
                                (2, 2, 6599699557202409914),
                                (2, 2, 6713710443665300146),
                                (2, 2, 8548336610588223223),
                                (2, 3, -8625946798564243121),
                                (2, 3, -5978017797878982853),
                                (2, 3, 2643990301082762767),
                                (2, 3, 8508186570548576698),
                                (3, 0, -4870647468080823253),
                                (3, 2, -8974343865488786402),
                                (3, 2, -7974464349526214689),
                                (3, 2, -1151494688392905782),
                                (3, 2, 6678701855379434317),
                                (3, 3, 6105818091045437248),
                            ],
                            None,
                        ),
                    ),
                    (
                        "2".to_string(),
                        make_edge_struct(
                            &[
                                (0, 3, 3173818553911961396),
                                (0, 3, 4032189829297051356),
                                (2, 2, -3459410470056403076),
                                (2, 2, -1442058839372107461),
                                (2, 2, 1347048756055893080),
                                (2, 2, 4245753183956832294),
                                (2, 2, 5801119603067364603),
                                (2, 2, 6217278671828409931),
                                (2, 2, 7352129614553342396),
                                (2, 3, -8748395263211815480),
                                (3, 2, -7497908467785936036),
                                (3, 2, -3152312160890338183),
                                (3, 2, 2905216384453491612),
                                (3, 3, -3739101010679472286),
                                (3, 3, -2373047721065137989),
                                (3, 3, -867292776505532128),
                                (3, 3, 469038746769819232),
                                (3, 3, 2695973115316366469),
                                (3, 3, 5927099026687572943),
                                (3, 3, 7430691676735013103),
                            ],
                            None,
                        ),
                    ),
                ],
                types: None,
                node_c_props: None,
                node_t_props: None,
                chunk_size: 8,
                props_chunk_size: 4,
            },
        };
        merge_test_inner_multilayer(input.left, input.right, input.merged)
    }

    #[test]
    fn test_edge_properties_fail3() {
        // input = MergeTestInput {
        //     left: GraphTestInput {
        //         nodes: [
        //             3,
        //         ],
        //         layers: [
        //             (
        //                 "0",
        //                 Some(
        //                     StructArray[{time: -8959471431607301714, src: 3, dst: 3, 1: None, 2: \￼N㈑𑤐FO/ೀO}, {time: -8370204226306237842, src: 3, dst: 3, 1: None, 2: None}, {time: -3905452029229723274, src: 3, dst: 3, 1: None, 2: None}, {time: -3004290179491894154, src: 3, dst: 3, 1: None, 2: ෞ?"/𬕨¥p&Y**Ѩd>𐮫🕴𑣔K}*q¥`<🪱'�`}, {time: 2627216774690630900, src: 3, dst: 3, 1: None, 2: None}, {time: 5471495323309507066, src: 3, dst: 3, 1: None, 2: Ⱥ:{z=}, {time: 7730937374962123815, src: 3, dst: 3, 1: None, 2: =SA"ê}],
        //                 ),
        //             ),
        //             (
        //                 "1",
        //                 Some(
        //                     StructArray[{time: -8069755937079635973, src: 3, dst: 3, 2: 540297124}, {time: -2108721513160478139, src: 3, dst: 3, 2: -938218464}, {time: -350798471955118581, src: 3, dst: 3, 2: None}, {time: 8474707726855679655, src: 3, dst: 3, 2: None}],
        //                 ),
        //             ),
        //             (
        //                 "2",
        //                 Some(
        //                     StructArray[{time: -8300850366113978842, src: 3, dst: 3, 1: 1174831362}, {time: -6214897813967589599, src: 3, dst: 3, 1: None}, {time: -3104668713472349576, src: 3, dst: 3, 1: 481418824}, {time: 4552452772834849983, src: 3, dst: 3, 1: 2033193117}, {time: 5737410735888382198, src: 3, dst: 3, 1: None}, {time: 7045881871089359872, src: 3, dst: 3, 1: None}],
        //                 ),
        //             ),
        //         ],
        //         types: None,
        //         node_c_props: None,
        //         node_t_props: None,
        //         chunk_size: 4,
        //         props_chunk_size: 7,
        //     },
        let left = GraphTestInput {
            nodes: vec![3],
            layers: vec![
                (
                    "0".to_string(),
                    make_edge_struct(
                        &[
                            (3, 3, -8959471431607301714),
                            (3, 3, -8370204226306237842),
                            (3, 3, -3905452029229723274),
                            (3, 3, -3004290179491894154),
                            (3, 3, 2627216774690630900),
                            (3, 3, 5471495323309507066),
                            (3, 3, 7730937374962123815),
                        ],
                        None,
                    ),
                ),
                (
                    "1".to_string(),
                    make_edge_struct(
                        &[
                            (3, 3, -8069755937079635973),
                            (3, 3, -2108721513160478139),
                            (3, 3, -350798471955118581),
                            (3, 3, 8474707726855679655),
                        ],
                        None,
                    ),
                ),
                (
                    "2".to_string(),
                    make_edge_struct(
                        &[
                            (3, 3, -8300850366113978842),
                            (3, 3, -6214897813967589599),
                            (3, 3, -3104668713472349576),
                            (3, 3, 4552452772834849983),
                            (3, 3, 5737410735888382198),
                            (3, 3, 7045881871089359872),
                        ],
                        None,
                    ),
                ),
            ],
            types: None,
            node_c_props: None,
            node_t_props: None,
            chunk_size: 4,
            props_chunk_size: 7,
        };
        //     right: GraphTestInput {
        //         nodes: [
        //             0,
        //             1,
        //             2,
        //             5,
        //         ],
        //         layers: [
        //             (
        //                 "0",
        //                 Some(
        //                     StructArray[{time: -6946189257675522426, src: 0, dst: 1, 1: None, 2: p4𑖽PN$𝨜\*𐓏?{:𚿲'{Ῑ`E;'𝕆}, {time: -3077934556543664082, src: 0, dst: 1, 1: ૐ!t&ﺙ𞅅𒿢6?-5c, 2: Ⱥ)%్}, {time: -1003310608997642694, src: 0, dst: 1, 1: લp%ۻ{𞸀UqѨ*ￅ*\<z.&?𑧤¥𝋉$»꡴·𱛯, 2: None}, {time: -7426350489886598556, src: 0, dst: 2, 1: ¥z=𝕃, 2: 𝐶$ :ØI𑴈𐿮ਅ"🕴🉑}, {time: -2795532415998143321, src: 1, dst: 0, 1: None, 2:  \=ᜓ&ꟑ{.-K=VȺk=𑋱.¥ [?/Dহ}, {time: -8030557141856176232, src: 1, dst: 1, 1: , 2: <໕-o𑂰}, {time: 3890797442906057970, src: 1, dst: 1, 1: "6Ð9\<ਂ>:q`;%''"W&\Ü, 2: None}, {time: -5531033197350346743, src: 1, dst: 5, 1: 𐖻:ぴ|!$፩𑃵IN:ⶻ𑴉𖭟0𞣇⁞h, 2: 𑗖¥4 `}, {time: 2274525496805076585, src: 1, dst: 5, 1: $𐤰, 2: None}, {time: -3614222918805423072, src: 2, dst: 0, 1: None, 2: Ｚዼ𞸴ቚ4.𐲒¸🕴ꬑྌ;s7𛱉8'𖽥ѨዀﬓѨ$|&𐔐ὰ+}, {time: -2964156677504024688, src: 2, dst: 2, 1: UᜣῚѨ𞹙$"𐺱r<🪈𒓊�Z𐣵, 2: Ѩ&𑊑G🄇Ὕm}, {time: -5146928027062721059, src: 2, dst: 5, 1: `{=&𐨒{5"𞸧S𞹔4+"<!$SedCԱ¢Z$s%, 2: 𝕆ળ⑈ͽ}, {time: 6907944027963841409, src: 5, dst: 2, 1: None, 2: 𫆩/𐇨🭼"{ࡋ𑍌¥K=ա𒾔﷏"V𚿱Ѩ'$ঊ𐕑𑆗.[*e}, {time: 7647094936185192977, src: 5, dst: 2, 1: -ৠ⛝<{𞹟Zᯫ*ਆ<RὒP🂲ৱ'𑧣"꯶ਸT, 2: J'>𐒘ন𐧉%*�=}𐫵<𐨿𐴸𞹤§<*𞥑/𖫵'Ѩ}, {time: 7699132147983612225, src: 5, dst: 5, 1: None, 2: ﷏𐄘|𖭐𑻰Ⱥૐ𝄌'}],
        //                 ),
        //             ),
        //             (
        //                 "1",
        //                 Some(
        //                     StructArray[{time: -5560381993853069877, src: 0, dst: 0, 2: 1996651016, 0: None, 1: 1590267470}, {time: -5472276360197243206, src: 0, dst: 0, 2: None, 0: None, 1: 1320544567}, {time: 7076886535070555787, src: 0, dst: 0, 2: 1149145534, 0: None, 1: None}, {time: -6565749092322173179, src: 0, dst: 1, 2: None, 0: None, 1: -850427537}, {time: -3686318905505156857, src: 0, dst: 1, 2: None, 0: 1751653540, 1: 1822693927}, {time: -2738585546365182324, src: 0, dst: 1, 2: 1506791915, 0: 508637071, 1: -448967720}, {time: -4277784028792630760, src: 0, dst: 5, 2: -1576928800, 0: -1058902216, 1: -543037163}, {time: 6440245090124588543, src: 0, dst: 5, 2: None, 0: None, 1: -461676893}, {time: 7112990369392856699, src: 1, dst: 0, 2: 663248545, 0: None, 1: None}, {time: 3024513833601334897, src: 1, dst: 1, 2: None, 0: None, 1: None}, {time: 4987313276447385600, src: 1, dst: 2, 2: None, 0: None, 1: -868276681}, {time: 6955697344957532988, src: 1, dst: 5, 2: None, 0: 1294894725, 1: None}, {time: -5262901447702039929, src: 2, dst: 0, 2: None, 0: -601789892, 1: None}, {time: 8123919435694227826, src: 2, dst: 0, 2: None, 0: None, 1: None}, {time: 8574203868684198909, src: 2, dst: 2, 2: None, 0: 1077520285, 1: -171380916}, {time: 3098838084725181777, src: 5, dst: 1, 2: 1268103956, 0: None, 1: -1734658364}, {time: -52316127849277753, src: 5, dst: 2, 2: -1277558555, 0: None, 1: None}, {time: -5741198006060425984, src: 5, dst: 5, 2: None, 0: None, 1: 1353362200}],
        //                 ),
        //             ),
        //             (
        //                 "2",
        //                 Some(
        //                     StructArray[{time: 1866712518627396198, src: 0, dst: 0, 1: None, 0: None}, {time: -7791749991472020317, src: 0, dst: 2, 1: -1985106975, 0: 𐤿*￼dꪕꢾ🉣)ⷞૠ𞺢¥-𒅩🕴🕴#𑵖/ኴ`:𑌽}, {time: 3304203354872896569, src: 0, dst: 2, 1: -2096284107, 0: q\^𐁝}, {time: -6032436129150284651, src: 0, dst: 5, 1: None, 0: None}, {time: 8096767526243760246, src: 0, dst: 5, 1: -1765142288, 0: ୂ🠂";EವD*𝒦áO*ਜ਼úᰍa%a{"🫕=}, {time: -8107222458096274499, src: 1, dst: 1, 1: None, 0: ﷏𐤢/a𐤿IÊ\ரy𖼒ZZⴭ%Ὓ%[🟨$G𛲟/𥳐ఐ🈶Ä&}, {time: 3589138218193040889, src: 1, dst: 2, 1: -716482612, 0: 𐽎🢡²ᦈ𑇯b.'}, {time: -6013580519862736219, src: 1, dst: 5, 1: None, 0: None}, {time: -4896248842734844248, src: 2, dst: 1, 1: None, 0: None}, {time: -4018231749137773447, src: 5, dst: 0, 1: -472196955, 0: None}, {time: 1640632645145804617, src: 5, dst: 0, 1: None, 0: None}, {time: 5572086624347077302, src: 5, dst: 2, 1: None, 0: None}, {time: 6714278105438745902, src: 5, dst: 2, 1: -2131553255, 0: ୃ\v￼ѨὋ%&&ৗ**ෆ𑱥7ॣ𑃸ኮ`i𐖌𝍯}, {time: 1312144601878498786, src: 5, dst: 5, 1: 1381031985, 0: None}],
        //                 ),
        //             ),
        //         ],
        //         types: None,
        //         node_c_props: None,
        //         node_t_props: None,
        //         chunk_size: 1,
        //         props_chunk_size: 1,
        //     },
        let right = GraphTestInput {
            nodes: vec![0, 1, 2, 5],
            layers: vec![
                (
                    "0".to_string(),
                    make_edge_struct(
                        &[
                            (0, 1, -6946189257675522426),
                            (0, 1, -3077934556543664082),
                            (0, 1, -1003310608997642694),
                            (0, 2, -7426350489886598556),
                            (1, 0, -2795532415998143321),
                            (1, 1, -8030557141856176232),
                            (1, 1, 3890797442906057970),
                            (1, 5, -5531033197350346743),
                            (1, 5, 2274525496805076585),
                            (2, 0, -3614222918805423072),
                            (2, 2, -2964156677504024688),
                            (2, 5, -5146928027062721059),
                            (5, 2, 6907944027963841409),
                            (5, 2, 7647094936185192977),
                            (5, 5, 7699132147983612225),
                        ],
                        None,
                    ),
                ),
                (
                    "1".to_string(),
                    make_edge_struct(
                        &[
                            (0, 0, -5560381993853069877),
                            (0, 0, -5472276360197243206),
                            (0, 0, 7076886535070555787),
                            (0, 1, -6565749092322173179),
                            (0, 1, -3686318905505156857),
                            (0, 1, -2738585546365182324),
                            (0, 5, -4277784028792630760),
                            (0, 5, 6440245090124588543),
                            (1, 0, 7112990369392856699),
                            (1, 1, 3024513833601334897),
                            (1, 2, 4987313276447385600),
                            (1, 5, 6955697344957532988),
                            (2, 0, -5262901447702039929),
                            (2, 0, 8123919435694227826),
                            (2, 2, 8574203868684198909),
                            (5, 1, 3098838084725181777),
                            (5, 2, -52316127849277753),
                            (5, 5, -5741198006060425984),
                        ],
                        None,
                    ),
                ),
                (
                    "2".to_string(),
                    make_edge_struct(
                        &[
                            (0, 0, 1866712518627396198),
                            (0, 2, -7791749991472020317),
                            (0, 2, 3304203354872896569),
                            (0, 5, -6032436129150284651),
                            (0, 5, 8096767526243760246),
                            (1, 1, -8107222458096274499),
                            (1, 2, 3589138218193040889),
                            (1, 5, -6013580519862736219),
                            (2, 1, -4896248842734844248),
                            (5, 0, -4018231749137773447),
                            (5, 0, 1640632645145804617),
                            (5, 2, 5572086624347077302),
                            (5, 2, 6714278105438745902),
                            (5, 5, 1312144601878498786),
                        ],
                        None,
                    ),
                ),
            ],
            types: None,
            node_c_props: None,
            node_t_props: None,
            chunk_size: 4,
            props_chunk_size: 7,
        };
        //     merged: GraphTestInput {
        //         nodes: [
        //             0,
        //             1,
        //             2,
        //             3,
        //             5,
        //         ],
        //         layers: [
        //             (
        //                 "0",
        //                 Some(
        //                     StructArray[{time: -6946189257675522426, src: 0, dst: 1, 1: None, 2: p4𑖽PN$𝨜\*𐓏?{:𚿲'{Ῑ`E;'𝕆}, {time: -3077934556543664082, src: 0, dst: 1, 1: ૐ!t&ﺙ𞅅𒿢6?-5c, 2: Ⱥ)%్}, {time: -1003310608997642694, src: 0, dst: 1, 1: લp%ۻ{𞸀UqѨ*ￅ*\<z.&?𑧤¥𝋉$»꡴·𱛯, 2: None}, {time: -7426350489886598556, src: 0, dst: 2, 1: ¥z=𝕃, 2: 𝐶$ :ØI𑴈𐿮ਅ"🕴🉑}, {time: -2795532415998143321, src: 1, dst: 0, 1: None, 2:  \=ᜓ&ꟑ{.-K=VȺk=𑋱.¥ [?/Dহ}, {time: -8030557141856176232, src: 1, dst: 1, 1: , 2: <໕-o𑂰}, {time: 3890797442906057970, src: 1, dst: 1, 1: "6Ð9\<ਂ>:q`;%''"W&\Ü, 2: None}, {time: -5531033197350346743, src: 1, dst: 5, 1: 𐖻:ぴ|!$፩𑃵IN:ⶻ𑴉𖭟0𞣇⁞h, 2: 𑗖¥4 `}, {time: 2274525496805076585, src: 1, dst: 5, 1: $𐤰, 2: None}, {time: -3614222918805423072, src: 2, dst: 0, 1: None, 2: Ｚዼ𞸴ቚ4.𐲒¸🕴ꬑྌ;s7𛱉8'𖽥ѨዀﬓѨ$|&𐔐ὰ+}, {time: -2964156677504024688, src: 2, dst: 2, 1: UᜣῚѨ𞹙$"𐺱r<🪈𒓊�Z𐣵, 2: Ѩ&𑊑G🄇Ὕm}, {time: -5146928027062721059, src: 2, dst: 5, 1: `{=&𐨒{5"𞸧S𞹔4+"<!$SedCԱ¢Z$s%, 2: 𝕆ળ⑈ͽ}, {time: -8959471431607301714, src: 3, dst: 3, 1: None, 2: \￼N㈑𑤐FO/ೀO}, {time: -8370204226306237842, src: 3, dst: 3, 1: None, 2: None}, {time: -3905452029229723274, src: 3, dst: 3, 1: None, 2: None}, {time: -3004290179491894154, src: 3, dst: 3, 1: None, 2: ෞ?"/𬕨¥p&Y**Ѩd>𐮫🕴𑣔K}*q¥`<🪱'�`}, {time: 2627216774690630900, src: 3, dst: 3, 1: None, 2: None}, {time: 5471495323309507066, src: 3, dst: 3, 1: None, 2: Ⱥ:{z=}, {time: 7730937374962123815, src: 3, dst: 3, 1: None, 2: =SA"ê}, {time: 6907944027963841409, src: 5, dst: 2, 1: None, 2: 𫆩/𐇨🭼"{ࡋ𑍌¥K=ա𒾔﷏"V𚿱Ѩ'$ঊ𐕑𑆗.[*e}, {time: 7647094936185192977, src: 5, dst: 2, 1: -ৠ⛝<{𞹟Zᯫ*ਆ<RὒP🂲ৱ'𑧣"꯶ਸT, 2: J'>𐒘ন𐧉%*�=}𐫵<𐨿𐴸𞹤§<*𞥑/𖫵'Ѩ}, {time: 7699132147983612225, src: 5, dst: 5, 1: None, 2: ﷏𐄘|𖭐𑻰Ⱥૐ𝄌'}],
        //                 ),
        //             ),
        //             (
        //                 "1",
        //                 Some(
        //                     StructArray[{time: -5560381993853069877, src: 0, dst: 0, 2: 1996651016, 0: None, 1: 1590267470}, {time: -5472276360197243206, src: 0, dst: 0, 2: None, 0: None, 1: 1320544567}, {time: 7076886535070555787, src: 0, dst: 0, 2: 1149145534, 0: None, 1: None}, {time: -6565749092322173179, src: 0, dst: 1, 2: None, 0: None, 1: -850427537}, {time: -3686318905505156857, src: 0, dst: 1, 2: None, 0: 1751653540, 1: 1822693927}, {time: -2738585546365182324, src: 0, dst: 1, 2: 1506791915, 0: 508637071, 1: -448967720}, {time: -4277784028792630760, src: 0, dst: 5, 2: -1576928800, 0: -1058902216, 1: -543037163}, {time: 6440245090124588543, src: 0, dst: 5, 2: None, 0: None, 1: -461676893}, {time: 7112990369392856699, src: 1, dst: 0, 2: 663248545, 0: None, 1: None}, {time: 3024513833601334897, src: 1, dst: 1, 2: None, 0: None, 1: None}, {time: 4987313276447385600, src: 1, dst: 2, 2: None, 0: None, 1: -868276681}, {time: 6955697344957532988, src: 1, dst: 5, 2: None, 0: 1294894725, 1: None}, {time: -5262901447702039929, src: 2, dst: 0, 2: None, 0: -601789892, 1: None}, {time: 8123919435694227826, src: 2, dst: 0, 2: None, 0: None, 1: None}, {time: 8574203868684198909, src: 2, dst: 2, 2: None, 0: 1077520285, 1: -171380916}, {time: -8069755937079635973, src: 3, dst: 3, 2: 540297124, 0: None, 1: None}, {time: -2108721513160478139, src: 3, dst: 3, 2: -938218464, 0: None, 1: None}, {time: -350798471955118581, src: 3, dst: 3, 2: None, 0: None, 1: None}, {time: 8474707726855679655, src: 3, dst: 3, 2: None, 0: None, 1: None}, {time: 3098838084725181777, src: 5, dst: 1, 2: 1268103956, 0: None, 1: -1734658364}, {time: -52316127849277753, src: 5, dst: 2, 2: -1277558555, 0: None, 1: None}, {time: -5741198006060425984, src: 5, dst: 5, 2: None, 0: None, 1: 1353362200}],
        //                 ),
        //             ),
        //             (
        //                 "2",
        //                 Some(
        //                     StructArray[{time: 1866712518627396198, src: 0, dst: 0, 1: None, 0: None}, {time: -7791749991472020317, src: 0, dst: 2, 1: -1985106975, 0: 𐤿*￼dꪕꢾ🉣)ⷞૠ𞺢¥-𒅩🕴🕴#𑵖/ኴ`:𑌽}, {time: 3304203354872896569, src: 0, dst: 2, 1: -2096284107, 0: q\^𐁝}, {time: -6032436129150284651, src: 0, dst: 5, 1: None, 0: None}, {time: 8096767526243760246, src: 0, dst: 5, 1: -1765142288, 0: ୂ🠂";EವD*𝒦áO*ਜ਼úᰍa%a{"🫕=}, {time: -8107222458096274499, src: 1, dst: 1, 1: None, 0: ﷏𐤢/a𐤿IÊ\ரy𖼒ZZⴭ%Ὓ%[🟨$G𛲟/𥳐ఐ🈶Ä&}, {time: 3589138218193040889, src: 1, dst: 2, 1: -716482612, 0: 𐽎🢡²ᦈ𑇯b.'}, {time: -6013580519862736219, src: 1, dst: 5, 1: None, 0: None}, {time: -4896248842734844248, src: 2, dst: 1, 1: None, 0: None}, {time: -8300850366113978842, src: 3, dst: 3, 1: 1174831362, 0: None}, {time: -6214897813967589599, src: 3, dst: 3, 1: None, 0: None}, {time: -3104668713472349576, src: 3, dst: 3, 1: 481418824, 0: None}, {time: 4552452772834849983, src: 3, dst: 3, 1: 2033193117, 0: None}, {time: 5737410735888382198, src: 3, dst: 3, 1: None, 0: None}, {time: 7045881871089359872, src: 3, dst: 3, 1: None, 0: None}, {time: -4018231749137773447, src: 5, dst: 0, 1: -472196955, 0: None}, {time: 1640632645145804617, src: 5, dst: 0, 1: None, 0: None}, {time: 5572086624347077302, src: 5, dst: 2, 1: None, 0: None}, {time: 6714278105438745902, src: 5, dst: 2, 1: -2131553255, 0: ୃ\v￼ѨὋ%&&ৗ**ෆ𑱥7ॣ𑃸ኮ`i𐖌𝍯}, {time: 1312144601878498786, src: 5, dst: 5, 1: 1381031985, 0: None}],
        //                 ),
        //             ),
        //         ],
        //         types: None,
        //         node_c_props: None,
        //         node_t_props: None,
        //         chunk_size: 4,
        //         props_chunk_size: 7,
        //     },
        // }
        let merged = GraphTestInput {
            nodes: vec![0, 1, 2, 3, 5],
            layers: vec![
                (
                    "0".to_string(),
                    make_edge_struct(
                        &[
                            (0, 1, -6946189257675522426),
                            (0, 1, -3077934556543664082),
                            (0, 1, -1003310608997642694),
                            (0, 2, -7426350489886598556),
                            (1, 0, -2795532415998143321),
                            (1, 1, -8030557141856176232),
                            (1, 1, 3890797442906057970),
                            (1, 5, -5531033197350346743),
                            (1, 5, 2274525496805076585),
                            (2, 0, -3614222918805423072),
                            (2, 2, -2964156677504024688),
                            (2, 5, -5146928027062721059),
                            (3, 3, -8959471431607301714),
                            (3, 3, -8370204226306237842),
                            (3, 3, -3905452029229723274),
                            (3, 3, -3004290179491894154),
                            (3, 3, 2627216774690630900),
                            (3, 3, 5471495323309507066),
                            (3, 3, 7730937374962123815),
                            (5, 2, 6907944027963841409),
                            (5, 2, 7647094936185192977),
                            (5, 5, 7699132147983612225),
                        ],
                        None,
                    ),
                ),
                (
                    "1".to_string(),
                    make_edge_struct(
                        &[
                            (0, 0, -5560381993853069877),
                            (0, 0, -5472276360197243206),
                            (0, 0, 7076886535070555787),
                            (0, 1, -6565749092322173179),
                            (0, 1, -3686318905505156857),
                            (0, 1, -2738585546365182324),
                            (0, 5, -4277784028792630760),
                            (0, 5, 6440245090124588543),
                            (1, 0, 7112990369392856699),
                            (1, 1, 3024513833601334897),
                            (1, 2, 4987313276447385600),
                            (1, 5, 6955697344957532988),
                            (2, 0, -5262901447702039929),
                            (2, 0, 8123919435694227826),
                            (2, 2, 8574203868684198909),
                            (3, 3, -8069755937079635973),
                            (3, 3, -2108721513160478139),
                            (3, 3, -350798471955118581),
                            (3, 3, 8474707726855679655),
                            (5, 1, 3098838084725181777),
                            (5, 2, -52316127849277753),
                            (5, 5, -5741198006060425984),
                        ],
                        None,
                    ),
                ),
                (
                    "2".to_string(),
                    make_edge_struct(
                        &[
                            (0, 0, 1866712518627396198),
                            (0, 2, -7791749991472020317),
                            (0, 2, 3304203354872896569),
                            (0, 5, -6032436129150284651),
                            (0, 5, 8096767526243760246),
                            (1, 1, -8107222458096274499),
                            (1, 2, 3589138218193040889),
                            (1, 5, -6013580519862736219),
                            (2, 1, -4896248842734844248),
                            (3, 3, -8300850366113978842),
                            (3, 3, -6214897813967589599),
                            (3, 3, -3104668713472349576),
                            (3, 3, 4552452772834849983),
                            (3, 3, 5737410735888382198),
                            (3, 3, 7045881871089359872),
                            (5, 0, -4018231749137773447),
                            (5, 0, 1640632645145804617),
                            (5, 2, 5572086624347077302),
                            (5, 2, 6714278105438745902),
                            (5, 5, 1312144601878498786),
                        ],
                        None,
                    ),
                ),
            ],
            types: None,
            node_c_props: None,
            node_t_props: None,
            chunk_size: 4,
            props_chunk_size: 7,
        };
        merge_test_inner_multilayer(left, right, merged)
        // Mismatched [0] adj_in_edges, left: [(0, 4), (0, 8), (1, 1), (1, 5), (2, 2), (2, 10), (2, 15), (3, 12), (4, 7), (4, 11), (4, 16)], right: [(0, 4), (0, 8), (1, 1), (1, 5), (2, 1), (2, 8), (2, 15), (3, 12), (4, 4), (4, 8), (4, 15)]
    }

    #[test]
    fn test_edge_properties_fail4() {
        // input = MergeTestInput {
        //     left: GraphTestInput {
        //         nodes: [
        //             0,
        //             4,
        //             5,
        //             6,
        //         ],
        //         layers: [
        //             (
        //                 "0",
        //                 Some(
        //                     StructArray[{time: 1, src: 0, dst: 0}, {time: 2, src: 0, dst: 5}, {time: 3, src: 5, dst: 0}, {time: 4, src: 6, dst: 4}, {time: 5, src: 6, dst: 5}, {time: 6, src: 6, dst: 6}],
        //                 ),
        //             ),
        //             (
        //                 "1",
        //                 Some(
        //                     StructArray[{time: 5282396441336232361, src: 0, dst: 0, 0: None, 1: None}, {time: 6655296641769187593, src: 0, dst: 0, 0: None, 1: None}, {time: 7855906953022476738, src: 0, dst: 4, 0: None, 1: None}, {time: -6954031954690592691, src: 0, dst: 5, 0: None, 1: None}, {time: -6452838878608528674, src: 0, dst: 5, 0: None, 1: -802850817}, {time: -7131172968040116202, src: 4, dst: 6, 0: None, 1: 1414522402}, {time: 4389671027473617996, src: 4, dst: 6, 0: None, 1: 1968647417}, {time: -7260474321831791552, src: 5, dst: 0, 0: None, 1: None}, {time: -2119010012653050546, src: 5, dst: 0, 0: None, 1: None}, {time: -2152007196428012803, src: 5, dst: 4, 0: 1, 1: -1593717439}, {time: 7025655663010421778, src: 5, dst: 6, 0: None, 1: 595229066}, {time: 366551276269517964, src: 6, dst: 4, 0: -188315765, 1: 250810268}, {time: -7486609339504198132, src: 6, dst: 5, 0: None, 1: -1798097129}, {time: -1873685932869155936, src: 6, dst: 6, 0: None, 1: 1167039681}],
        //                 ),
        //             ),
        //         ],
        //         types: None,
        //         node_c_props: None,
        //         node_t_props: None,
        //         chunk_size: 4,
        //         props_chunk_size: 7,
        //     },
        let left = GraphTestInput {
            nodes: vec![0, 4, 5, 6],
            layers: vec![
                (
                    "0".to_string(),
                    make_edge_struct(
                        &[
                            (0, 0, 1),
                            (0, 5, 2),
                            (5, 0, 3),
                            (6, 4, 4),
                            (6, 5, 5),
                            (6, 6, 6),
                        ],
                        None,
                    ),
                ),
                (
                    "1".to_string(),
                    make_edge_struct(
                        &[
                            (0, 0, 5282396441336232361),
                            (0, 0, 6655296641769187593),
                            (0, 4, 7855906953022476738),
                            (0, 5, -6954031954690592691),
                            (0, 5, -6452838878608528674),
                            (4, 6, -7260474321831791552),
                            (4, 6, 4389671027473617996),
                            (5, 0, -7260474321831791552),
                            (5, 0, -2119010012653050546),
                            (5, 4, -2152007196428012803),
                            (5, 6, 7025655663010421778),
                            (6, 4, 366551276269517964),
                            (6, 5, -7486609339504198132),
                            (6, 6, -1873685932869155936),
                        ],
                        None,
                    ),
                ),
            ],
            types: None,
            node_c_props: None,
            node_t_props: None,
            chunk_size: 40,
            props_chunk_size: 70,
        };
        //     right: GraphTestInput {
        //         nodes: [
        //             4,
        //         ],
        //         layers: [],
        //         types: None,
        //         node_c_props: None,
        //         node_t_props: None,
        //         chunk_size: 3,
        //         props_chunk_size: 5,
        //     },
        let right = GraphTestInput {
            nodes: vec![4],
            layers: vec![],
            types: None,
            node_c_props: None,
            node_t_props: None,
            chunk_size: 3,
            props_chunk_size: 50,
        };
        //     merged: GraphTestInput {
        //         nodes: [
        //             0,
        //             4,
        //             5,
        //             6,
        //         ],
        //         layers: [
        //             (
        //                 "0",
        //                 Some(
        //                     StructArray[{time: 1, src: 0, dst: 0}, {time: 2, src: 0, dst: 5}, {time: 3, src: 5, dst: 0}, {time: 4, src: 6, dst: 4}, {time: 5, src: 6, dst: 5}, {time: 6, src: 6, dst: 6}],
        //                 ),
        //             ),
        //             (
        //                 "1",
        //                 Some(
        //                     StructArray[{time: 5282396441336232361, src: 0, dst: 0, 0: None, 1: None}, {time: 6655296641769187593, src: 0, dst: 0, 0: None, 1: None}, {time: 7855906953022476738, src: 0, dst: 4, 0: None, 1: None}, {time: -6954031954690592691, src: 0, dst: 5, 0: None, 1: None}, {time: -6452838878608528674, src: 0, dst: 5, 0: None, 1: -802850817}, {time: -7131172968040116202, src: 4, dst: 6, 0: None, 1: 1414522402}, {time: 4389671027473617996, src: 4, dst: 6, 0: None, 1: 1968647417}, {time: -7260474321831791552, src: 5, dst: 0, 0: None, 1: None}, {time: -2119010012653050546, src: 5, dst: 0, 0: None, 1: None}, {time: -2152007196428012803, src: 5, dst: 4, 0: 1, 1: -1593717439}, {time: 7025655663010421778, src: 5, dst: 6, 0: None, 1: 595229066}, {time: 366551276269517964, src: 6, dst: 4, 0: -188315765, 1: 250810268}, {time: -7486609339504198132, src: 6, dst: 5, 0: None, 1: -1798097129}, {time: -1873685932869155936, src: 6, dst: 6, 0: None, 1: 1167039681}],
        //                 ),
        //             ),
        //         ],
        //         types: None,
        //         node_c_props: None,
        //         node_t_props: None,
        //         chunk_size: 4,
        //         props_chunk_size: 7,
        //     },
        // }

        let merged = GraphTestInput {
            nodes: vec![0, 4, 5, 6],
            layers: vec![
                (
                    "0".to_string(),
                    make_edge_struct(
                        &[
                            (0, 0, 1),
                            (0, 5, 2),
                            (5, 0, 3),
                            (6, 4, 4),
                            (6, 5, 5),
                            (6, 6, 6),
                        ],
                        None,
                    ),
                ),
                (
                    "1".to_string(),
                    make_edge_struct(
                        &[
                            (0, 0, 5282396441336232361),
                            (0, 0, 6655296641769187593),
                            (0, 4, 7855906953022476738),
                            (0, 5, -6954031954690592691),
                            (0, 5, -6452838878608528674),
                            (4, 6, -7260474321831791552),
                            (4, 6, 4389671027473617996),
                            (5, 0, -7260474321831791552),
                            (5, 0, -2119010012653050546),
                            (5, 4, -2152007196428012803),
                            (5, 6, 7025655663010421778),
                            (6, 4, 366551276269517964),
                            (6, 5, -7486609339504198132),
                            (6, 6, -1873685932869155936),
                        ],
                        None,
                    ),
                ),
            ],
            types: None,
            node_c_props: None,
            node_t_props: None,
            chunk_size: 40,
            props_chunk_size: 70,
        };
        merge_test_inner_multilayer(left, right, merged)
    }

    #[test]
    fn test_edge_properties_fail5() {
        // minimal failing input: input = MergeTestInput {
        // left: GraphTestInput {
        //     nodes: [],
        //     layers: [
        //         (
        //             "1",
        //             None,
        //         ),
        //         (
        //             "2",
        //             None,
        //         ),
        //     ],
        //     types: None,
        //     node_c_props: None,
        //     node_t_props: None,
        //     chunk_size: 2,
        //     props_chunk_size: 4,
        // },
        let left = GraphTestInput {
            nodes: vec![],
            layers: vec![
                ("1".to_string(), make_edge_struct(&[], None)),
                ("2".to_string(), make_edge_struct(&[], None)),
            ],
            types: None,
            node_c_props: None,
            node_t_props: None,
            chunk_size: 2,
            props_chunk_size: 4,
        };
        // right: GraphTestInput {
        //     nodes: [
        //         0,
        //         3,
        //         4,
        //     ],
        //     layers: [
        //         (
        //             "0",
        //             Some(
        //                 StructArray[{time: 5276857042054047653, src: 0, dst: 0, 0: None, 2: 1502377552}, {time: 6746655051447407140, src: 0, dst: 3, 0: -1149749357, 2: -2031680015}, {time: -3084500666133239234, src: 3, dst: 0, 0: 1123500190, 2: None}, {time: 2207898770080961903, src: 3, dst: 0, 0: -1064909909, 2: None}, {time: -2647087945744226332, src: 3, dst: 4, 0: -474752406, 2: 39112627}, {time: 675772143639015102, src: 3, dst: 4, 0: -2104098453, 2: 1925854359}, {time: 3427408144546218084, src: 3, dst: 4, 0: None, 2: 1381946265}, {time: 3867413416033385300, src: 3, dst: 4, 0: None, 2: None}, {time: 8606453537612303068, src: 3, dst: 4, 0: -587715364, 2: None}, {time: 939584108604248482, src: 4, dst: 3, 0: 2110117309, 2: None}],
        //             ),
        //         ),
        //         (
        //             "1",
        //             None,
        //         ),
        //         (
        //             "2",
        //             Some(
        //                 StructArray[{time: -6065037085450787535, src: 0, dst: 0, 2: None}, {time: 6130462778622110628, src: 0, dst: 0, 2: None}, {time: 7363388346673274272, src: 0, dst: 0, 2: None}, {time: 7851485112302650920, src: 0, dst: 0, 2: None}, {time: 7438244091190352289, src: 0, dst: 3, 2: None}, {time: 55467325619664232, src: 0, dst: 4, 2: None}, {time: 3789218423269015554, src: 0, dst: 4, 2: None}, {time: 97934560264029728, src: 3, dst: 0, 2: 22647}, {time: 5079343266069839204, src: 3, dst: 0, 2: 1987419139}, {time: -8210014843484380174, src: 3, dst: 4, 2: -1906835082}, {time: 8365365399655079823, src: 3, dst: 4, 2: -869854102}, {time: 500061971858349389, src: 4, dst: 0, 2: None}, {time: 1262756163063656831, src: 4, dst: 0, 2: 2050980769}, {time: 3581220004728928060, src: 4, dst: 0, 2: None}, {time: -2700293643120834412, src: 4, dst: 3, 2: -1301619986}, {time: 4723981058517446179, src: 4, dst: 3, 2: None}, {time: 2410646386766231897, src: 4, dst: 4, 2: -1691513890}],
        //             ),
        //         ),
        //     ],
        //     types: None,
        //     node_c_props: None,
        //     node_t_props: None,
        //     chunk_size: 7,
        //     props_chunk_size: 8,
        // },
        let right = GraphTestInput {
            nodes: vec![0, 3, 4],
            layers: vec![
                (
                    "0".to_string(),
                    make_edge_struct(
                        &[
                            (0, 0, 5276857042054047653),
                            (0, 3, 6746655051447407140),
                            (3, 0, -3084500666133239234),
                            (3, 0, 2207898770080961903),
                            (3, 4, -2647087945744226332),
                            (3, 4, 675772143639015102),
                            (3, 4, 3427408144546218084),
                            (3, 4, 3867413416033385300),
                            (3, 4, 8606453537612303068),
                            (4, 3, 939584108604248482),
                        ],
                        None,
                    ),
                ),
                ("1".to_string(), make_edge_struct(&[], None)),
                (
                    "2".to_string(),
                    make_edge_struct(
                        &[
                            (0, 0, -6065037085450787535),
                            (0, 0, 6130462778622110628),
                            (0, 0, 7363388346673274272),
                            (0, 0, 7851485112302650920),
                            (0, 3, 7438244091190352289),
                            (0, 4, 55467325619664232),
                            (0, 4, 3789218423269015554),
                            (3, 0, 97934560264029728),
                            (3, 0, 5079343266069839204),
                            (3, 4, -8210014843484380174),
                            (3, 4, 8365365399655079823),
                            (4, 0, 500061971858349389),
                            (4, 0, 1262756163063656831),
                            (4, 0, 3581220004728928060),
                            (4, 3, -2700293643120834412),
                            (4, 3, 4723981058517446179),
                            (4, 4, 2410646386766231897),
                        ],
                        None,
                    ),
                ),
            ],
            types: None,
            node_c_props: None,
            node_t_props: None,
            chunk_size: 7,
            props_chunk_size: 8,
        };
        // merged: GraphTestInput {
        //     nodes: [
        //         0,
        //         3,
        //         4,
        //     ],
        //     layers: [
        //         (
        //             "1",
        //             None,
        //         ),
        //         (
        //             "2",
        //             Some(
        //                 StructArray[{time: -6065037085450787535, src: 0, dst: 0, 2: None}, {time: 6130462778622110628, src: 0, dst: 0, 2: None}, {time: 7363388346673274272, src: 0, dst: 0, 2: None}, {time: 7851485112302650920, src: 0, dst: 0, 2: None}, {time: 7438244091190352289, src: 0, dst: 3, 2: None}, {time: 55467325619664232, src: 0, dst: 4, 2: None}, {time: 3789218423269015554, src: 0, dst: 4, 2: None}, {time: 97934560264029728, src: 3, dst: 0, 2: 22647}, {time: 5079343266069839204, src: 3, dst: 0, 2: 1987419139}, {time: -8210014843484380174, src: 3, dst: 4, 2: -1906835082}, {time: 8365365399655079823, src: 3, dst: 4, 2: -869854102}, {time: 500061971858349389, src: 4, dst: 0, 2: None}, {time: 1262756163063656831, src: 4, dst: 0, 2: 2050980769}, {time: 3581220004728928060, src: 4, dst: 0, 2: None}, {time: -2700293643120834412, src: 4, dst: 3, 2: -1301619986}, {time: 4723981058517446179, src: 4, dst: 3, 2: None}, {time: 2410646386766231897, src: 4, dst: 4, 2: -1691513890}],
        //             ),
        //         ),
        //         (
        //             "0",
        //             Some(
        //                 StructArray[{time: 5276857042054047653, src: 0, dst: 0, 0: None, 2: 1502377552}, {time: 6746655051447407140, src: 0, dst: 3, 0: -1149749357, 2: -2031680015}, {time: -3084500666133239234, src: 3, dst: 0, 0: 1123500190, 2: None}, {time: 2207898770080961903, src: 3, dst: 0, 0: -1064909909, 2: None}, {time: -2647087945744226332, src: 3, dst: 4, 0: -474752406, 2: 39112627}, {time: 675772143639015102, src: 3, dst: 4, 0: -2104098453, 2: 1925854359}, {time: 3427408144546218084, src: 3, dst: 4, 0: None, 2: 1381946265}, {time: 3867413416033385300, src: 3, dst: 4, 0: None, 2: None}, {time: 8606453537612303068, src: 3, dst: 4, 0: -587715364, 2: None}, {time: 939584108604248482, src: 4, dst: 3, 0: 2110117309, 2: None}],
        //             ),
        //         ),
        //     ],
        //     types: None,
        //     node_c_props: None,
        //     node_t_props: None,
        //     chunk_size: 7,
        //     props_chunk_size: 8,
        // },
        let merged = GraphTestInput {
            nodes: vec![0, 3, 4],
            layers: vec![
                ("1".to_string(), make_edge_struct(&[], None)),
                (
                    "2".to_string(),
                    make_edge_struct(
                        &[
                            (0, 0, -6065037085450787535),
                            (0, 0, 6130462778622110628),
                            (0, 0, 7363388346673274272),
                            (0, 0, 7851485112302650920),
                            (0, 3, 7438244091190352289),
                            (0, 4, 55467325619664232),
                            (0, 4, 3789218423269015554),
                            (3, 0, 97934560264029728),
                            (3, 0, 5079343266069839204),
                            (3, 4, -8210014843484380174),
                            (3, 4, 8365365399655079823),
                            (4, 0, 500061971858349389),
                            (4, 0, 1262756163063656831),
                            (4, 0, 3581220004728928060),
                            (4, 3, -2700293643120834412),
                            (4, 3, 4723981058517446179),
                            (4, 4, 2410646386766231897),
                        ],
                        None,
                    ),
                ),
                (
                    "0".to_string(),
                    make_edge_struct(
                        &[
                            (0, 0, 5276857042054047653),
                            (0, 3, 6746655051447407140),
                            (3, 0, -3084500666133239234),
                            (3, 0, 2207898770080961903),
                            (3, 4, -2647087945744226332),
                            (3, 4, 675772143639015102),
                            (3, 4, 3427408144546218084),
                            (3, 4, 3867413416033385300),
                            (3, 4, 8606453537612303068),
                            (4, 3, 939584108604248482),
                        ],
                        None,
                    ),
                ),
            ],
            types: None,
            node_c_props: None,
            node_t_props: None,
            chunk_size: 7,
            props_chunk_size: 8,
        };
        merge_test_inner_multilayer(left, right, merged)
    }

    #[test]
    fn test_node_properties() {
        proptest!(|(input in gen_merge_test_input(100, 0, 0, 0, 3, 3, 100, 0, 1usize..10, 1usize..10, 1usize..10, 1usize..10), num_threads in 1usize..10)| {
            let pool = rayon::ThreadPoolBuilder::new().num_threads(num_threads).build().unwrap();
            pool.install(|| {
                merge_test_inner_multilayer(input.left, input.right, input.merged)
            })
        })
    }

    #[test]
    fn test_node_properties_1() {
        let left = GraphTestInput {
            nodes: vec![2, 9, 24],
            layers: vec![],
            types: None,
            node_c_props: make_props_struct(
                &["1".to_string()],
                vec![Some(
                    PrimitiveArray::<u64>::from(vec![None, None, None]).boxed(),
                )],
            ),
            node_t_props: make_props_struct(
                &["node".to_string(), "time".to_string(), "2".to_string()],
                vec![
                    Some(PrimitiveArray::from_vec(vec![2u64, 9, 24]).boxed()),
                    Some(PrimitiveArray::from_vec(vec![-8i64, -3, -1]).boxed()),
                    Some(Utf8Array::<i32>::from([Some("bla".to_string()), None, None]).boxed()),
                ],
            ),
            chunk_size: 8,
            props_chunk_size: 6,
        };

        let right = GraphTestInput {
            nodes: vec![1, 7, 11],
            layers: vec![],
            types: None,
            node_c_props: make_props_struct(
                &["1".to_string(), "0".to_string()],
                vec![
                    Some(PrimitiveArray::<u64>::from(vec![None, None, None]).boxed()),
                    Some(
                        PrimitiveArray::<i32>::from(vec![None, Some(63), Some(-1348677201)])
                            .boxed(),
                    ),
                ],
            ),
            node_t_props: make_props_struct(
                &["node".to_string(), "time".to_string(), "0".to_string()],
                vec![
                    Some(PrimitiveArray::from_vec(vec![1u64, 11]).boxed()),
                    Some(PrimitiveArray::from_vec(vec![-8i64, -3]).boxed()),
                    Some(Utf8Array::<i32>::from([Some("bla".to_string()), None]).boxed()),
                ],
            ),
            chunk_size: 9,
            props_chunk_size: 1,
        };

        let merged = GraphTestInput {
            nodes: vec![1, 2, 7, 9, 11, 24],
            layers: vec![],
            types: None,
            node_c_props: make_props_struct(
                &["1".to_string(), "0".to_string()],
                vec![
                    Some(PrimitiveArray::<u64>::from(vec![None, None, None]).boxed()),
                    Some(
                        PrimitiveArray::<i32>::from(vec![None, Some(63), Some(-1348677201)])
                            .boxed(),
                    ),
                ],
            ),
            node_t_props: make_props_struct(
                &[
                    "node".to_string(),
                    "time".to_string(),
                    "2".to_string(),
                    "0".to_string(),
                ],
                vec![
                    Some(PrimitiveArray::from_vec(vec![1u64, 11, 2, 24]).boxed()),
                    Some(PrimitiveArray::from_vec(vec![-8i64, -3, -1, -3]).boxed()),
                    Some(
                        Utf8Array::<i32>::from([Some("bla".to_string()), None, None, None]).boxed(),
                    ),
                ],
            ),
            chunk_size: 9,
            props_chunk_size: 6,
        };

        merge_test_inner_multilayer(left, right, merged)
    }

    #[test]
    fn node_properties_too_many_values() {
        let left_props = make_props_struct(
            &["1".to_string()],
            vec![Some(PrimitiveArray::from_vec(vec![0u64, 2]).boxed())],
        );
        let merged_props = make_props_struct(
            &["1".to_string()],
            vec![Some(
                PrimitiveArray::from(vec![Some(0u64), None, Some(2)]).boxed(),
            )],
        );
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap();
        pool.install(|| {
            merge_test_inner_multilayer(
                GraphTestInput::new(vec![0, 2], vec![], None, left_props, None, 2, 1),
                GraphTestInput::new(vec![1], vec![], None, None, None, 1, 1),
                GraphTestInput::new(vec![0, 1, 2], vec![], None, merged_props, None, 3, 2),
            )
        })
    }

    #[test]
    fn node_properties_index_error() {
        let props = make_props_struct(
            &["1".to_string()],
            vec![Some(PrimitiveArray::from_vec(vec![1u64]).boxed())],
        );
        let props_merged = make_props_struct(
            &["1".to_string()],
            vec![Some(PrimitiveArray::from_vec(vec![1u64, 1]).boxed())],
        );
        merge_test_inner_multilayer(
            GraphTestInput::new(vec![0], vec![], None, props.clone(), None, 1, 1),
            GraphTestInput::new(vec![1], vec![], None, props, None, 1, 1),
            GraphTestInput::new(vec![0, 1], vec![], None, props_merged, None, 2, 2),
        )
    }

    #[test]
    fn test_node_types() {
        proptest!(|(input in gen_merge_test_input(100, 0, 0,10, 0, 0, 0, 0,  1usize..10, 1usize..10, 1usize..10, 1usize..10), num_threads in 1usize..10)| {
            let pool = rayon::ThreadPoolBuilder::new().num_threads(num_threads).build().unwrap();
            pool.install(|| {
                merge_test_inner_multilayer(input.left, input.right, input.merged)
            })
        })
    }

    #[test]
    fn multilayer_with_empty() {
        merge_test_inner_multilayer(
            GraphTestInput::new(
                vec![],
                vec![("0".to_string(), build_simple_edges(&[]))],
                None,
                None,
                None,
                1,
                1,
            ),
            GraphTestInput::new(
                vec![0],
                vec![("1".to_string(), build_simple_edges(&[(0, 0, 0)]))],
                None,
                None,
                None,
                1,
                1,
            ),
            GraphTestInput::new(
                vec![0],
                vec![
                    ("0".to_string(), build_simple_edges(&[])),
                    ("1".to_string(), build_simple_edges(&[(0, 0, 0)])),
                ],
                None,
                None,
                None,
                1,
                1,
            ),
        )
    }

    #[test]
    fn test_types_both_sides() {
        merge_test_inner_multilayer(
            GraphTestInput::new(
                vec![0],
                vec![],
                Some(Utf8Array::<i32>::from([Some("0")]).boxed()),
                None,
                None,
                1,
                1,
            ),
            GraphTestInput::new(
                vec![1],
                vec![],
                Some(Utf8Array::<i32>::from([Some("1")]).boxed()),
                None,
                None,
                1,
                1,
            ),
            GraphTestInput::new(
                vec![0, 1],
                vec![],
                Some(Utf8Array::<i32>::from([Some("0"), Some("1")]).boxed()),
                None,
                None,
                1,
                1,
            ),
        )
    }

    #[test]
    fn test_types_one_sided() {
        merge_test_inner_multilayer(
            GraphTestInput::new(vec![0], vec![], None, None, None, 1, 1),
            GraphTestInput::new(
                vec![1, 2, 3],
                vec![],
                Some(Utf8Array::<i32>::from([Some("1"), Some("2"), Some("3")]).boxed()),
                None,
                None,
                1,
                1,
            ),
            GraphTestInput::new(
                vec![0, 1, 2, 3],
                vec![],
                Some(Utf8Array::<i32>::from([None, Some("1"), Some("2"), Some("3")]).boxed()),
                None,
                None,
                1,
                1,
            ),
        )
    }

    #[test]
    fn test_one_empty() {
        merge_test_inner_multilayer(
            GraphTestInput::new(vec![], vec![], None, None, None, 1, 1),
            GraphTestInput::new(
                vec![0, 1],
                vec![("0".to_string(), build_simple_edges(&[(1, 1, 0)]))],
                None,
                None,
                None,
                1,
                1,
            ),
            GraphTestInput::new(
                vec![0, 1],
                vec![("0".to_string(), build_simple_edges(&[(1, 1, 0)]))],
                None,
                None,
                None,
                1,
                1,
            ),
        )
    }

    #[test]
    fn test_all_empty() {
        merge_test_inner_multilayer(
            GraphTestInput::new(vec![], vec![], None, None, None, 1, 1),
            GraphTestInput::new(vec![], vec![], None, None, None, 1, 1),
            GraphTestInput::new(vec![], vec![], None, None, None, 1, 1),
        )
    }

    #[test]
    fn merge_graphs_check_adds() {
        let nodes = [1, 2, 3, 4];
        let edges = [
            (1, 2, 0),
            (1, 3, 1),
            (1, 3, 2),
            (2, 3, 3),
            (2, 4, 4),
            (2, 4, 5),
        ];
        let g1 = make_graph(&edges, &nodes, 1, 1).0;

        let nodes = [3, 4, 5, 6];
        let edges = [
            (3, 4, 6),
            (3, 5, 7),
            (3, 5, 8),
            (4, 5, 9),
            (4, 6, 10),
            (4, 6, 11),
        ];

        let g2 = make_graph(&edges, &nodes, 1, 1).0;

        let graph_dir = TempDir::new().unwrap();
        let g = merge_graphs(graph_dir, &g1, &g2).expect("Failed to merge graphs");

        let n1 = GidRef::U64(1);
        let vid = g.find_node(n1).expect("Failed to find node");
        let node = g.layer(0).nodes_storage().node(vid);
        let ts = node.timestamps().into_iter_t().collect::<Vec<_>>();
        assert_eq!(ts, vec![0, 1, 2]);

        let n2 = GidRef::U64(2);
        let vid = g.find_node(n2).expect("Failed to find node");
        let node = g.layer(0).nodes_storage().node(vid);
        let ts = node.timestamps().into_iter_t().collect::<Vec<_>>();
        assert_eq!(ts, vec![0, 3, 4, 5]);

        let n3 = GidRef::U64(3);
        let vid = g.find_node(n3).expect("Failed to find node");
        let node = g.layer(0).nodes_storage().node(vid);
        let ts = node.timestamps().into_iter_t().collect::<Vec<_>>();
        assert_eq!(ts, vec![1, 2, 3, 6, 7, 8]);

        let n4 = GidRef::U64(4);
        let vid = g.find_node(n4).expect("Failed to find node");
        let node = g.layer(0).nodes_storage().node(vid);
        let ts = node.timestamps().into_iter_t().collect::<Vec<_>>();
        assert_eq!(ts, vec![4, 5, 6, 9, 10, 11]);
    }
}
