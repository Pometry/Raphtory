use std::{
    borrow::Borrow,
    fmt::Debug,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{
    chunked_array::chunked_array::ChunkedArray,
    edges::EdgeTemporalProps,
    global_order::GlobalOrder,
    graph_builder::node_addition_builder::make_node_additions,
    load::{
        ipc::read_batch,
        mmap::mmap_batch,
        parquet_reader::ParquetOffsetIter,
        parquet_source::{resolve_and_dedup_chunk, ParquetSource},
    },
    nodes::{Node, Nodes},
    split_struct_chunk, GraphChunk, Time,
};

use crate::{
    arrow2::{
        array::{Array, PrimitiveArray, StructArray},
        datatypes::{ArrowDataType as DataType, Field},
        offset::OffsetsBuffer,
        record_batch::RecordBatchT,
    },
    chunked_array::{
        chunked_array::NonNull,
        chunked_offsets::ChunkedOffsets,
        list_array::ChunkedListArray,
        mutable_chunked_array::{
            MutChunkedOffsets, MutChunkedStructArray, MutPrimitiveChunkedArray,
        },
    },
    file_prefix::GraphPaths,
    graph_builder::{props_builder::PropsBuilder, EFBResult, EdgeFrameBuilder},
    interop::GraphLike,
    load::{mmap::mmap_buffer, parquet_reader::ParquetReader},
    nodes::{Adj, AdjTotal},
    prelude::{ArrayOps, BaseArrayOps, Chunked},
    prepare_graph_dir, RAError,
};
use itertools::Itertools;
use raphtory_api::{
    compute::par_cum_sum,
    core::{
        entities::{EID, VID},
        storage::timeindex::AsTime,
        Direction,
    },
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::instrument;

#[derive(Debug, Clone)]
pub struct TempColGraphFragment {
    pub(crate) nodes: Nodes,
    pub(crate) edges: EdgeTemporalProps,
    pub(crate) graph_dir: Arc<Path>,
    pub(crate) metadata: Metadata,
}

impl TempColGraphFragment {
    pub fn new_empty(path: impl Into<Arc<Path>>) -> Self {
        Self {
            nodes: Default::default(),
            edges: Default::default(),
            graph_dir: path.into(),
            metadata: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct Metadata {
    earliest_time: Time,
    latest_time: Time,
    chunk_size: usize,
    t_props_chunk_size: usize,
}

impl Metadata {
    pub fn earliest_time(&self) -> Time {
        self.earliest_time
    }

    pub fn latest_time(&self) -> Time {
        self.latest_time
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub fn t_props_chunk_size(&self) -> usize {
        self.t_props_chunk_size
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            earliest_time: Time::MAX,
            latest_time: Time::MIN,
            chunk_size: 0,
            t_props_chunk_size: 0,
        }
    }
}

impl Metadata {
    pub(crate) fn new(
        earliest_time: Time,
        latest_time: Time,
        chunk_size: usize,
        t_props_chunk_size: usize,
    ) -> Self {
        Self {
            earliest_time,
            latest_time,
            chunk_size,
            t_props_chunk_size,
        }
    }

    fn from_path(graph_dir: &Path) -> Result<Self, RAError> {
        let path = GraphPaths::Metadata.to_path(graph_dir, 0);
        let metadata = std::fs::read(path)?;
        let metadata = bincode::deserialize(&metadata)?;
        Ok(metadata)
    }

    pub(crate) fn write_to_path(&self, graph_dir: &Path) -> Result<(), RAError> {
        let path = GraphPaths::Metadata.to_path(graph_dir, 0);
        let metadata = bincode::serialize(self)?;
        std::fs::write(path, metadata)?;
        Ok(())
    }
}

pub fn list_sorted_files(
    path: impl AsRef<Path>,
) -> Result<impl Iterator<Item = (GraphPaths, PathBuf)>, RAError> {
    let iter = std::fs::read_dir(&path)?
        .flatten()
        .filter_map(|dir| {
            let path = dir.path();
            GraphPaths::try_from(&path).map(|p| (p, path))
        })
        .sorted();
    Ok(iter)
}

impl TempColGraphFragment {
    pub fn graph_dir(&self) -> &Path {
        &self.graph_dir
    }

    pub fn edges_storage(&self) -> &EdgeTemporalProps {
        &self.edges
    }

    pub fn nodes_storage(&self) -> &Nodes {
        &self.nodes
    }

    pub(crate) fn from_graph<T: AsTime, G: GraphLike<T>>(
        graph: &G,
        graph_dir: impl AsRef<Path>,
        layer: usize,
        global_nodes: &Nodes<AdjTotal>,
        edge_ids_map: &[usize],
    ) -> Result<Self, RAError> {
        let n = graph.num_nodes();
        let m = graph.num_edges();
        let graph_dir = graph_dir.as_ref();
        prepare_graph_dir(graph_dir)?;

        let mut out_offsets: Vec<usize> = vec![0; n + 1];
        let ((out_srcs, out_dsts), disk_eids): ((Vec<_>, Vec<_>), Vec<_>) = out_offsets[1..]
            .iter_mut()
            .enumerate()
            .flat_map(|(v, o)| {
                *o = graph.out_degree(VID(v), layer);
                graph.out_edges(VID(v), layer) // (src, dst, eid)
            })
            .filter_map(|(src, dst, _)| {
                global_nodes
                    .find_edge(src, dst)
                    .map(|disk_eid| ((src.0 as u64, dst.0 as u64), disk_eid.0 as u64))
            })
            .unzip();
        let actual_m = out_srcs.len();

        let mut out_src_builder: MutPrimitiveChunkedArray<u64> =
            MutPrimitiveChunkedArray::new_persisted(actual_m, graph_dir, GraphPaths::AdjOutSrcs);
        let mut out_dst_builder: MutPrimitiveChunkedArray<u64> =
            MutPrimitiveChunkedArray::new_persisted(actual_m, graph_dir, GraphPaths::AdjOutDsts);
        let mut out_eids_builder: MutPrimitiveChunkedArray<u64> =
            MutPrimitiveChunkedArray::new_persisted(actual_m, graph_dir, GraphPaths::AdjOutEdges);

        par_cum_sum(&mut out_offsets);
        let mut adj_out_offsets_builder = MutChunkedOffsets::new(
            n,
            Some((GraphPaths::AdjOutOffsets, graph_dir.to_path_buf())),
            true,
        );
        adj_out_offsets_builder.push_chunk(out_offsets)?;

        let mut global_edge_t_offsets: Vec<usize> = vec![0; m + 1];
        let edge_timestamps = global_edge_t_offsets[1..]
            .par_iter_mut()
            .zip((0..m).into_par_iter())
            .flat_map_iter(|(o, disk_eid)| {
                let eid = edge_ids_map[disk_eid];
                let timestamps = graph.edge_additions(EID(eid), layer);
                *o = timestamps.try_len().unwrap_or_else(|_| timestamps.count());
                graph.edge_additions(EID(eid), layer)
            })
            .collect::<Vec<_>>();

        let mut local_edge_t_offsets = Vec::with_capacity(actual_m + 1);
        local_edge_t_offsets.push(0);
        local_edge_t_offsets.extend(
            global_edge_t_offsets[1..]
                .iter()
                .filter(|&&count| count > 0),
        );
        par_cum_sum(&mut local_edge_t_offsets);

        par_cum_sum(&mut global_edge_t_offsets);

        let num_timestamps = edge_timestamps.len();

        let props = graph.edge_prop_keys();
        let (prop_cols, prop_fields): (Vec<_>, Vec<_>) = props
            .par_iter()
            .enumerate()
            .map(|(id, key)| {
                let col = graph.prop_as_arrow(
                    &disk_eids,
                    &edge_ids_map,
                    &edge_timestamps,
                    &global_edge_t_offsets,
                    layer,
                    id,
                    key,
                );
                col.map(|col| {
                    let dtype = col.data_type().clone();
                    (col, Field::new(key, dtype, true))
                })
            })
            .flatten()
            .unzip();

        let mut edge_t_offset_builder = MutChunkedOffsets::new(
            m,
            Some((GraphPaths::EdgeTPropsOffsets, graph_dir.to_path_buf())),
            true,
        );
        edge_t_offset_builder.push_chunk(global_edge_t_offsets)?;

        let mut fields: Vec<Field> = vec![Field::new("time", DataType::Int64, false)];
        fields.extend(prop_fields);
        let mut props_builder = MutChunkedStructArray::new_persisted(
            num_timestamps,
            graph_dir,
            GraphPaths::EdgeTProps,
            fields,
        );

        let mut columns: Vec<Box<dyn Array>> =
            vec![PrimitiveArray::from_values(edge_timestamps.into_iter().map(|t| t.t())).boxed()];
        columns.extend(prop_cols);
        props_builder.push_chunk(columns)?;

        let mut in_offsets: Vec<usize> = vec![0; n + 1];
        let (srcs, in_eids): (Vec<_>, Vec<_>) = in_offsets[1..]
            .par_iter_mut()
            .enumerate()
            .flat_map(|(v, o)| {
                *o = graph.in_degree(VID(v), layer);
                graph.in_edges(VID(v), layer, |src, _| {
                    let disk_eid = global_nodes
                        .find_edge(src, VID(v))
                        .expect("Inbound edge not found");
                    (src.0 as u64, disk_eid.0 as u64)
                })
            })
            .unzip();
        let mut src_builder: MutPrimitiveChunkedArray<u64> =
            MutPrimitiveChunkedArray::new_persisted(m, graph_dir, GraphPaths::AdjInSrcs);
        let mut in_eids_builder: MutPrimitiveChunkedArray<u64> =
            MutPrimitiveChunkedArray::new_persisted(m, graph_dir, GraphPaths::AdjInEdges);
        in_eids_builder.push_chunk(in_eids)?;
        src_builder.push_chunk(srcs)?;
        par_cum_sum(&mut in_offsets);
        let mut adj_in_offsets_builder = MutChunkedOffsets::new(
            n,
            Some((GraphPaths::AdjInOffsets, graph_dir.to_path_buf())),
            true,
        );
        adj_in_offsets_builder.push_chunk(in_offsets)?;

        out_src_builder.push_chunk(out_srcs)?;
        out_dst_builder.push_chunk(out_dsts)?;
        out_eids_builder.push_chunk(disk_eids)?;

        let out_srcs = out_src_builder.finish()?;
        let out_dsts = out_dst_builder.finish()?;
        let in_srcs = src_builder.finish()?;
        let in_eids = in_eids_builder.finish()?;
        let out_eids = out_eids_builder.finish()?;
        let adj_out_offsets = adj_out_offsets_builder.finish()?;
        let adj_in_offsets = adj_in_offsets_builder.finish()?;

        let nodes = Nodes {
            adj_out: Adj {
                neighbours: ChunkedListArray::new_from_parts(
                    out_dsts.clone(),
                    adj_out_offsets.clone(),
                ),
                edges: ChunkedListArray::new_from_parts(out_eids, adj_out_offsets),
            },
            adj_in: Adj {
                neighbours: ChunkedListArray::new_from_parts(in_srcs, adj_in_offsets.clone()),
                edges: ChunkedListArray::new_from_parts(in_eids, adj_in_offsets),
            },
            additions: ChunkedListArray::new_from_parts(
                ChunkedArray::empty(),
                ChunkedOffsets::default(),
            ),
        };

        let props_values = props_builder.finish()?;
        let props_offsets = edge_t_offset_builder.finish()?;

        let edges = EdgeTemporalProps::from_structs(props_values, props_offsets);
        let (earliest, latest_time) = edges.earliest_latest();
        let metadata = Metadata::new(
            earliest,
            latest_time,
            nodes.adj_out.neighbours.values().chunk_size(),
            edges.t_props_chunk_size(),
        );
        Metadata::write_to_path(&metadata, graph_dir)?;

        let mut graph_layer = Self {
            nodes,
            edges,
            graph_dir: graph_dir.into(),
            metadata,
        };

        let mut local_t_prop_offsets = MutChunkedOffsets::new(actual_m, None, false);

        local_t_prop_offsets.push_chunk(local_edge_t_offsets)?;

        graph_layer.build_node_additions(
            2 * num_timestamps,
            &out_srcs,
            &local_t_prop_offsets.finish()?,
        )?;
        Ok(graph_layer)
    }

    pub fn new<P: AsRef<Path>>(graph_dir: P, mmap: bool) -> Result<Self, RAError> {
        let graph_dir = graph_dir.as_ref();
        let files = list_sorted_files(graph_dir)?;
        let mut adj_out_offsets_chunks = vec![];
        let mut adj_out_eids_chunks = vec![];
        let mut edge_tprops_offsets_chunks = vec![];
        let mut adj_in_offsets_chunks = vec![];
        let mut adj_in_srcs_chunks = vec![];
        let mut adj_in_eids_chunks = vec![];

        let mut t_props: Vec<StructArray> = vec![];

        let mut dst_ids_chunks = vec![];
        let mut src_ids_chunks = vec![];

        let mut node_additions_offsets = vec![];
        let mut node_additions_chunks = vec![];

        let mut metadata = Metadata::default();

        for (file_type, path) in files {
            match file_type {
                GraphPaths::AdjOutSrcs => {
                    let chunk = read_or_mmap_chunk(mmap, &path)?;
                    let src_ids = chunk[0]
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u64>>()
                        .unwrap()
                        .clone();
                    src_ids_chunks.push(src_ids);
                }
                GraphPaths::AdjOutDsts => {
                    let chunk = read_or_mmap_chunk(mmap, &path)?;
                    let dst_ids = chunk[0]
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u64>>()
                        .unwrap()
                        .clone();
                    dst_ids_chunks.push(dst_ids);
                }
                GraphPaths::AdjOutOffsets => {
                    let chunk = unsafe { mmap_buffer(&path, 0) }?;
                    adj_out_offsets_chunks.push(unsafe { OffsetsBuffer::new_unchecked(chunk) });
                }
                GraphPaths::EdgeTPropsOffsets => {
                    let chunk = unsafe { mmap_buffer(&path, 0) }?;
                    edge_tprops_offsets_chunks.push(unsafe { OffsetsBuffer::new_unchecked(chunk) });
                }
                GraphPaths::EdgeTProps => {
                    let chunk = read_or_mmap_chunk(mmap, &path)?;
                    let t_prop_array = chunk[0]
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .unwrap()
                        .clone();
                    t_props.push(t_prop_array);
                }
                GraphPaths::NodeAdditionsOffsets => {
                    let chunk = unsafe { mmap_buffer(&path, 0) }?;
                    node_additions_offsets.push(unsafe { OffsetsBuffer::new_unchecked(chunk) });
                }
                GraphPaths::NodeAdditions => {
                    let chunk = read_or_mmap_chunk(mmap, &path)?;
                    let node_additions = chunk[0]
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .unwrap()
                        .clone();

                    node_additions_chunks.push(node_additions);
                }
                GraphPaths::AdjInSrcs => {
                    let chunk = read_or_mmap_chunk(mmap, &path)?;
                    let array = chunk[0]
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u64>>()
                        .unwrap()
                        .clone();
                    adj_in_srcs_chunks.push(array);
                }
                GraphPaths::AdjInEdges => {
                    let chunk = read_or_mmap_chunk(mmap, &path)?;
                    let array = chunk[0]
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u64>>()
                        .unwrap()
                        .clone();
                    adj_in_eids_chunks.push(array);
                }
                GraphPaths::AdjOutEdges => {
                    let chunk = read_or_mmap_chunk(mmap, &path)?;
                    let array = chunk[0]
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u64>>()
                        .unwrap()
                        .clone();
                    adj_out_eids_chunks.push(array);
                }
                GraphPaths::AdjInOffsets => {
                    let chunk = unsafe { mmap_buffer(&path, 0) }?;
                    adj_in_offsets_chunks.push(unsafe { OffsetsBuffer::new_unchecked(chunk) });
                }
                GraphPaths::Metadata => {
                    metadata = Metadata::from_path(graph_dir)?;
                }
                _ => {} // rest not loaded here
            }
        }

        let edges =
            EdgeTemporalProps::from_structs(t_props.into(), edge_tprops_offsets_chunks.into());

        let dst_ids = ChunkedArray::from_non_nulls(dst_ids_chunks);

        let nodes = if !adj_in_offsets_chunks.is_empty() {
            Nodes::new_with_inbound(
                adj_out_offsets_chunks.into(),
                dst_ids,
                ChunkedArray::from_non_nulls(adj_out_eids_chunks),
                adj_in_offsets_chunks.into(),
                ChunkedArray::from_non_nulls(adj_in_srcs_chunks),
                ChunkedArray::from_non_nulls(adj_in_eids_chunks),
            )
        } else {
            println!("No inbounds edges, building them");
            Nodes::new(
                graph_dir,
                adj_out_offsets_chunks.into(),
                dst_ids,
                ChunkedArray::from_non_nulls(adj_out_eids_chunks),
            )?
        };

        let has_additions = !node_additions_offsets.is_empty();

        let mut grapho = Self {
            nodes,
            edges,
            graph_dir: graph_dir.into(),
            metadata,
        };

        if !has_additions {
            println!("No node additions found!",);
        } else {
            grapho.nodes.additions = ChunkedListArray::new_from_parts(
                ChunkedArray::from_non_nulls(node_additions_chunks),
                ChunkedOffsets::from(node_additions_offsets),
            );
        }

        Ok(grapho)
    }

    pub(crate) fn earliest_time(&self) -> Time {
        self.metadata.earliest_time
    }

    pub(crate) fn latest_time(&self) -> Time {
        self.metadata.latest_time
    }

    pub fn edge_property_id(&self, name: &str) -> Option<usize> {
        self.edges.property_id(name)
    }

    pub fn edges_data_type(&self) -> &[Field] {
        self.edges.prop_dtypes()
    }

    pub fn edges_props_data_type(&self) -> Option<&DataType> {
        self.edges.data_type_arrow()
    }

    pub fn build_node_additions(
        &mut self,
        chunk_size: usize,
        src_ids: &ChunkedArray<PrimitiveArray<u64>, NonNull>,
        offsets: &ChunkedOffsets,
    ) -> Result<(), RAError> {
        let additions = make_node_additions(self, src_ids, offsets, chunk_size)?;
        self.nodes.additions = additions;
        Ok(())
    }

    pub fn load_from_edge_list<
        G: GlobalOrder + Send + Sync,
        I: IntoIterator<Item = StructArray>,
    >(
        graph_dir: &Path,
        tmp_graph_dir: Option<&Path>,
        edge_chunk_size: usize,
        t_props_chunk_size: usize,
        go: Arc<G>,
        node_gids: Box<dyn Array>,
        src_col_idx: usize,
        dst_col_idx: usize,
        time_col_idx: usize,
        skip_inbound: bool,
        edge_list: I,
    ) -> Result<(ChunkedArray<PrimitiveArray<u64>, NonNull>, Self), RAError> {
        if node_gids.len() == 0 {
            return Err(RAError::EmptyChunk);
        }
        prepare_graph_dir(graph_dir)?;
        let edge_builder = EdgeFrameBuilder::new(edge_chunk_size, graph_dir, tmp_graph_dir);
        let edge_props_builder = PropsBuilder::new(graph_dir, GraphPaths::EdgeTProps, Some(0));

        let (edges, props): (Vec<_>, Vec<_>) = edge_list
            .into_iter()
            .map(|chunk| split_struct_chunk(chunk, src_col_idx, dst_col_idx, time_col_idx))
            .unzip();

        let EFBResult {
            adj_out_offsets,
            src_chunks,
            dst_chunks,
            edge_offsets,
        } = load_chunks(edge_builder, go, edges.iter().cloned())?;

        let offset_iter = ParquetOffsetIter::new(props.iter(), t_props_chunk_size);
        let edge_chunks = offset_iter.collect_vec();
        let edge_props_values = edge_props_builder.load_props_from_par_structs(
            NonZeroUsize::new(1).unwrap(),
            edge_chunks.into_par_iter(),
            |builder, chunk_id, struct_arr| builder.write_props(struct_arr, chunk_id),
        )?;

        let edges = EdgeTemporalProps::from_structs(edge_props_values.into(), edge_offsets);

        let edge_ids = gen_edge_ids(edge_chunk_size, graph_dir, src_chunks.len(), tmp_graph_dir)?;

        let nodes = if skip_inbound {
            Nodes::new_only_outbound(adj_out_offsets, dst_chunks, edge_ids)?
        } else {
            Nodes::new(graph_dir, adj_out_offsets, dst_chunks, edge_ids)?
        };

        let (earliest, latest_time) = edges.earliest_latest();
        let metadata = Metadata::new(earliest, latest_time, edge_chunk_size, t_props_chunk_size);
        Metadata::write_to_path(&metadata, graph_dir)?;

        Ok((
            src_chunks,
            TempColGraphFragment {
                nodes,
                edges,
                graph_dir: graph_dir.into(),
                metadata,
            },
        ))
    }

    pub fn num_nodes(&self) -> usize {
        self.nodes.len()
    }

    pub fn num_edges(&self) -> usize {
        self.nodes.adj_out.edges.values().len()
    }

    pub fn num_temporal_edges(&self) -> usize {
        self.edges.time_col().values().len()
    }

    pub fn edges(
        &self,
        node_id: VID,
        dir: Direction,
    ) -> Box<dyn Iterator<Item = (EID, VID)> + Send + '_> {
        match dir.into() {
            Direction::OUT => Box::new(self.nodes.out_adj_list(node_id)),
            Direction::IN => Box::new(self.nodes.in_adj_list(node_id)),
            Direction::BOTH => {
                let out = self.edges(node_id, Direction::OUT);
                let inb = self.edges(node_id, Direction::IN);
                Box::new(out.merge_by(inb, |(v1, _), (v2, _)| v1 < v2))
            }
        }
    }

    pub fn out_edges(&self, node_id: VID) -> impl DoubleEndedIterator<Item = (EID, VID)> + '_ {
        self.nodes.out_adj_list(node_id)
    }

    pub fn out_edges_from(
        &self,
        node_id: VID,
        offset: usize,
    ) -> impl DoubleEndedIterator<Item = (EID, VID)> + '_ {
        self.nodes.out_adj_list_from(node_id, offset)
    }

    pub fn in_edges(&self, node_id: VID) -> impl DoubleEndedIterator<Item = (EID, VID)> + '_ {
        self.nodes.in_adj_list(node_id)
    }

    pub fn out_edges_par(
        &self,
        node_id: VID,
    ) -> impl IndexedParallelIterator<Item = (EID, VID)> + '_ {
        self.nodes
            .out_edges_par(node_id)
            .zip(self.nodes.out_neighbours_par(node_id))
    }

    pub fn in_edges_par(
        &self,
        node_id: VID,
    ) -> impl IndexedParallelIterator<Item = (EID, VID)> + '_ {
        self.nodes
            .in_edges_par(node_id)
            .zip(self.nodes.in_neighbours_par(node_id))
    }

    pub fn edges_iter(
        &self,
        node_id: VID,
        dir: Direction,
    ) -> Box<dyn DoubleEndedIterator<Item = (EID, VID)> + Send + '_> {
        match dir {
            Direction::OUT => Box::new(self.nodes.out_adj_list(node_id)),
            Direction::IN => Box::new(self.nodes.in_adj_list(node_id)),
            Direction::BOTH => {
                todo!()
            }
        }
    }

    pub fn node(&self, v_id: VID) -> Node<'_> {
        self.nodes.node(v_id)
    }

    pub fn all_edges_iter(&self) -> impl Iterator<Item = (VID, VID)> + '_ {
        (0..self.nodes.len())
            .map(VID)
            .flat_map(move |v_id| self.node(v_id).out_neighbours().map(move |n| (v_id, n)))
    }

    pub fn all_edge_ids(&self) -> impl Iterator<Item = EID> {
        self.nodes
            .adj_out
            .edges
            .values()
            .clone()
            .sliced(..)
            .into_iter()
            .map(|eid| EID(eid as usize))
    }

    pub fn all_edge_ids_iter(&self) -> impl Iterator<Item = (EID, VID)> + '_ {
        let edge_ids = self
            .nodes
            .adj_out
            .edges
            .values()
            .slice(..)
            .into_iter()
            .map(|eid| EID(eid as usize));

        let dst_ids = self
            .nodes
            .adj_out
            .neighbours
            .values()
            .slice(..)
            .into_iter()
            .map(|dst| VID(dst as usize));
        edge_ids.zip(dst_ids)
    }

    pub fn all_edge_ids_par_iter(&self) -> impl IndexedParallelIterator<Item = (EID, VID)> + '_ {
        let edge_ids = self
            .nodes
            .adj_out
            .edges
            .values()
            .slice(..)
            .into_par_iter()
            .map(|eid| EID(eid as usize));

        let dst_ids = self
            .nodes
            .adj_out
            .neighbours
            .values()
            .slice(..)
            .into_par_iter()
            .map(|dst| VID(dst as usize));
        edge_ids.zip(dst_ids)
    }

    pub fn all_edge_ids_par(&self) -> impl IndexedParallelIterator<Item = EID> {
        self.nodes
            .adj_out
            .edges
            .values()
            .clone()
            .sliced(..)
            .into_par_iter()
            .map(|eid| EID(eid as usize))
    }

    pub fn exploded_edges(&self) -> impl Iterator<Item = (VID, VID, Time)> + '_ {
        (0..self.nodes.len())
            .map(VID)
            .flat_map(move |v_id| self.node(v_id).out_neighbours().map(move |n| (v_id, n)))
            .enumerate()
            .flat_map(|(e_id, (src, dst))| {
                self.edges_storage()
                    .timestamps::<i64>(EID(e_id))
                    .into_iter()
                    .map(move |ts| (src, dst, ts))
            })
    }

    pub fn edge_additions(&self, eid: EID) -> impl Iterator<Item = Time> + '_ {
        self.edges_storage().timestamps::<Time>(eid).into_iter()
    }

    pub fn outbound_edges(&self) -> impl Iterator<Item = (VID, EID)> + '_ {
        self.nodes
            .outbound_edges()
            .values()
            .iter()
            .zip(self.nodes.outbound_neighbours().values().iter())
            .map(|(eid, dst)| (VID(dst as usize), EID(eid as usize)))
    }

    pub fn all_nodes_par(&self) -> impl IndexedParallelIterator<Item = Node> + '_ {
        self.nodes.par_iter()
    }

    pub fn all_nodes(&self) -> impl Iterator<Item = Node> + '_ {
        self.nodes.iter()
    }

    pub(crate) fn t_len(&self) -> usize {
        self.edges.t_len()
    }
}

pub fn gen_edge_ids(
    chunk_size: usize,
    graph_dir: &Path,
    num_edges: usize,
    tmp_graph_dir: Option<impl AsRef<Path>>,
) -> Result<ChunkedArray<PrimitiveArray<u64>, NonNull>, RAError> {
    let mut edge_ids = MutPrimitiveChunkedArray::new_persisted(
        chunk_size,
        tmp_graph_dir
            .as_ref()
            .map(|p| p.as_ref())
            .unwrap_or(graph_dir),
        GraphPaths::AdjOutEdges,
    );
    for chunk in (0u64..num_edges as u64).chunks(chunk_size).into_iter() {
        edge_ids.push_chunk(chunk.collect::<Vec<_>>())?;
    }
    let edge_ids = edge_ids.finish()?;
    Ok(edge_ids)
}

#[instrument(level = "debug", skip(edges))]
pub(crate) fn make_metadata(
    edges: &EdgeTemporalProps,
    edge_chunk_size: usize,
    t_props_chunk_size: usize,
    graph_dir: &Path,
) -> Result<Metadata, RAError> {
    let (earliest, latest_time) = edges.earliest_latest();
    let metadata = Metadata::new(earliest, latest_time, edge_chunk_size, t_props_chunk_size);
    Metadata::write_to_path(&metadata, graph_dir)?;
    Ok(metadata)
}

#[instrument(level = "debug", skip_all)]
pub(crate) fn copy_edge_temporal_properties(
    src_col: &str,
    dst_col: &str,
    exclude_cols: &[&str],
    graph_dir: &Path,
    files: &[PathBuf],
    time_col: &str,
    num_threads: NonZeroUsize,
    t_props_chunk_size: usize,
) -> Result<Vec<StructArray>, RAError> {
    let mut excluded_cols = vec![src_col, dst_col];
    excluded_cols.extend_from_slice(exclude_cols);
    let reader = ParquetReader::new_from_filelist(
        graph_dir,
        files,
        Some(time_col),
        GraphPaths::EdgeTProps,
        |name| !excluded_cols.contains(&name),
    )?;
    let t_prop_values = reader.load_values(num_threads, t_props_chunk_size)?;
    Ok(t_prop_values)
}

#[instrument(level = "debug", skip_all)]
pub(crate) fn static_graph_builder<GO: GlobalOrder + Send + Sync>(
    files: &[PathBuf],
    concurrent_files: usize,
    src_col: &str,
    dst_col: &str,
    global_order: Arc<GO>,
    edge_chunk_size: usize,
    graph_dir: &Path,
    tmp_graph_dir: Option<&Path>,
) -> Result<EFBResult, RAError> {
    let source = ParquetSource::new(
        files.to_vec(),
        concurrent_files,
        src_col,
        dst_col,
        |chunk| {
            // get the source and dest and map them to their IDs also dedupe them
            resolve_and_dedup_chunk(chunk.arrays(), global_order.as_ref())
        },
    );
    let mut edge_builder = EdgeFrameBuilder::new(edge_chunk_size, graph_dir, tmp_graph_dir);
    source.produce(&mut edge_builder, |edge_builder, _, _, state| {
        edge_builder.push_update_state(state)
    })?;
    edge_builder.finalize(global_order.len())
}

pub(crate) fn read_or_mmap_chunk(
    mmap: bool,
    path: &PathBuf,
) -> Result<RecordBatchT<Box<dyn Array>>, RAError> {
    let chunk = if mmap {
        unsafe { mmap_batch(path, 0)? }
    } else {
        read_batch(path)?
    };
    Ok(chunk)
}

pub(crate) fn load_chunks<GO: GlobalOrder + Send + Sync, C: Borrow<GraphChunk>>(
    mut edge_builder: EdgeFrameBuilder,
    go: impl AsRef<GO>,
    chunks_iter: impl IntoIterator<Item = C>,
) -> Result<EFBResult, RAError> {
    // g_id, [{v_id1, e_id1}, {v_id2, e_id2}, ...]
    for chunk in chunks_iter.into_iter().map(|c| c.borrow().to_chunk()) {
        let state = resolve_and_dedup_chunk(chunk.arrays(), go.as_ref())?;
        edge_builder.push_update_state(state)?;
    }
    // finalize edge_builder
    edge_builder.finalize(go.as_ref().len())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        arrow2::datatypes::{ArrowDataType as DataType, ArrowSchema as Schema},
        global_order::GlobalMap,
        test::build_single_layer_test_fragment,
    };
    use tempfile::TempDir;

    fn schema() -> Schema {
        let srcs = Field::new("srcs", DataType::UInt64, false);
        let dsts = Field::new("dsts", DataType::UInt64, false);
        let time = Field::new("time", DataType::Int64, false);
        let weight = Field::new("weight", DataType::Float64, true);
        Schema::from(vec![srcs, dsts, time, weight])
    }

    #[test]
    fn load_one_edge_from_sorted_adj_list_num_nodes_multiple_timestamps() {
        let nodes = [1, 2];
        let edges = [(1u64, 2, 0), (1, 2, 3), (1, 2, 7)];
        let test_dir = TempDir::new().unwrap();

        let (graph, _, _) =
            build_single_layer_test_fragment(test_dir.path(), &edges, &nodes, 3, 100, 100).unwrap();

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges_iter().collect::<Vec<_>>();
        let expected = vec![(VID(0), VID(1))];
        assert_eq!(actual, expected)
    }

    #[test]
    fn load_muliple_sorted_edges() {
        let test_dir = TempDir::new().unwrap();
        let nodes = [1, 2, 3, 4, 5, 6, 7];
        let edges = [
            (1, 2, 0),
            (1, 3, 1),
            (1, 4, 2),
            (2, 3, 3),
            (2, 4, 4),
            (2, 5, 5),
            (3, 4, 6),
            (3, 5, 7),
            (3, 6, 8),
            (4, 5, 9),
            (4, 6, 10),
            (4, 7, 11),
        ];
        let (graph, _, _) =
            build_single_layer_test_fragment(test_dir.path(), &edges, &nodes, 12, 4, 4).unwrap();

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2)), (EID(2), VID(3))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges_iter().collect::<Vec<_>>();
        let expected = vec![
            (VID(0), VID(1)),
            (VID(0), VID(2)),
            (VID(0), VID(3)),
            (VID(1), VID(2)),
            (VID(1), VID(3)),
            (VID(1), VID(4)),
            (VID(2), VID(3)),
            (VID(2), VID(4)),
            (VID(2), VID(5)),
            (VID(3), VID(4)),
            (VID(3), VID(5)),
            (VID(3), VID(6)),
        ];
        assert_eq!(actual, expected);

        let mut e0 = graph.edge_additions(EID(0));
        assert_eq!(e0.next().unwrap(), 0i64);

        let mut e5 = graph.edge_additions(EID(5));
        assert_eq!(e5.next().unwrap(), 5i64);
    }

    #[test]
    fn load_muliple_sorted_edges_multiple_ts() {
        let test_dir = TempDir::new().unwrap();
        let nodes = [1, 2, 3, 4];
        let edges = [
            (1, 2, 0),
            (1, 3, 1),
            (1, 3, 2),
            (2, 3, 3),
            (2, 4, 4),
            (2, 4, 5),
        ];
        let (graph, _, _) =
            build_single_layer_test_fragment(test_dir.path(), &edges, &nodes, 6, 100, 100).unwrap();

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2))];
        assert_eq!(actual, expected);

        let actual: Vec<_> = graph.edges(VID(2), Direction::IN).collect();
        let expected = vec![(EID(1), VID(0)), (EID(2), VID(1))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges_iter().collect::<Vec<_>>();
        let expected = vec![
            (VID(0), VID(1)),
            (VID(0), VID(2)),
            (VID(1), VID(2)),
            (VID(1), VID(3)),
        ];
        assert_eq!(actual, expected);
    }

    #[test]
    fn load_muliple_sorted_edges_multiple_ts_2_input_chunks() {
        let test_dir = TempDir::new().unwrap();

        let srcs = PrimitiveArray::from_vec(vec![1u64, 1u64]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![2u64, 3u64]).boxed();
        let time = PrimitiveArray::from_vec(vec![0i64, 1i64]).boxed();
        let weight = PrimitiveArray::from_vec(vec![1.14f64, 2.14f64]).boxed();

        let chunk1 = StructArray::new(
            DataType::Struct(schema().fields),
            vec![srcs, dsts, time, weight],
            None,
        );

        let srcs = PrimitiveArray::from_vec(vec![1u64, 2u64, 2u64, 2u64]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![3u64, 3u64, 4u64, 4u64]).boxed();
        let time = PrimitiveArray::from_vec(vec![2i64, 3i64, 4i64, 5i64]).boxed();
        let weight = PrimitiveArray::from_vec(vec![3.14f64, 4.14f64, 5.14f64, 6.14f64]).boxed();

        let chunk2 = StructArray::new(
            DataType::Struct(schema().fields),
            vec![srcs, dsts, time, weight],
            None,
        );

        let node_gids = PrimitiveArray::from_vec((1u64..=4u64).collect()).boxed();

        let (_, graph) = TempColGraphFragment::load_from_edge_list(
            test_dir.path(),
            None,
            100,
            100,
            GlobalMap::from(1u64..=4u64).into(),
            node_gids,
            0,
            1,
            2,
            false,
            vec![chunk1, chunk2],
        )
        .unwrap();

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.exploded_edges().collect::<Vec<_>>();
        let expected = vec![
            (VID(0), VID(1), 0),
            (VID(0), VID(2), 1),
            (VID(0), VID(2), 2),
            (VID(1), VID(2), 3),
            (VID(1), VID(3), 4),
            (VID(1), VID(3), 5),
        ];
        assert_eq!(actual, expected);
    }

    #[test]
    fn load_muliple_sorted_edges_multiple_ts_chunks_size_1() {
        let test_dir = TempDir::new().unwrap();
        let nodes = [1, 2, 3, 4];
        let edges = [
            (1, 2, 0),
            (1, 3, 1),
            (1, 3, 2),
            (2, 3, 3),
            (2, 4, 4),
            (2, 4, 5),
        ];
        let (graph, _, _) =
            build_single_layer_test_fragment(test_dir.path(), &edges, &nodes, 100, 1, 1).unwrap();

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges_iter().collect::<Vec<_>>();
        let expected = vec![
            (VID(0), VID(1)),
            (VID(0), VID(2)),
            (VID(1), VID(2)),
            (VID(1), VID(3)),
        ];
        assert_eq!(actual, expected);
    }

    // #[test]
    // fn test_reload() {
    //     let test_dir = TempDir::new().unwrap();
    //     let graph = Graph::new();
    //     graph.add_edge(0, 0, 1, [("weight", 0.)], None).unwrap();
    //     graph.add_edge(1, 0, 1, [("weight", 1.)], None).unwrap();
    //     graph.add_edge(2, 0, 1, [("weight", 2.)], None).unwrap();
    //     graph.add_edge(3, 1, 2, [("weight", 3.)], None).unwrap();
    //     let arrow_graph = graph.persist_as_arrow(test_dir.path()).unwrap();
    //     let graph = arrow_graph.inner.layer(0);

    //     let all_exploded: Vec<_> = graph
    //         .exploded_edges()
    //         .map(|e| (e.src(), e.dst(), e.timestamp()))
    //         .collect();
    //     let expected: Vec<_> = vec![
    //         (VID(0), VID(1), 0),
    //         (VID(0), VID(1), 1),
    //         (VID(0), VID(1), 2),
    //         (VID(1), VID(2), 3),
    //     ];
    //     assert_eq!(all_exploded, expected);

    //     let node_gids = PrimitiveArray::from_slice([0u64, 1, 2]).boxed();
    //     let reloaded_graph =
    //         TempColGraphFragment::new(&graph.graph_dir, true, 0, node_gids).unwrap();
    //     check_graph_sanity(
    //         &[(0, 1, 0), (0, 1, 1), (0, 1, 2), (1, 2, 3)],
    //         &[0, 1, 2],
    //         &reloaded_graph,
    //     );
    // }

    // mod addition_bounds {
    //     use itertools::Itertools;
    //     use proptest::{prelude::*, sample::size_range};
    //     use tempfile::TempDir;

    //     use crate::graph_fragment::test::{
    //         edges_sanity_check_build_graph//, AdditionOps, Graph, NO_PROPS,
    //     };

    //     // use super::{GraphViewOps, NodeViewOps, TempColGraphFragment};

    //     fn compare_raphtory_graph(edges: Vec<(u64, u64, i64)>, chunk_size: usize) {
    //         let nodes = edges
    //             .iter()
    //             .flat_map(|(src, dst, _)| [*src, *dst])
    //             .sorted()
    //             .dedup()
    //             .collect::<Vec<_>>();

    //         let rg = Graph::new();

    //         for (src, dst, time) in &edges {
    //             rg.add_edge(*time, *src, *dst, NO_PROPS, None)
    //                 .expect("failed to add edge");
    //         }

    //         let test_dir = TempDir::new().unwrap();
    //         let graph: TempColGraphFragment = edges_sanity_check_build_graph(
    //             test_dir.path(),
    //             &edges,
    //             &nodes,
    //             edges.len() as u64,
    //             chunk_size,
    //             chunk_size,
    //         )
    //         .unwrap();

    //         for (v_id, node) in nodes.into_iter().enumerate() {
    //             let node = rg.node(node).expect("failed to get node id");
    //             let expected = node.history();
    //             let node = graph.node(v_id.into());
    //             let actual = node.timestamps().into_iter_t().collect::<Vec<_>>();
    //             assert_eq!(actual, expected);
    //         }
    //     }

    //     #[test]
    //     fn node_additions_bounds_to_arrays() {
    //         let edges = vec![(0, 0, -2), (0, 0, -1), (0, 0, 0), (0, 0, 1), (0, 0, 2)];

    //         compare_raphtory_graph(edges, 2);
    //     }

    //     #[test]
    //     fn test_load_from_graph_missing_edge() {
    //         let g = Graph::new();
    //         g.add_edge(0, 1, 2, [("test", "test1")], Some("1")).unwrap();
    //         g.add_edge(1, 2, 3, [("test", "test2")], Some("2")).unwrap();
    //         let test_dir = TempDir::new().unwrap();
    //         let _ = g.persist_as_arrow(test_dir.path()).unwrap();
    //     }

    //     #[test]
    //     fn one_edge_bounds_chunk_remainder() {
    //         let edges = vec![(0u64, 1, 0)];
    //         compare_raphtory_graph(edges, 3);
    //     }

    //     #[test]
    //     fn same_edge_twice() {
    //         let edges = vec![(0, 1, 0), (0, 1, 1)];
    //         compare_raphtory_graph(edges, 3);
    //     }

    //     proptest! {
    //         #[test]
    //         fn node_addition_bounds_test(
    //             edges in any_with::<Vec<(u8, u8, Vec<i64>)>>(size_range(1..=100).lift()).prop_map(|v| {
    //                 let mut v: Vec<(u64, u64, i64)> = v.into_iter().flat_map(|(src, dst, times)| {
    //                     let src = src as u64;
    //                     let dst = dst as u64;
    //                     times.into_iter().map(move |t| (src, dst, t))}).collect();
    //                 v.sort();
    //                 v}).prop_filter("edge list mut have one edge at least",|edges| edges.len() > 0),
    //             chunk_size in 1..300usize,
    //         ) {
    //             compare_raphtory_graph(edges, chunk_size);
    //         }
    //     }
    // }
}
