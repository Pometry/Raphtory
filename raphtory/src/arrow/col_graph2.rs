use std::{
    borrow::Borrow,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow2::{
    array::{Array, PrimitiveArray, StructArray, Utf8Array},
    chunk::Chunk,
    datatypes::{DataType, Field},
    offset::OffsetsBuffer,
};
use itertools::Itertools;
use rayon::prelude::*;

use crate::{
    arrow::{
        chunked_array::{chunked_offsets::ChunkedOffsets, list_array::ChunkedListArray},
        edge::{Edge, ExplodedEdge},
        file_prefix::GraphPaths,
        global_order::GlobalMap,
        graph_builder::{
            edge_props_builder::EdgePropsBuilder, node_addition_builder::make_node_additions,
            EdgeFrameBuilder,
        },
        load::{ipc::read_schema, mmap::mmap_buffer, parquet_reader::ParquetReader},
        prepare_graph_dir, Error,
    },
    core::{
        entities::{EID, VID},
        Direction,
    },
};

use super::{
    chunked_array::chunked_array::ChunkedArray,
    edges::Edges,
    global_order::GlobalOrder,
    load::ipc::read_batch,
    load::{
        mmap::mmap_batch,
        parquet_reader::ParquetOffsetIter,
        parquet_source::{resolve_and_dedup_chunk, ParquetSource},
        ExternalEdgeList,
    },
    nodes::{Node, Nodes},
    split_struct_chunk, GraphChunk, Time, GID,
};

#[derive(Debug)]
pub struct TempColGraphFragment {
    pub(crate) nodes: Nodes,
    pub(crate) edges: Edges,
    pub(crate) graph_dir: Box<Path>,
}

pub fn list_sorted_files(
    path: impl AsRef<Path>,
) -> Result<impl Iterator<Item = (GraphPaths, PathBuf)>, Error> {
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
    pub fn new<P: AsRef<Path>>(
        graph_dir: P,
        mmap: bool,
        layer_id: usize,
        node_gids: Box<dyn Array>,
    ) -> Result<Self, Error> {
        println!("Loading graph from {:?}", graph_dir.as_ref());
        let graph_dir = graph_dir.as_ref();
        let files = list_sorted_files(graph_dir)?;
        let mut adj_out_offsets_chunks: Vec<OffsetsBuffer<i64>> = vec![];
        let mut edge_tprops_offsets_chunks: Vec<OffsetsBuffer<i64>> = vec![];
        let mut adj_in_offsets_chunks: Vec<OffsetsBuffer<i64>> = vec![];
        let mut adj_in_srcs_chunks: Vec<PrimitiveArray<u64>> = vec![];
        let mut adj_in_eids_chunks: Vec<PrimitiveArray<u64>> = vec![];

        let mut t_props: Vec<StructArray> = vec![];

        let mut dst_ids_chunks: Vec<PrimitiveArray<u64>> = vec![];
        let mut src_ids_chunks: Vec<PrimitiveArray<u64>> = vec![];

        let mut node_additions_offsets: Vec<OffsetsBuffer<i64>> = vec![];
        let mut node_additions_chunks: Vec<StructArray> = vec![];

        for (file_type, path) in files {
            match file_type {
                GraphPaths::EdgeIds => {
                    let chunk = read_or_mmap_chunk(mmap, &path)?;
                    let src_ids = chunk[0]
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u64>>()
                        .unwrap()
                        .clone();
                    src_ids_chunks.push(src_ids);
                    let dst_ids = chunk[1]
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
                    let arrays = chunk.into_arrays();
                    let schema = read_schema(&path)?;
                    let t_prop_array =
                        StructArray::new(DataType::Struct(schema.fields), arrays, None);
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

                    let additions_struct_arr = StructArray::new(
                        DataType::Struct(vec![Field::new(
                            "additions",
                            node_additions.data_type().clone(),
                            false,
                        )]),
                        vec![node_additions.boxed()],
                        None,
                    );
                    node_additions_chunks.push(additions_struct_arr);
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
                GraphPaths::AdjInOffsets => {
                    let chunk = unsafe { mmap_buffer(&path, 0) }?;
                    adj_in_offsets_chunks.push(unsafe { OffsetsBuffer::new_unchecked(chunk) });
                }
            }
        }

        let edges = Edges::from_parts(
            src_ids_chunks,
            dst_ids_chunks,
            t_props,
            edge_tprops_offsets_chunks,
            layer_id,
        );

        let nodes = if !adj_in_offsets_chunks.is_empty() {
            Nodes::new_with_inbound(
                node_gids,
                adj_out_offsets_chunks,
                edges.dst_ids.clone(),
                adj_in_offsets_chunks,
                adj_in_srcs_chunks,
                adj_in_eids_chunks,
            )
        } else {
            println!("No inbounds edges, building them");
            Nodes::new(
                graph_dir,
                node_gids,
                adj_out_offsets_chunks,
                edges.dst_ids.clone(),
            )?
        };

        let has_additions = node_additions_chunks.len() > 0;

        let edges_chunk_size = edges.t_props_chunk_size();

        let mut grapho = Self {
            nodes,
            edges,
            graph_dir: graph_dir.into(),
        };

        if !has_additions {
            println!("No additions found, building them");
            let now = std::time::Instant::now();
            grapho.node_additions(edges_chunk_size)?;
            println!("Node additions took {:?}", now.elapsed());
        } else {
            grapho.nodes.additions = ChunkedListArray::new_from_parts(
                ChunkedArray::from(node_additions_chunks),
                ChunkedOffsets::from(node_additions_offsets),
            );
        }

        Ok(grapho)
    }

    pub(crate) fn earliest_time(&self) -> Time {
        self.edges.earliest_time
    }

    pub(crate) fn latest_time(&self) -> Time {
        self.edges.latest_time
    }

    pub fn edge_property_id(&self, name: &str) -> Option<usize> {
        self.edges.property_id(name)
    }

    pub fn edges_data_type(&self) -> &Vec<Field> {
        self.edges.data_type()
    }

    pub fn node_additions(&mut self, temp_prop_chunk_size: usize) -> Result<(), super::Error> {
        let (arrays, offsets) = make_node_additions(self, temp_prop_chunk_size)?;
        let chunked_t_array = ChunkedArray::from(arrays);

        let chunked_list_array = ChunkedListArray::new_from_parts(chunked_t_array, offsets);
        self.nodes.additions = chunked_list_array;
        Ok(())
    }

    pub fn from_edges<ID: Into<GID>, E: IntoIterator<Item = (i64, ID, ID, f64)>>(
        graph_dir: impl AsRef<Path>,
        edges: E,
        layer_id: usize,
        edge_chunk_size: usize,
    ) -> Result<Self, Error> {
        let mut times: Vec<i64> = vec![];
        let mut src_ids: Vec<GID> = vec![];
        let mut dst_ids: Vec<GID> = vec![];
        let mut weights: Vec<f64> = vec![];
        let mut go = GlobalMap::default();

        for (time, src, dst, weight) in edges {
            times.push(time);
            src_ids.push(src.into());
            dst_ids.push(dst.into());
            weights.push(weight);
        }

        for (index, id) in src_ids
            .iter()
            .chain(dst_ids.iter())
            .sorted()
            .dedup()
            .enumerate()
        {
            go.insert(id.clone(), index);
        }

        let mut sorted_gids_iter = go.sorted_gids().peekable();
        let first_id = sorted_gids_iter.peek().unwrap();
        let sorted_gids: Box<dyn Array> = match first_id {
            GID::U64(_) => {
                PrimitiveArray::from_vec(sorted_gids_iter.flat_map(|id| id.into_u64()).collect())
                    .boxed()
            }
            GID::I64(_) => {
                PrimitiveArray::from_vec(sorted_gids_iter.flat_map(|id| id.into_i64()).collect())
                    .boxed()
            }
            GID::Str(_) => {
                <Utf8Array<i64>>::from_iter_values(sorted_gids_iter.flat_map(|id| id.into_str()))
                    .boxed()
            }
        };

        let srcs = match &src_ids[0] {
            GID::U64(_) => {
                PrimitiveArray::from_iter(src_ids.into_iter().map(|v| v.into_u64())).boxed()
            }
            GID::I64(_) => {
                PrimitiveArray::from_iter(src_ids.into_iter().map(|v| v.into_i64())).boxed()
            }
            GID::Str(_) => {
                <Utf8Array<i64>>::from_iter(src_ids.into_iter().map(|v| v.into_str())).boxed()
            }
        };

        let dsts = match &dst_ids[0] {
            GID::U64(_) => {
                PrimitiveArray::from_iter(dst_ids.into_iter().map(|v| v.into_u64())).boxed()
            }
            GID::I64(_) => {
                PrimitiveArray::from_iter(dst_ids.into_iter().map(|v| v.into_i64())).boxed()
            }
            GID::Str(_) => {
                <Utf8Array<i64>>::from_iter(dst_ids.into_iter().map(|v| v.into_str())).boxed()
            }
        };
        let time = PrimitiveArray::from_vec(times).boxed();
        let weight = PrimitiveArray::from_vec(weights).boxed();

        let src_field = Field::new("srcs", DataType::UInt64, false);
        let dst_field = Field::new("dsts", DataType::UInt64, false);
        let time_field = Field::new("time", DataType::Int64, false);
        let weight_field = Field::new("weight", DataType::Float64, true);

        let chunk = StructArray::new(
            DataType::Struct(vec![src_field, dst_field, time_field, weight_field]),
            vec![srcs, dsts, time, weight],
            None,
        );
        Self::load_from_edge_list(
            graph_dir.as_ref(),
            layer_id,
            1.try_into().unwrap(),
            edge_chunk_size,
            edge_chunk_size,
            Arc::new(go),
            sorted_gids,
            0,
            1,
            2,
            [chunk],
        )
    }

    pub fn load_from_edge_list<
        G: GlobalOrder + Send + Sync,
        I: IntoIterator<Item = StructArray>,
    >(
        graph_dir: &Path,
        layer_id: usize,
        num_threads: NonZeroUsize,
        edge_chunk_size: usize,
        t_props_chunk_size: usize,
        go: Arc<G>,
        node_gids: Box<dyn Array>,
        src_col_idx: usize,
        dst_col_idx: usize,
        time_col_idx: usize,
        edge_list: I,
    ) -> Result<Self, Error> {
        if node_gids.len() == 0 {
            return Err(Error::EmptyChunk);
        }
        prepare_graph_dir(graph_dir)?;
        let mut edge_builder = EdgeFrameBuilder::new(edge_chunk_size, graph_dir);
        let edge_props_builder = EdgePropsBuilder::new(graph_dir, 0);

        let (edges, props): (Vec<_>, Vec<_>) = edge_list
            .into_iter()
            .map(|chunk| split_struct_chunk(chunk, src_col_idx, dst_col_idx, time_col_idx))
            .unzip();

        load_chunks(&mut edge_builder, go, edges.iter().cloned())?;

        let offset_iter = ParquetOffsetIter::new(props.iter(), t_props_chunk_size);
        let edge_chunks = offset_iter.collect_vec();
        let edge_props_values = edge_props_builder
            .load_t_edges_from_par_structs(num_threads, edge_chunks.into_par_iter())?;

        let edges = Edges::from_parts(
            edge_builder.src_chunks,
            edge_builder.dst_chunks,
            edge_props_values,
            edge_builder.edge_offsets,
            layer_id,
        );

        let nodes = Nodes::new(
            graph_dir,
            node_gids,
            edge_builder.adj_out_offsets,
            edges.dst_ids.clone(),
        )?;

        Ok(TempColGraphFragment {
            nodes,
            edges,
            graph_dir: graph_dir.into(),
        })
    }

    pub(crate) fn from_external_edge_list<GO: GlobalOrder + Send + Sync, P: AsRef<Path>>(
        el: &ExternalEdgeList<P>,
        graph_dir: &Path,
        global_order: Arc<GO>,
        layer_id: usize,
        exclude_cols: &[&str],
        num_threads: NonZeroUsize,
        edge_chunk_size: usize,
        t_props_chunk_size: usize,
        gids: Box<dyn Array>,
    ) -> Result<TempColGraphFragment, Error> {
        Self::from_sorted_parquet_files_edge_list_2(
            el.files(),
            global_order,
            layer_id,
            graph_dir.as_ref(),
            el.src_col,
            el.src_hash_col,
            el.dst_col,
            el.dst_hash_col,
            el.time_col,
            exclude_cols,
            num_threads,
            edge_chunk_size,
            t_props_chunk_size,
            gids,
        )
    }

    fn from_sorted_parquet_files_edge_list_2<GO: GlobalOrder + Sync + Send>(
        files: &[PathBuf],
        global_order: Arc<GO>,
        layer_id: usize,
        graph_dir: &Path,
        src_col: &str,
        src_hash_col: &str,
        dst_col: &str,
        dst_hash_col: &str,
        time_col: &str,
        exclude_cols: &[&str],
        num_threads: NonZeroUsize,
        edge_chunk_size: usize,
        t_props_chunk_size: usize,
        gids: Box<dyn Array>,
    ) -> Result<Self, Error> {
        prepare_graph_dir(graph_dir)?;

        let now = std::time::Instant::now();

        let source = ParquetSource::new(
            files.into_iter().cloned().collect(),
            8,
            Some(vec![src_col, dst_col]),
            |chunk| {
                // get the source and dest and map them to their IDs also dedupe them
                resolve_and_dedup_chunk(chunk, global_order.as_ref())
            },
        );

        let mut edge_builder = EdgeFrameBuilder::new(edge_chunk_size, graph_dir);

        source.produce(&mut edge_builder, |edge_builder, _, _, state| {
            edge_builder.push_update_state(state)
        })?;

        // finalize edge_builder
        edge_builder.finalize(global_order.len())?;

        println!("Edge builder took {:?}", now.elapsed());

        let now = std::time::Instant::now();

        let mut excluded_cols = vec![src_col, dst_col, src_hash_col, dst_hash_col];
        excluded_cols.extend_from_slice(exclude_cols);

        let reader = ParquetReader::new_from_filelist(graph_dir, files, time_col, &excluded_cols)?;

        let t_prop_values = reader.load_t_edge_values(num_threads, t_props_chunk_size)?;
        println!("COPY T prop values took {:?}", now.elapsed());

        let edge_offsets = edge_builder.edge_offsets;
        let edges = Edges::from_parts(
            edge_builder.src_chunks,
            edge_builder.dst_chunks,
            t_prop_values,
            edge_offsets,
            layer_id,
        );

        let dst_ids = edges.dst_ids.clone();

        let nodes = Nodes::new(graph_dir, gids, edge_builder.adj_out_offsets, dst_ids)?;

        Ok(TempColGraphFragment {
            nodes,
            edges,
            graph_dir: graph_dir.into(),
        })
    }

    pub(crate) fn nodes(&self) -> &Nodes {
        &self.nodes
    }
    pub fn num_nodes(&self) -> usize {
        self.nodes.len()
    }

    pub fn num_edges(&self) -> usize {
        self.edges.len()
    }

    pub fn edges(
        &self,
        node_id: VID,
        dir: Direction,
    ) -> Box<dyn Iterator<Item = (EID, VID)> + Send> {
        match dir {
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
    ) -> Box<dyn DoubleEndedIterator<Item = (EID, VID)> + Send> {
        match dir {
            Direction::OUT => Box::new(self.nodes.out_adj_list(node_id)),
            Direction::IN => Box::new(self.nodes.in_adj_list(node_id)),
            Direction::BOTH => {
                todo!()
            }
        }
    }

    pub fn edge(&self, e_id: EID) -> Edge<'_> {
        self.edges.edge(e_id)
    }

    pub fn node(&self, v_id: VID) -> Node<'_> {
        self.nodes.node(v_id)
    }

    pub fn all_edges_iter(&self) -> impl Iterator<Item = Edge> + '_ {
        self.edges.iter()
    }

    pub fn all_edges(&self) -> &Edges {
        &self.edges
    }

    pub fn all_edge_ids(&self) -> impl Iterator<Item = EID> {
        (0..self.num_edges()).map(|e_id| EID(e_id))
    }

    pub fn all_edges_par(&self) -> impl IndexedParallelIterator<Item = Edge> + '_ {
        self.edges.par_iter()
    }

    pub fn all_nodes_par(&self) -> impl IndexedParallelIterator<Item = Node> + '_ {
        self.nodes.par_iter()
    }

    pub fn exploded_edges(&self) -> impl Iterator<Item = ExplodedEdge> + '_ {
        self.edges.iter().flat_map(|e| e.explode())
    }

    pub(crate) fn t_len(&self) -> usize {
        self.edges.t_len()
    }
}

fn read_or_mmap_chunk(mmap: bool, path: &PathBuf) -> Result<Chunk<Box<dyn Array>>, Error> {
    let chunk = if mmap {
        unsafe { mmap_batch(path, 0)? }
    } else {
        read_batch(path)?
    };
    Ok(chunk)
}

pub(crate) fn load_chunks<GO: GlobalOrder + Send + Sync, C: Borrow<GraphChunk>>(
    edge_builder: &mut EdgeFrameBuilder,
    go: impl AsRef<GO>,
    chunks_iter: impl IntoIterator<Item = C>,
) -> Result<(), Error> {
    // g_id, [{v_id1, e_id1}, {v_id2, e_id2}, ...]
    for chunk in chunks_iter.into_iter().map(|c| c.borrow().to_chunk()) {
        let state = resolve_and_dedup_chunk(chunk, go.as_ref())?;
        edge_builder.push_update_state(state)?;
    }
    // finalize edge_builder
    edge_builder.finalize(go.as_ref().len())?;
    Ok(())
}

#[cfg(test)]
mod test {
    use arrow2::datatypes::{DataType, Schema};
    use proptest::prelude::*;
    use tempfile::TempDir;

    use crate::{arrow::global_order::GlobalMap, prelude::*};

    use super::*;

    fn edges_sanity_node_list(edges: &[(u64, u64, i64)]) -> Vec<u64> {
        edges
            .iter()
            .map(|(s, _, _)| *s)
            .chain(edges.iter().map(|(_, d, _)| *d))
            .sorted()
            .dedup()
            .collect()
    }

    fn edges_sanity_check_build_graph<P: AsRef<Path>>(
        test_dir: P,
        edges: &[(u64, u64, i64)],
        nodes: &[u64],
        input_chunk_size: u64,
        edge_chunk_size: usize,
        t_props_chunk_size: usize,
    ) -> Result<TempColGraphFragment, Error> {
        let chunks = edges
            .iter()
            .map(|(src, _, _)| *src)
            .chunks(input_chunk_size as usize);
        let srcs = chunks
            .into_iter()
            .map(|chunk| PrimitiveArray::from_vec(chunk.collect()));
        let chunks = edges
            .iter()
            .map(|(_, dst, _)| *dst)
            .chunks(input_chunk_size as usize);
        let dsts = chunks
            .into_iter()
            .map(|chunk| PrimitiveArray::from_vec(chunk.collect()));
        let chunks = edges
            .iter()
            .map(|(_, _, times)| *times)
            .chunks(input_chunk_size as usize);
        let times = chunks
            .into_iter()
            .map(|chunk| PrimitiveArray::from_vec(chunk.collect()));

        let schema = Schema::from(vec![
            Field::new("srcs", DataType::UInt64, false),
            Field::new("dsts", DataType::UInt64, false),
            Field::new("time", DataType::Int64, false),
        ]);

        let triples = srcs.zip(dsts).zip(times).map(move |((a, b), c)| {
            StructArray::new(
                DataType::Struct(schema.fields.clone()),
                vec![a.boxed(), b.boxed(), c.boxed()],
                None,
            )
        });

        let go: GlobalMap = nodes.iter().copied().collect();
        let node_gids = PrimitiveArray::from_slice(nodes).boxed();

        let mut graph = TempColGraphFragment::load_from_edge_list(
            test_dir.as_ref(),
            0,
            4.try_into().unwrap(),
            edge_chunk_size,
            t_props_chunk_size,
            go.into(),
            node_gids,
            0,
            1,
            2,
            triples,
        )?;
        graph.node_additions(t_props_chunk_size)?;
        Ok(graph)
    }

    fn check_graph_sanity(edges: &[(u64, u64, i64)], nodes: &[u64], graph: &TempColGraphFragment) {
        let expected_graph = Graph::new();
        for (src, dst, t) in edges {
            expected_graph
                .add_edge(*t, *src, *dst, NO_PROPS, None)
                .unwrap();
        }

        let actual_num_verts = nodes.len();
        let g_num_verts = graph.num_nodes();
        assert_eq!(actual_num_verts, g_num_verts);
        assert!(graph
            .all_edges_iter()
            .all(|e| e.src().0 < g_num_verts && e.dst().0 < g_num_verts));

        for v in 0..g_num_verts {
            let v = VID(v);
            assert!(graph
                .edges(v, Direction::OUT)
                .map(|(_, v)| v)
                .tuple_windows()
                .all(|(v1, v2)| v1 <= v2));
            assert!(graph
                .edges(v, Direction::IN)
                .map(|(_, v)| v)
                .tuple_windows()
                .all(|(v1, v2)| v1 <= v2));
        }

        let exploded_edges: Vec<_> = graph
            .exploded_edges()
            .map(|e| (nodes[e.src().0], nodes[e.dst().0], e.timestamp()))
            .collect();
        assert_eq!(exploded_edges, edges);

        // check incoming edges
        for (v_id, g_id) in nodes.iter().enumerate() {
            let node = expected_graph.node(*g_id).unwrap();
            let mut expected_inbound = node.in_edges().id().map(|(v, _)| v).collect::<Vec<_>>();
            expected_inbound.sort();

            let actual_inbound = graph
                .edges(VID(v_id), Direction::IN)
                .map(|(_, v)| nodes[v.0])
                .collect::<Vec<_>>();

            assert_eq!(expected_inbound, actual_inbound);
        }

        let unique_edges = edges.iter().map(|(src, dst, _)| (*src, *dst)).dedup();

        for (e_id, (src, dst)) in unique_edges.enumerate() {
            let edge = graph.edge(EID(e_id));
            let VID(src_id) = edge.src();
            let VID(dst_id) = edge.dst();

            assert_eq!(nodes[src_id], src);
            assert_eq!(nodes[dst_id], dst);
        }
    }

    fn edges_sanity_check_inner(
        edges: Vec<(u64, u64, i64)>,
        input_chunk_size: u64,
        edge_chunk_size: usize,
        edge_max_list_size: usize,
    ) {
        let test_dir = TempDir::new().unwrap();
        let nodes = edges_sanity_node_list(&edges);
        match edges_sanity_check_build_graph(
            test_dir.path(),
            &edges,
            &nodes,
            input_chunk_size,
            edge_chunk_size,
            edge_max_list_size,
        ) {
            Ok(graph) => {
                // check graph is sane
                check_graph_sanity(&edges, &nodes, &graph);
                let node_gids = PrimitiveArray::from_slice(&nodes).boxed();

                // check that reloading from graph dir works
                let reloaded_graph =
                    TempColGraphFragment::new(test_dir.path(), true, 0, node_gids).unwrap();
                check_graph_sanity(&edges, &nodes, &reloaded_graph)
            }
            Err(Error::NoEdgeLists | Error::EmptyChunk) => assert!(edges.is_empty()),
            Err(error) => panic!("{}", error.to_string()),
        };
    }

    proptest! {
        #[test]
        fn edges_sanity_check(
            edges in any::<Vec<(u8, u8, Vec<i64>)>>().prop_map(|v| {
                let mut v: Vec<(u64, u64, i64)> = v.into_iter().flat_map(|(src, dst, times)| {
                    let src = src as u64;
                    let dst = dst as u64;
                    times.into_iter().map(move |t| (src, dst, t))}).collect();
                v.sort();
                v}),
            input_chunk_size in 1..1024u64,
            edge_chunk_size in 1..1024usize,
            edge_max_list_size in 1..128usize
        ) {
            edges_sanity_check_inner(edges, input_chunk_size, edge_chunk_size, edge_max_list_size);
        }
    }

    #[test]
    fn edge_sanity_bad() {
        let edges = vec![
            (0, 85, -8744527736816607775),
            (0, 85, -8533859256444633783),
            (0, 85, -7949123054744509169),
            (0, 85, -7208573652910411733),
            (0, 85, -7004677070223473589),
            (0, 85, -6486844751834401685),
            (0, 85, -6420653301843451067),
            (0, 85, -6151481582745013767),
            (0, 85, -5577061971106014565),
            (0, 85, -5484794766797320810),
        ];
        edges_sanity_check_inner(edges, 3, 5, 6)
    }
    #[test]
    fn edge_sanity_more_bad() {
        let edges = vec![
            (1, 3, -8622734205120758463),
            (2, 0, -8064563587743129892),
            (2, 0, 0),
            (2, 0, 66718116),
            (2, 0, 733950369757766878),
            (2, 0, 2044789983495278802),
            (2, 0, 2403967656666566197),
            (2, 4, -9199293364914546702),
            (2, 4, -9104424882442202562),
            (2, 4, -8942117006530427874),
            (2, 4, -8805351871358148900),
            (2, 4, -8237347600058197888),
        ];
        edges_sanity_check_inner(edges, 3, 5, 6)
    }

    #[test]
    fn edges_sanity_chunk_1() {
        edges_sanity_check_inner(vec![(876787706323152993, 0, 0)], 1, 1, 1)
    }

    #[test]
    fn edges_sanity_chunk_2() {
        edges_sanity_check_inner(vec![(4, 3, 2), (4, 5, 0)], 2, 2, 2)
    }

    #[test]
    fn large_failing_edge_sanity_repeated() {
        let edges = vec![
            (0, 0, 0),
            (0, 1, 0),
            (0, 2, 0),
            (0, 3, 0),
            (0, 4, 0),
            (0, 5, 0),
            (0, 6, -30),
            (4, 7, -83),
            (4, 7, -77),
            (6, 8, -68),
            (6, 8, -65),
            (9, 10, 46),
            (9, 10, 46),
            (9, 10, 51),
            (9, 10, 54),
            (9, 10, 59),
            (9, 10, 59),
            (9, 10, 59),
            (9, 10, 65),
            (9, 11, -75),
        ];
        let input_chunk_size = 411;
        let edge_chunk_size = 5;
        let edge_max_list_size = 7;

        edges_sanity_check_inner(edges, input_chunk_size, edge_chunk_size, edge_max_list_size);
    }

    #[test]
    fn edge_sanity_chunk_broken_incoming() {
        let edges = vec![
            (0, 0, 0),
            (0, 0, 0),
            (0, 0, 66),
            (0, 1, 0),
            (2, 0, 0),
            (3, 4, 0),
            (4, 0, 0),
            (4, 4, 0),
            (4, 4, 0),
            (4, 4, 0),
            (4, 4, 0),
            (5, 0, 0),
            (6, 7, 7274856480798084567),
            (8, 3, -7707029126214574305),
        ];

        edges_sanity_check_inner(edges, 853, 122, 98)
    }

    #[test]
    fn edge_sanity_chunk_broken_something() {
        let edges = vec![(0, 3, 0), (1, 2, 0), (3, 2, 0)];
        edges_sanity_check_inner(edges, 1, 1, 1)
    }

    fn schema() -> Schema {
        let srcs = Field::new("srcs", DataType::UInt64, false);
        let dsts = Field::new("dsts", DataType::UInt64, false);
        let time = Field::new("time", DataType::Int64, false);
        let weight = Field::new("weight", DataType::Float64, true);
        Schema::from(vec![srcs, dsts, time, weight])
    }

    #[test]
    fn load_one_edge_from_sorted_adj_list_num_nodes_props() {
        let test_dir = TempDir::new().unwrap();

        let graph =
            TempColGraphFragment::from_edges(test_dir, [(9, 1u64, 2, 3.14)], 0, 100).unwrap();

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph
            .all_edges_iter()
            .map(|e| (e.src(), e.dst()))
            .collect::<Vec<_>>();
        let expected = vec![(VID(0), VID(1))];
        assert_eq!(actual, expected)
    }

    #[test]
    fn load_one_edge_from_sorted_adj_list_num_nodes_multiple_timestamps() {
        let test_dir = TempDir::new().unwrap();
        let graph = TempColGraphFragment::from_edges(
            test_dir.path(),
            [(0, 1u64, 2, 1.14), (3, 1, 2, 2.14), (7, 1, 2, 3.14)],
            0,
            100,
        )
        .unwrap();

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph
            .all_edges_iter()
            .map(|e| (e.src(), e.dst()))
            .collect::<Vec<_>>();
        let expected = vec![(VID(0), VID(1))];
        assert_eq!(actual, expected)
    }

    #[test]
    fn load_muliple_sorted_edges() {
        let test_dir = TempDir::new().unwrap();
        let graph = TempColGraphFragment::from_edges(
            test_dir.path(),
            [
                (0, 1u64, 2, 1.14),
                (1, 1, 3, 2.14),
                (2, 1, 4, 3.14),
                (3, 2, 3, 4.14),
                (4, 2, 4, 5.14),
                (5, 2, 5, 6.14),
                (6, 3, 4, 7.14),
                (7, 3, 5, 8.14),
                (8, 3, 6, 9.14),
                (9, 4, 5, 10.14),
                (10, 4, 6, 11.14),
                (11, 4, 7, 12.14),
            ],
            0,
            4,
        )
        .unwrap();

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2)), (EID(2), VID(3))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph
            .all_edges_iter()
            .map(|e| (e.src(), e.dst()))
            .collect::<Vec<_>>();
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

        let e0 = graph.edge(0.into());
        assert_eq!(e0.timestamps().into_iter().next().unwrap(), 0i64);

        let e5 = graph.edge(5.into());
        assert_eq!(e5.timestamps().into_iter().next().unwrap(), 5i64);
    }

    #[test]
    fn load_muliple_sorted_edges_multiple_ts() {
        let test_dir = TempDir::new().unwrap();
        let graph = TempColGraphFragment::from_edges(
            test_dir.path(),
            [
                (0, 1u64, 2, 1.14),
                (1, 1, 3, 2.14),
                (2, 1, 3, 3.14),
                (3, 2, 3, 4.14),
                (4, 2, 4, 5.14),
                (5, 2, 4, 6.14),
            ],
            0,
            100,
        )
        .unwrap();

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2))];
        assert_eq!(actual, expected);

        let actual: Vec<_> = graph.edges(VID(2), Direction::IN).collect();
        let expected = vec![(EID(1), VID(0)), (EID(2), VID(1))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph
            .all_edges_iter()
            .map(|e| (e.src(), e.dst()))
            .collect::<Vec<_>>();
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

        let graph = TempColGraphFragment::load_from_edge_list(
            test_dir.path(),
            0,
            4.try_into().unwrap(),
            100,
            100,
            GlobalMap::from(1u64..=4u64).into(),
            node_gids,
            0,
            1,
            2,
            vec![chunk1, chunk2],
        )
        .unwrap();

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph
            .exploded_edges()
            .map(|e| (e.src(), e.dst(), e.timestamp()))
            .collect::<Vec<_>>();
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
        let graph = TempColGraphFragment::from_edges(
            test_dir.path(),
            [
                (0, 1u64, 2, 1.14),
                (1, 1, 3, 2.14),
                (2, 1, 3, 3.14),
                (3, 2, 3, 4.14),
                (4, 2, 4, 5.14),
                (5, 2, 4, 6.14),
            ],
            0,
            1,
        )
        .unwrap();

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph
            .all_edges_iter()
            .map(|e| (e.src(), e.dst()))
            .collect::<Vec<_>>();
        let expected = vec![
            (VID(0), VID(1)),
            (VID(0), VID(2)),
            (VID(1), VID(2)),
            (VID(1), VID(3)),
        ];
        assert_eq!(actual, expected);
    }

    #[test]
    fn load_multiple_edges_across_chunks() {
        let test_dir = TempDir::new().unwrap();
        let graph = TempColGraphFragment::from_edges(
            test_dir.path(),
            [
                (0, 1u64, 2, 1.14),
                (1, 1, 3, 2.14),
                (2, 1, 4, 3.14),
                (3, 2, 3, 4.14),
                (4, 2, 4, 5.14),
                (5, 2, 5, 6.14),
                (6, 3, 4, 7.14),
                (7, 3, 5, 8.14),
                (8, 3, 6, 9.14),
                (0, 4, 5, 10.14),
                (10, 4, 6, 11.14),
                (11, 4, 7, 12.14),
            ],
            0,
            2,
        )
        .unwrap();

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2)), (EID(2), VID(3))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph
            .all_edges_iter()
            .map(|e| (e.src(), e.dst()))
            .collect::<Vec<_>>();
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
    }

    #[test]
    fn test_number_of_nodes() {
        let test_dir = TempDir::new().unwrap();
        let graph = TempColGraphFragment::from_edges(
            test_dir.path(),
            [
                (0, 1u64, 2, 1.14),
                (1, 1, 3, 2.14),
                (2, 1, 4, 3.14),
                (3, 2, 3, 4.14),
                (4, 2, 4, 5.14),
                (5, 2, 5, 6.14),
                (6, 3, 4, 7.14),
                (7, 3, 5, 8.14),
                (8, 3, 6, 9.14),
                (9, 4, 5, 10.14),
                (10, 4, 6, 11.14),
                (11, 4, 7, 12.14),
            ],
            0,
            2,
        )
        .unwrap();
        assert_eq!(graph.num_nodes(), 7);
    }

    #[test]
    fn test_single_edge_overflow() {
        let test_dir = TempDir::new().unwrap();
        let graph = TempColGraphFragment::from_edges(
            test_dir.path(),
            (0..10).map(|t| (t, 0u64, 1, 1.0)),
            0,
            2,
        )
        .unwrap();

        let all_exploded: Vec<_> = graph
            .exploded_edges()
            .map(|e| (e.src(), e.dst(), e.timestamp()))
            .collect();
        let expected: Vec<_> = (0i64..10).map(|t| (VID(0), VID(1), t)).collect();
        assert_eq!(all_exploded, expected);
    }

    #[test]
    fn missing_overflow_on_finalise() {
        let test_dir = TempDir::new().unwrap();
        let mut graph = TempColGraphFragment::from_edges(
            test_dir.path(),
            [
                (0, 0u64, 1, 0.),
                (1, 0, 1, 1.),
                (2, 0, 1, 2.),
                (3, 1, 2, 3.),
            ],
            0,
            2,
        )
        .unwrap();

        let all_exploded: Vec<_> = graph
            .exploded_edges()
            .map(|e| (e.src(), e.dst(), e.timestamp()))
            .collect();
        let expected: Vec<_> = vec![
            (VID(0), VID(1), 0),
            (VID(0), VID(1), 1),
            (VID(0), VID(1), 2),
            (VID(1), VID(2), 3),
        ];
        assert_eq!(all_exploded, expected);
        graph.node_additions(2).unwrap();

        let node_gids = PrimitiveArray::from_slice([0u64, 1, 2]).boxed();
        let reloaded_graph =
            TempColGraphFragment::new(test_dir.path(), true, 0, node_gids).unwrap();
        check_graph_sanity(
            &[(0, 1, 0), (0, 1, 1), (0, 1, 2), (1, 2, 3)],
            &[0, 1, 2],
            &reloaded_graph,
        );
    }

    mod addition_bounds {
        use arrow2::array::PrimitiveArray;
        use proptest::prelude::*;
        use tempfile::TempDir;

        use crate::arrow::graph_builder::node_addition_builder::{
            addition_bounds, bound_to_array, make_offsets, NodeAdditionBound,
        };

        use super::{TempColGraphFragment, VID};

        #[test]
        fn node_additions_bounds_1_self_edge() {
            let test_dir = TempDir::new().unwrap();
            let graph = TempColGraphFragment::from_edges(test_dir.path(), [(0, 1u64, 1, 0.)], 0, 2)
                .unwrap();

            let temp_prop_chunk_size = 2;
            let offsets =
                make_offsets(&graph, temp_prop_chunk_size).expect("failed to make offsets");

            let addition_bounds = addition_bounds(&graph, &offsets, temp_prop_chunk_size);

            let expected = vec![NodeAdditionBound {
                start: VID(0),
                end: VID(0),
                from: 0,
                to: 1,
            }];

            assert_eq!(addition_bounds, expected);
        }

        #[test]
        fn node_additions_bounds_2_nodes_1_chunk() {
            let test_dir = TempDir::new().unwrap();
            let graph = TempColGraphFragment::from_edges(test_dir.path(), [(0, 0u64, 1, 0.)], 0, 2)
                .unwrap();

            let temp_prop_chunk_size = 2;
            let offsets =
                make_offsets(&graph, temp_prop_chunk_size).expect("failed to make offsets");

            let addition_bounds = addition_bounds(&graph, &offsets, temp_prop_chunk_size);

            let expected = vec![NodeAdditionBound {
                start: VID(0),
                end: VID(1),
                from: 0,
                to: 1,
            }];

            assert_eq!(addition_bounds, expected);
        }

        #[test]
        fn node_additions_bounds_2_nodes_2_chunks() {
            let test_dir = TempDir::new().unwrap();
            let graph = TempColGraphFragment::from_edges(test_dir.path(), [(0, 0u64, 1, 0.)], 0, 2)
                .unwrap();

            let temp_prop_chunk_size = 1;
            let offsets =
                make_offsets(&graph, temp_prop_chunk_size).expect("failed to make offsets");

            let addition_bounds = addition_bounds(&graph, &offsets, temp_prop_chunk_size);

            let expected = vec![
                NodeAdditionBound {
                    start: VID(0),
                    end: VID(0),
                    from: 0,
                    to: 1,
                },
                NodeAdditionBound {
                    start: VID(1),
                    end: VID(1),
                    from: 0,
                    to: 1,
                },
            ];

            assert_eq!(addition_bounds, expected);
        }

        #[test]
        fn node_additions_bounds_5_nodes_across_3_chunks() {
            // first chunk has node 0 and a bit of node 1
            // second chunk has node 1 only
            // third chunk has node 1, 2 and a bit of node 3
            // fourth chunk has node 3 and 4
            // chunks_size is 3

            let test_dir = TempDir::new().unwrap();
            let graph = TempColGraphFragment::from_edges(
                test_dir.path(),
                [
                    (0, 0u64, 1, 0.),
                    (1, 0u64, 2, 0.),
                    (2, 1u64, 1, 0.),
                    (3, 1u64, 1, 0.),
                    (4, 1u64, 3, 0.),
                    (5, 1u64, 3, 0.),
                    (5, 3u64, 4, 0.),
                ],
                0,
                2,
            )
            .unwrap();

            let temp_prop_chunk_size = 3;
            let offsets =
                make_offsets(&graph, temp_prop_chunk_size).expect("failed to make offsets");

            let addition_bounds = addition_bounds(&graph, &offsets, temp_prop_chunk_size);

            let expected_bounds_vids = vec![
                (VID(0), VID(1)),
                (VID(1), VID(1)),
                (VID(1), VID(3)),
                (VID(3), VID(4)),
            ];

            let actual = addition_bounds
                .iter()
                .map(|bound| (bound.start, bound.end))
                .collect::<Vec<_>>();

            assert_eq!(actual, expected_bounds_vids);

            let expected_bounds = vec![
                NodeAdditionBound {
                    start: VID(0),
                    end: VID(1),
                    from: 0,
                    to: 1,
                },
                NodeAdditionBound {
                    start: VID(1),
                    end: VID(1),
                    from: 1,
                    to: 4,
                },
                NodeAdditionBound {
                    start: VID(1),
                    end: VID(3),
                    from: 4,
                    to: 1,
                },
                NodeAdditionBound {
                    start: VID(3),
                    end: VID(4),
                    from: 1,
                    to: 1,
                },
            ];

            assert_eq!(addition_bounds, expected_bounds);

            let actual = expected_bounds
                .into_iter()
                .map(|bound| bound_to_array(bound, &graph))
                .collect::<Vec<_>>();

            let expected = vec![
                PrimitiveArray::from_vec(vec![0i64, 1, 0]),
                PrimitiveArray::from_vec(vec![2i64, 3, 4]),
                PrimitiveArray::from_vec(vec![5i64, 1, 4]),
                PrimitiveArray::from_vec(vec![5i64, 5]),
            ];

            assert_eq!(actual, expected);
        }

        #[test]
        fn one_node_across_3_chunks() {
            let test_dir = TempDir::new().unwrap();
            let graph = TempColGraphFragment::from_edges(
                test_dir.path(),
                [
                    (0, 0u64, 0, 0.),
                    (1, 0u64, 0, 0.),
                    (2, 0u64, 0, 0.),
                    (3, 0u64, 0, 0.),
                    (4, 0u64, 0, 0.),
                ],
                0,
                2,
            )
            .unwrap();

            let temp_prop_chunk_size = 2;
            let offsets =
                make_offsets(&graph, temp_prop_chunk_size).expect("failed to make offsets");

            let addition_bounds = addition_bounds(&graph, &offsets, temp_prop_chunk_size);

            let expected = vec![
                NodeAdditionBound {
                    start: VID(0),
                    end: VID(0),
                    from: 0,
                    to: 2,
                },
                NodeAdditionBound {
                    start: VID(0),
                    end: VID(0),
                    from: 2,
                    to: 4,
                },
                NodeAdditionBound {
                    start: VID(0),
                    end: VID(0),
                    from: 4,
                    to: 5,
                },
            ];

            assert_eq!(addition_bounds, expected);
        }

        fn node_addition_bounds_check(edges: Vec<(i64, u64, u64, f64)>, chunk_size: usize) {
            let test_dir = TempDir::new().unwrap();
            let graph = TempColGraphFragment::from_edges(test_dir.path(), edges, 0, chunk_size)
                .expect("failed to create graph");

            let offsets = make_offsets(&graph, chunk_size).expect("failed to make offsets");
            let bounds = addition_bounds(&graph, &offsets, chunk_size);

            assert_eq!(bounds.first().unwrap().start, VID(0));
            assert_eq!(bounds.last().unwrap().end, VID(graph.num_nodes() - 1));
            bounds.windows(2).for_each(|w| {
                let (prev, next) = (&w[0], &w[1]);
                assert!(prev.end <= next.start);
            });
        }

        #[test]
        fn one_edge_bounds_chunk_remainder() {
            let edges = vec![(0, 0u64, 1, 1.)];
            node_addition_bounds_check(edges, 3)
        }

        proptest! {
            #[test]
            fn node_addition_bounds_test(
                edges in any::<Vec<(u8, u8, Vec<i64>)>>().prop_map(|v| {
                    let mut v: Vec<(i64, u64, u64, f64)> = v.into_iter().flat_map(|(src, dst, times)| {
                        let src = src as u64;
                        let dst = dst as u64;
                        times.into_iter().map(move |t| (t, src, dst, 1f64))}).collect();
                    v.sort_by_key(|(t, src, dst, _)| (*src, *dst, *t));
                    v}).prop_filter("edge list mut have one edge at least",|edges| edges.len() > 0),
                chunk_size in 1..1024usize,
            ) {
                node_addition_bounds_check(edges, chunk_size)
            }
        }
    }
}
