use crate::{
    arrow2::{
        array::{Array, StructArray},
        compute::concatenate::concatenate,
        datatypes::{ArrowDataType as DataType, ArrowSchema as Schema, Field},
        legacy::error,
    },
    global_order::GidType,
    load::parquet_reader::{NumRows, TrySlice},
};
use itertools::Itertools;
use polars_arrow::{
    compute::cast::{self, CastOptions},
    datatypes::{ArrowDataType, TimeUnit},
    record_batch::RecordBatchT,
};
use std::{
    num::TryFromIntError,
    ops::Range,
    path::{Path, PathBuf},
};

pub use polars_arrow as arrow2;

pub use raphtory_api::core::entities::{GidRef, GID};

pub mod algorithms;
pub mod chunked_array;
pub mod compute;
pub mod disk_hmap;
pub mod edge;
pub mod edge_list;
pub mod edges;
pub mod global_order;
pub mod graph;
pub mod graph_builder;
pub mod graph_fragment;
pub mod interop;
pub mod load;
pub mod merge;
pub mod nodes;
pub mod properties;
pub mod timestamps;
pub mod tprops;

pub mod prelude {
    pub use super::chunked_array::array_ops::*;
}

pub type Time = i64;

#[derive(thiserror::Error, Debug)]
pub enum RAError {
    #[error("Failed to memory map file {file:?}, source: {source}")]
    MMap {
        file: PathBuf,
        source: error::PolarsError,
    },
    #[error("Arrow error: {0}")]
    Arrow(#[from] error::PolarsError),
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
    //serde error
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Bad data type for node column: {0:?}")]
    DType(DataType),
    #[error("Size mismatch, expected: {expected}, actual: {actual}?")]
    SizeError { expected: usize, actual: usize },
    #[error("Graph directory is not empty before loading")]
    GraphDirNotEmpty,
    #[error("Invalid type for column: {0}")]
    InvalidTypeColumn(String),
    #[error("Secondary index needs to be UInt64, not {0:?}")]
    InvalidIndexColumn(ArrowDataType),
    #[error("Missing secondary index column")]
    MissingIndexColumn,
    #[error("Timestamps need to be Int64, not {0:?}")]
    InvalidTimestampColumn(ArrowDataType),
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    #[error("No Edge lists found in input path")]
    NoEdgeLists,
    #[error("No Node lists found in input path")]
    NoNodeLists,
    #[error("Unable to open graph: {0:?}")]
    EmptyGraphDir(PathBuf),
    #[error("Empty parquet chunk")]
    EmptyChunk,
    #[error("Conversion error: {0}")]
    ArgumentError(#[from] TryFromIntError),
    #[error("Invalid file: {0:?}")]
    InvalidFile(PathBuf),
    #[error("Invalid metadata: {0:?}")]
    MetadataError(#[from] Box<bincode::ErrorKind>),
    #[error("Failed to cast mmap_mut to [i64]: {0:?}")]
    SliceCastError(bytemuck::PodCastError),
    #[error("Failed to cast array")]
    TypeCastError,
    #[error("Missing chunk {0}")]
    MissingChunk(usize),
    #[error("Cannot merge global node ids due to type mismatch: left {0:?}, right {1:?}")]
    NodeGIDTypeError(GidType, GidType),
    #[error("Cannot find node {0:?}")]
    MissingNode(GID),
    #[error("Timestamps are required for all rows")]
    MissingTimestamp,
    #[error("Cannot merge edge property {name} due to type mismatch: left {left_dtype:?}, right {right_dtype:?}")]
    EdgeMergeDTypeError {
        name: String,
        left_dtype: ArrowDataType,
        right_dtype: ArrowDataType,
    },

    #[error("Mismatched column lengths: {0} != {1}")]
    MismatchedLengths(usize, usize),
    #[error("Failed to read parquet files: {0:?}")]
    GenericFailure(String),
}

const TIME_COLUMN: &str = "rap_time";

pub(crate) const V_COLUMN: &str = "v";
pub(crate) const E_COLUMN: &str = "e";

#[inline]
pub fn adj_schema() -> DataType {
    DataType::Struct(vec![
        Field::new(V_COLUMN, DataType::UInt64, false),
        Field::new(E_COLUMN, DataType::UInt64, false),
    ])
}

pub fn cast_time_column(time_col: Box<dyn Array>) -> Result<Box<dyn Array>, RAError> {
    let time_col = match time_col.data_type() {
        DataType::Int64 | DataType::Date64 => time_col,
        DataType::Date32 => {
            cast::cast(time_col.as_ref(), &DataType::Date64, CastOptions::default())?
        }

        DataType::Timestamp(_, _) => cast::cast(
            time_col.as_ref(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".to_string())),
            CastOptions::default(),
        )?,
        d_type => return Err(RAError::DType(d_type.clone())),
    };
    Ok(time_col)
}

pub mod file_prefix {
    use std::{
        path::{Path, PathBuf},
        str::FromStr,
    };

    use itertools::Itertools;
    use strum::{AsRefStr, EnumString};

    use super::RAError;

    #[derive(AsRefStr, EnumString, PartialEq, Debug, Ord, PartialOrd, Eq, Copy, Clone)]
    pub enum GraphPaths {
        NodeAdditions,
        NodeAdditionsOffsets,
        NodeTProps,
        NodeTPropsTimestamps,
        NodeTPropsSecondaryIndex,
        NodeTPropsOffsets,
        NodeConstProps,
        NodeTypeIds,
        NodeTypes,
        AdjOutSrcs,
        AdjOutDsts,
        AdjOutEdges,
        AdjOutOffsets,
        EdgeTPropsOffsets,
        EdgeTProps,
        AdjInSrcs,
        AdjInEdges,
        AdjInOffsets,
        Metadata,
        HashMap,
    }

    #[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Copy, Clone)]
    pub struct GraphFile {
        pub prefix: GraphPaths,
        pub chunk: usize,
    }

    impl GraphFile {
        pub fn try_from_path(path: impl AsRef<Path>) -> Option<Self> {
            let name = path.as_ref().file_stem()?.to_str()?;
            let mut name_parts = name.split('-');
            let prefix = GraphPaths::from_str(name_parts.next()?).ok()?;
            let chunk_str = name_parts.next();
            let chunk: usize = chunk_str?.parse().ok()?;
            Some(Self { prefix, chunk })
        }
    }

    impl GraphPaths {
        pub fn try_from(path: impl AsRef<Path>) -> Option<Self> {
            let path = path.as_ref();
            let name = path.file_name().and_then(|name| name.to_str())?;
            let prefix = name.split('-').next()?;
            GraphPaths::from_str(prefix).ok()
        }

        pub fn to_path(&self, location_path: impl AsRef<Path>, id: usize) -> PathBuf {
            let prefix: &str = self.as_ref();
            let make_path = make_path(location_path, prefix, id);
            make_path
        }
    }

    pub fn make_path(location_path: impl AsRef<Path>, prefix: &str, id: usize) -> PathBuf {
        let file_path = location_path
            .as_ref()
            .join(format!("{}-{:08}.ipc", prefix, id));
        file_path
    }

    pub fn sorted_file_list(
        dir: impl AsRef<Path>,
        prefix: GraphPaths,
    ) -> Result<impl Iterator<Item = PathBuf>, RAError> {
        let mut files = dir
            .as_ref()
            .read_dir()?
            .filter_map_ok(|f| {
                let path = f.path();
                GraphFile::try_from_path(&path)
                    .filter(|f| f.prefix == prefix)
                    .map(|f| (f.chunk, path))
            })
            .collect::<Result<Vec<_>, _>>()?;
        files.sort();
        for (i, (chunk, _)) in files.iter().enumerate() {
            if &i != chunk {
                return Err(RAError::MissingChunk(i));
            }
        }
        Ok(files.into_iter().map(|(_, path)| path))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GraphChunk {
    srcs: Box<dyn Array>,
    dsts: Box<dyn Array>,
}

impl GraphChunk {
    pub fn to_chunk(&self) -> RecordBatchT<Box<dyn Array>> {
        RecordBatchT::new(vec![self.srcs.clone(), self.dsts.clone()])
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PropsChunk(pub StructArray);

impl NumRows for &PropsChunk {
    fn num_rows(&self) -> usize {
        self.0.len()
    }
}

impl TrySlice for &PropsChunk {
    fn try_slice(&self, range: Range<usize>) -> Result<StructArray, RAError> {
        self.0.try_slice(range)
    }
}

pub fn concat<A: Array + Clone>(arrays: Vec<A>) -> Result<A, RAError> {
    let mut refs: Vec<&dyn Array> = Vec::with_capacity(arrays.len());
    for array in arrays.iter() {
        refs.push(array);
    }
    Ok(concatenate(&refs)?
        .as_any()
        .downcast_ref::<A>()
        .unwrap()
        .clone())
}

pub(crate) fn split_struct_chunk(
    chunk: StructArray,
    src_col_idx: usize,
    dst_col_idx: usize,
    time_col_idx: usize,
) -> (GraphChunk, PropsChunk) {
    let (fields, cols, _) = chunk.into_data();
    split_chunk(
        cols.to_vec(),
        src_col_idx,
        dst_col_idx,
        time_col_idx,
        fields.into(),
    )
}

pub(crate) fn split_chunk<I: IntoIterator<Item = Box<dyn Array>>>(
    columns_in_chunk: I,
    src_col_idx: usize,
    dst_col_idx: usize,
    time_col_idx: usize,
    chunk_schema: Schema,
) -> (GraphChunk, PropsChunk) {
    let all_cols = columns_in_chunk.into_iter().collect_vec();

    let time_d_type = all_cols[time_col_idx].data_type().clone();
    assert_eq!(time_d_type, DataType::Int64, "time column must be i64");
    let first_len = all_cols.first().unwrap().len();
    if all_cols.iter().any(|arr| arr.len() != first_len) {
        panic!("All arrays in a chunk must have the same length");
    }

    let mut temporal_props = vec![all_cols[time_col_idx].clone()];
    for (i, column) in all_cols.iter().enumerate() {
        if !(i == src_col_idx || i == dst_col_idx || i == time_col_idx) {
            temporal_props.push(column.clone());
        }
    }

    let mut props_only_schema =
        chunk_schema.filter(|i, _| !(i == src_col_idx || i == dst_col_idx || i == time_col_idx));
    // put time as the first column in the struct
    props_only_schema
        .fields
        .insert(0, Field::new(TIME_COLUMN, time_d_type, false));
    let data_type = DataType::Struct(props_only_schema.fields);
    let t_prop_cols = StructArray::new(data_type, temporal_props, None);

    (
        GraphChunk {
            srcs: all_cols[src_col_idx].clone(),
            dsts: all_cols[dst_col_idx].clone(),
            // time: all_cols[time_col_idx].clone(),
        },
        PropsChunk(t_prop_cols),
    )
}

fn prepare_graph_dir<P: AsRef<Path>>(graph_dir: P) -> Result<(), RAError> {
    // create graph dir if it does not exist
    // if it exists make sure it's empty
    std::fs::create_dir_all(&graph_dir)?;

    let mut dir_iter = std::fs::read_dir(&graph_dir)?;
    if dir_iter.next().is_some() {
        return Err(RAError::GraphDirNotEmpty);
    }

    Ok(())
}

fn prepare_layer_dir<P: AsRef<Path>>(graph_dir: P, layer_name: &str) -> Result<PathBuf, RAError> {
    let path = graph_dir.as_ref().join(format!("l_{layer_name}"));
    prepare_graph_dir(&path)?;
    Ok(path)
}

pub mod utils {

    use std::hash::{Hash, Hasher};
    use twox_hash::XxHash64;

    use crate::GID;

    pub fn calculate_hash<T: Hash + ?Sized>(t: &T) -> u64 {
        let mut s = XxHash64::default();
        t.hash(&mut s);
        s.finish()
    }

    pub fn calculate_hash_spark(gid: &GID) -> i64 {
        let mut s = XxHash64::with_seed(42);
        match gid {
            GID::U64(x) => s.write_u64(*x),
            GID::Str(t) => {
                t.chars().for_each(|c| s.write_u8(c as u8));
            }
        }
        s.finish() as i64
    }
}

#[cfg(test)]
mod test {
    use crate::{
        chunked_array::chunked_array::{ChunkedArray, NonNull},
        disk_hmap::DiskHashMap,
        edge_list::EdgeList,
        global_order::GlobalOrder,
        graph::{
            build_total_edge_list, fix_t_prop_offsets, rebuild_edge_ids_values_for_static_graph,
            TemporalGraph, TotalGraphResult,
        },
        graph_builder::EFBResult,
        graph_fragment::TempColGraphFragment,
        nodes::Nodes,
        prelude::BaseArrayOps,
        prepare_graph_dir, prepare_layer_dir, RAError,
    };
    use itertools::{multiunzip, Itertools};
    use polars_arrow::{
        array::{Array, PrimitiveArray, StructArray},
        datatypes::{ArrowDataType, Field},
    };
    use raphtory_api::core::entities::GidRef;
    use std::{path::Path, sync::Arc};
    use tempfile::TempDir;

    pub fn build_test_layer<P: AsRef<Path>>(
        test_dir: P,
        go: Arc<DiskHashMap>,
        gids: Box<dyn Array>,
        _layer_id: usize,
        layer_name: &str,
        edges: impl IntoIterator<Item = StructArray>,
        chunk_size: usize,
        t_props_chunk_size: usize,
    ) -> Result<
        (
            ChunkedArray<PrimitiveArray<u64>, NonNull>,
            TempColGraphFragment,
        ),
        RAError,
    > {
        let test_dir = prepare_layer_dir(test_dir, layer_name)?;
        if gids.is_empty() {
            return Ok((
                Default::default(),
                TempColGraphFragment::new_empty(test_dir.as_ref()),
            ));
        }
        let (src_ids, mut graph) = TempColGraphFragment::load_from_edge_list(
            test_dir.as_ref(),
            None,
            chunk_size,
            t_props_chunk_size,
            go,
            gids,
            1,
            2,
            0,
            false,
            edges,
        )?;

        let offsets = graph.edges_storage().time_col().offsets().clone();
        graph.build_node_additions(chunk_size, &src_ids, &offsets)?;
        Ok((src_ids, graph))
    }

    pub fn build_simple_edge_chunks(
        edges: &[(u64, u64, i64)],
        input_chunk_size: usize,
    ) -> impl Iterator<Item = StructArray> + '_ {
        edges.chunks(input_chunk_size).map(|chunk| {
            let (srcs, dsts, times): (Vec<_>, Vec<_>, Vec<_>) = multiunzip(chunk.iter().copied());
            let srcs = PrimitiveArray::from_vec(srcs).boxed();
            let dsts = PrimitiveArray::from_vec(dsts).boxed();
            let times = PrimitiveArray::from_vec(times).boxed();

            let fields = vec![
                Field::new("time", times.data_type().clone(), false),
                Field::new("src", srcs.data_type().clone(), false),
                Field::new("dst", dsts.data_type().clone(), false),
            ];

            StructArray::new(ArrowDataType::Struct(fields), vec![times, srcs, dsts], None)
        })
    }

    pub fn build_simple_edges(edges: &[(u64, u64, i64)]) -> Option<StructArray> {
        build_simple_edge_chunks(edges, usize::MAX).next()
    }
    pub fn build_single_layer_test_fragment<P: AsRef<Path>>(
        test_dir: P,
        edges: &[(u64, u64, i64)],
        nodes: &[u64],
        input_chunk_size: usize,
        chunk_size: usize,
        t_props_chunk_size: usize,
    ) -> Result<(TempColGraphFragment, Arc<DiskHashMap>, Box<dyn Array>), RAError> {
        let (go, gids) = build_gids(nodes.iter().copied());
        let go = Arc::new(go);
        let edges = build_simple_edge_chunks(edges, input_chunk_size);
        build_test_layer(
            test_dir,
            go.clone(),
            gids.clone(),
            0,
            "_default",
            edges,
            chunk_size,
            t_props_chunk_size,
        )
        .map(|(_, layer)| (layer, go, gids))
    }

    pub fn build_gids(nodes: impl IntoIterator<Item = u64>) -> (DiskHashMap, Box<dyn Array>) {
        let node_gids = PrimitiveArray::from_values(nodes).boxed();
        let go = DiskHashMap::from_sorted_dedup(node_gids.clone()).unwrap();
        (go, node_gids)
    }

    pub fn make_graph(
        edges: &[(u64, u64, i64)],
        nodes: &[u64],
        chunk_size: usize,
        t_props_chunk_size: usize,
    ) -> (TemporalGraph, TempDir) {
        let test_dir = TempDir::new().unwrap();
        let (layer, global_mapping, global_ordering) = build_single_layer_test_fragment(
            test_dir.path(),
            &edges,
            &nodes,
            usize::MAX,
            chunk_size,
            t_props_chunk_size,
        )
        .unwrap();

        let (src_ids, dst_ids): (Vec<_>, Vec<_>) = edges
            .iter()
            .filter_map(|(src, dst, _)| {
                let src = global_mapping.find(GidRef::U64(*src))?;
                let dst = global_mapping.find(GidRef::U64(*dst))?;
                Some((src as u64, dst as u64))
            })
            .dedup()
            .unzip();

        let edge_list = EdgeList::new(
            ChunkedArray::from_non_nulls(vec![PrimitiveArray::from_vec(src_ids)]),
            ChunkedArray::from_non_nulls(vec![PrimitiveArray::from_vec(dst_ids)]),
        );

        let dst_ids = (&layer.nodes).outbound_neighbours().values();
        let offsets = (&layer.nodes).outbound_neighbours().offsets().clone();
        let nodes = Nodes::new_out_only_total(offsets, dst_ids.clone());

        (
            TemporalGraph::new_from_layers(
                test_dir.path().to_path_buf(),
                global_ordering,
                global_mapping,
                edge_list,
                nodes,
                vec![layer],
                vec!["_default".to_string()],
            ),
            test_dir,
        )
    }

    use crate::chunked_array::array_ops::ArrayOps;
    pub fn make_multilayer_graph<'a>(
        nodes: impl IntoIterator<Item = u64>,
        edges: Vec<(String, Option<StructArray>)>,
        types: Option<Box<dyn Array>>,
        node_c_props: Option<StructArray>,
        node_t_props: Option<StructArray>,
        chunk_size: usize,
        t_props_chunk_size: usize,
    ) -> (TemporalGraph, TempDir) {
        let test_dir = TempDir::new().unwrap();
        let temp_test_dirs = edges
            .iter()
            .map(|_| TempDir::new().unwrap())
            .collect::<Vec<_>>();
        let (go, gids) = build_gids(nodes);
        let go = Arc::new(go);

        let (srcs, mut layers): (Vec<_>, Vec<_>) = edges
            .iter()
            .zip(temp_test_dirs.iter())
            .map(|((layer_name, edges), temp_layer_dir)| {
                let layer_dir = test_dir.as_ref().join(format!("l_{}", layer_name));

                if !gids.is_empty() {
                    let (src_ids, layer) = TempColGraphFragment::load_from_edge_list(
                        &layer_dir,
                        Some(temp_layer_dir.as_ref()),
                        chunk_size,
                        t_props_chunk_size,
                        go.clone(),
                        gids.clone(),
                        1,
                        2,
                        0,
                        true,
                        edges.as_ref().cloned().into_iter(),
                    )
                    .unwrap();
                    (src_ids, layer)
                } else {
                    prepare_graph_dir(&layer_dir).unwrap();
                    (
                        Default::default(),
                        TempColGraphFragment::new_empty(layer_dir),
                    )
                }
            })
            .unzip();

        let layer_names = edges
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();

        let TotalGraphResult {
            src_ids,
            dst_ids,
            offsets,
        } = build_total_edge_list(
            gids.len(),
            chunk_size,
            test_dir.path(),
            layers.iter().zip(srcs.iter()).map(|(layer, src_ids)| {
                (
                    src_ids.iter(),
                    layer.nodes_storage().outbound_neighbours().values().iter(),
                )
            }),
        )
        .unwrap();

        let total_num_edges = dst_ids.len();
        let global_nodes = Nodes::new_out_only_total(offsets, dst_ids.clone());
        let edge_list = EdgeList::new(src_ids.clone(), dst_ids.clone());

        layers
            .iter_mut()
            .zip(srcs.iter())
            .for_each(|(fragment, src_ids)| {
                let nodes_additions = {
                    let offsets = fragment.edges_storage().time_col().offsets().clone();

                    fragment
                        .build_node_additions(chunk_size, src_ids, &offsets)
                        .unwrap();

                    fragment.nodes_storage().additions().clone()
                };

                let mut nodes = rebuild_edge_ids_values_for_static_graph(
                    chunk_size,
                    &[EFBResult {
                        adj_out_offsets: fragment
                            .nodes_storage()
                            .outbound_neighbours()
                            .offsets()
                            .clone(),
                        src_chunks: src_ids.clone(),
                        dst_chunks: fragment
                            .nodes_storage()
                            .outbound_neighbours()
                            .values()
                            .clone(),
                        edge_offsets: Default::default(),
                    }],
                    &[fragment.graph_dir().to_path_buf()],
                    &global_nodes,
                )
                .unwrap()
                .pop()
                .unwrap();

                let t_prop_new_offsets = fix_t_prop_offsets(
                    total_num_edges,
                    chunk_size,
                    fragment.graph_dir(),
                    fragment.edges_storage().time_col().offsets(),
                    fragment.nodes_storage().outbound_edges().values().iter(),
                    nodes.outbound_edges().values().iter(),
                )
                .unwrap();

                fragment.edges = fragment
                    .edges_storage()
                    .with_new_offsets(t_prop_new_offsets);

                nodes.additions = nodes_additions;
                fragment.nodes = nodes;
            });

        let mut graph = TemporalGraph::new_from_layers(
            test_dir.path().to_path_buf(),
            gids,
            go.clone(),
            edge_list,
            global_nodes,
            layers,
            layer_names,
        );
        if let Some(types) = types {
            graph
                .load_node_types_from_chunks([types].into_iter().map(Ok), graph.num_nodes())
                .unwrap();
        }
        graph
            .load_const_node_props_from_chunks(node_c_props.map(Ok), graph.num_nodes())
            .unwrap();

        graph
            .load_temporal_node_props_from_chunks(
                node_t_props.map(|s| Ok(s)),
                t_props_chunk_size,
                false,
            )
            .unwrap();
        (graph, test_dir)
    }
}
