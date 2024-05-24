use std::{
    borrow::Cow,
    num::TryFromIntError,
    ops::Range,
    path::{Path, PathBuf},
};

use crate::arrow2::{
    array::{Array, StructArray},
    compute::concatenate::concatenate,
    datatypes::{ArrowDataType as DataType, ArrowSchema as Schema, Field},
};
use itertools::Itertools;
use num_traits::ToPrimitive;
use polars_arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::{
    arrow2::legacy::error,
    load::parquet_reader::{NumRows, TrySlice},
};

pub mod algorithms;
pub mod arrow_hmap;
pub mod chunked_array;
pub mod edge;
pub mod edges;
pub mod global_order;
pub mod graph;
pub mod graph_builder;
pub mod graph_fragment;
pub mod interop;
pub mod load;
pub mod nodes;
pub mod properties;
pub mod timestamps;
pub mod tprops;

mod compute;

pub type Time = i64;

pub mod prelude {
    pub use super::chunked_array::array_ops::*;
}

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
    #[error("Graph directory is not empty before loading")]
    GraphDirNotEmpty,
    #[error("Invalid type for column: {0}")]
    InvalidTypeColumn(String),
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    #[error("No Edge lists found in input path")]
    NoEdgeLists,
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
}

const TIME_COLUMN: &str = "rap_time";
const TIME_COLUMN_IDX: usize = 0;

pub(crate) const V_COLUMN: &str = "v";
pub(crate) const E_COLUMN: &str = "e";

#[inline]
pub fn adj_schema() -> DataType {
    DataType::Struct(vec![
        Field::new(V_COLUMN, DataType::UInt64, false),
        Field::new(E_COLUMN, DataType::UInt64, false),
    ])
}

pub(crate) mod file_prefix {
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
        AdjOutSrcs,
        AdjOutDsts,
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
            make_path(location_path, prefix, id)
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

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub enum GID {
    U64(u64),
    I64(i64),
    Str(String),
}

impl GID {
    pub fn into_str(self) -> Option<String> {
        match self {
            GID::Str(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_i64(self) -> Option<i64> {
        match self {
            GID::I64(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_u64(self) -> Option<u64> {
        match self {
            GID::U64(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            GID::Str(v) => Some(v.as_str()),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            GID::I64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            GID::U64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn to_str(&self) -> Cow<String> {
        match self {
            GID::U64(v) => Cow::Owned(v.to_string()),
            GID::I64(v) => Cow::Owned(v.to_string()),
            GID::Str(v) => Cow::Borrowed(v),
        }
    }

    pub fn to_i64(&self) -> Option<i64> {
        match self {
            GID::U64(v) => v.to_i64(),
            GID::I64(v) => Some(*v),
            GID::Str(v) => parse_u64_strict(v)?.to_i64(),
        }
    }

    pub fn to_u64(&self) -> Option<u64> {
        match self {
            GID::U64(v) => Some(*v),
            GID::I64(v) => v.to_u64(),
            GID::Str(v) => parse_u64_strict(v),
        }
    }
}

const MAX_U64_BYTES: [u8; 20] = [
    49, 56, 52, 52, 54, 55, 52, 52, 48, 55, 51, 55, 48, 57, 53, 53, 49, 54, 49, 53,
];

pub fn parse_u64_strict(input: &str) -> Option<u64> {
    if input.len() > 20 {
        // a u64 string has at most 20 bytes
        return None;
    }
    let byte_0 = b'0';
    let byte_1 = b'1';
    let byte_9 = b'9';
    let mut input_iter = input.bytes();
    let first = input_iter.next()?;
    if first == byte_0 {
        return input_iter.next().is_none().then_some(0);
    }
    if input.len() == 20 && (byte_1..=MAX_U64_BYTES[0]).contains(&first) {
        let mut result = (first - byte_0) as u64;
        for (next_byte, max_byte) in input_iter.zip(MAX_U64_BYTES[1..].iter().copied()) {
            if !(byte_0..=max_byte).contains(&next_byte) {
                return None;
            }
            result = result * 10 + (next_byte - byte_0) as u64;
        }
        return Some(result);
    }
    if (byte_1..=byte_9).contains(&first) {
        let mut result = (first - byte_0) as u64;
        for next_byte in input_iter {
            if !(byte_0..=byte_9).contains(&next_byte) {
                return None;
            }
            result = result * 10 + (next_byte - byte_0) as u64;
        }
        return Some(result);
    }

    None
}

impl From<u64> for GID {
    fn from(id: u64) -> Self {
        Self::U64(id)
    }
}

impl From<i64> for GID {
    fn from(id: i64) -> Self {
        Self::I64(id)
    }
}

impl From<String> for GID {
    fn from(id: String) -> Self {
        Self::Str(id)
    }
}

impl From<&str> for GID {
    fn from(id: &str) -> Self {
        Self::Str(id.to_string())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GraphChunk {
    srcs: Box<dyn Array>,
    dsts: Box<dyn Array>,
}

impl GraphChunk {
    pub fn to_chunk(&self) -> RecordBatch<Box<dyn Array>> {
        RecordBatch::new(vec![self.srcs.clone(), self.dsts.clone()])
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
            GID::I64(x) => s.write_i64(*x),
            GID::Str(t) => {
                t.chars().for_each(|c| s.write_u8(c as u8));
            }
        }
        s.finish() as i64
    }
}

pub use polars_arrow as arrow2;
