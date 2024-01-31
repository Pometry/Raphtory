use crate::arrow::load::parquet_reader::{NumRows, TrySlice};
use arrow2::{
    array::{Array, PrimitiveArray, StructArray, Utf8Array},
    chunk::Chunk,
    compute::concatenate::concatenate,
    datatypes::{DataType, Field, Schema},
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    num::TryFromIntError,
    ops::Range,
    path::{Path, PathBuf},
};

pub(crate) mod chunked_array;
pub mod col_graph2;
pub mod edge;
pub(crate) mod graph_builder;
// pub(crate) mod node_additions;
pub mod algorithms;
pub(crate) mod edges;
pub mod global_order;
pub mod graph;
pub mod graph_impl;
pub mod load;
pub(crate) mod nodes;
pub(crate) mod timestamps;

pub type Time = i64;

pub mod prelude {
    pub use super::chunked_array::array_ops::*;
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow2::error::Error),
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
    #[error("Unable to open database: {0:?}")]
    DatabaseNotFound(PathBuf),
    #[error("Empty parquet chunk")]
    EmptyChunk,
    #[error("Conversion error: {0}")]
    ArgumentError(#[from] TryFromIntError),
    #[error("Invalid file: {0:?}")]
    InvalidFile(PathBuf),
    #[error("Invalid metadata: {0:?}")]
    MetadataError(#[from] Box<bincode::ErrorKind>),
}

unsafe impl Send for Error {} // heed::Error can't be made Send

const SRC_COLUMN: &str = "src";
const DST_COLUMN: &str = "dst";

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
    use strum::{AsRefStr, EnumString};

    #[derive(AsRefStr, EnumString, PartialEq, Debug, Ord, PartialOrd, Eq)]
    pub enum GraphPaths {
        EdgeIds,
        NodeAdditions,
        NodeAdditionsOffsets,
        AdjOutOffsets,
        EdgeTPropsOffsets,
        EdgeTProps,
        AdjInSrcs,
        AdjInEdges,
        AdjInOffsets,
        Metadata,
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
    pub fn from_chunk(
        chunk: Chunk<Box<dyn Array>>,
        src_col_idx: usize,
        dst_col_idx: usize,
    ) -> Self {
        let srcs = chunk[src_col_idx].clone();
        let dsts = chunk[dst_col_idx].clone();
        Self { srcs, dsts }
    }

    pub fn to_chunk(&self) -> Chunk<Box<dyn Array>> {
        Chunk::new(vec![self.srcs.clone(), self.dsts.clone()])
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
    fn try_slice(&self, range: Range<usize>) -> Result<StructArray, Error> {
        self.0.try_slice(range)
    }
}

pub fn concat<A: Array + Clone>(arrays: Vec<A>) -> Result<A, Error> {
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

impl GraphChunk {
    fn sources(&self) -> Result<impl Iterator<Item = GID>, Error> {
        array_as_id_iter(&self.srcs)
    }

    fn destinations(&self) -> Result<impl Iterator<Item = GID>, Error> {
        array_as_id_iter(&self.dsts)
    }
}

fn array_as_id_iter(array: &Box<dyn Array>) -> Result<Box<dyn Iterator<Item = GID>>, Error> {
    match array.data_type() {
        DataType::UInt64 => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<u64>>()
                .ok_or_else(|| {
                    Error::InvalidTypeColumn(format!(
                        "expected u64 column, got {:?}",
                        array.data_type()
                    ))
                })?
                .clone();
            Ok(Box::new(array.into_iter().flatten().map(GID::U64)))
        }
        DataType::Int64 => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .ok_or_else(|| {
                    Error::InvalidTypeColumn(format!(
                        "expected i64 column, got {:?}",
                        array.data_type()
                    ))
                })?
                .clone();
            Ok(Box::new(array.into_iter().flatten().map(GID::I64)))
        }
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .ok_or_else(|| {
                    Error::InvalidTypeColumn(format!(
                        "expected utf8 column, got {:?}",
                        array.data_type()
                    ))
                })?
                .clone();
            Ok(Box::new(
                (0..array.len()).map(move |i| unsafe { array.value_unchecked(i) }.into()),
            ))
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<Utf8Array<i64>>()
                .ok_or_else(|| {
                    Error::InvalidTypeColumn(format!(
                        "expected large_utf8 column, got {:?}",
                        array.data_type()
                    ))
                })?
                .clone();
            Ok(Box::new(
                (0..array.len()).map(move |i| unsafe { array.value_unchecked(i) }.into()),
            ))
        }
        v => Err(Error::DType(v.clone())),
    }
}

fn prepare_graph_dir<P: AsRef<Path>>(graph_dir: P) -> Result<(), Error> {
    // create graph dir if it does not exist
    // if it exists make sure it's empty
    std::fs::create_dir_all(&graph_dir)?;

    // let mut dir_iter = std::fs::read_dir(&graph_dir)?;
    // if dir_iter.next().is_some() {
    //     return Err(Error::GraphDirNotEmpty);
    // }

    return Ok(());
}
