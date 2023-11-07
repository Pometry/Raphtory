use arrow2::{
    array::{Array, PrimitiveArray, StructArray, Utf8Array},
    datatypes::{DataType, Field, Schema}, chunk::Chunk,
};
use itertools::Itertools;
use std:: path::{Path, PathBuf};

pub(crate) mod chunked_array;
pub mod col_graph2;
pub(crate) mod columnar_graph;
pub mod edge;
pub(crate) mod edges;
pub(crate) mod edge_frame_builder;
pub(crate) mod global_order;
pub mod graph;
pub mod ipc;
pub(crate) mod list_buffer;
pub mod loader;
pub mod mmap;
pub(crate) mod parquet_reader;
pub(crate) mod vertex_chunk;
pub(crate) mod vertex_frame_builder;

pub type Time = i64;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow2::error::Error),
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
    #[error("LMDB error: {0}")]
    LMDB(#[from] heed::Error),
    #[error("Bad data type for vertex column: {0:?}")]
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
}

unsafe impl Send for Error {} // heed::Error can't be made Send

const OUTBOUND_COLUMN: &str = "outbound";
const INBOUND_COLUMN: &str = "inbound";

const V_ADDITIONS_COLUMN: &str = "additions";
const E_ADDITIONS_COLUMN: &str = "additions";
const E_DELETIONS_COLUMN: &str = "deletions";

const NAME_COLUMN: &str = "name";
const TEMPORAL_PROPS_COLUMN: &str = "t_props";
const EDGE_OVERFLOW_COLUMN: &str = "edge_overflow";

const GID_COLUMN: &str = "global_vertex_id";
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

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum GID {
    U64(u64),
    I64(i64),
    Str(String),
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
}

pub(crate) struct PropsChunk(pub StructArray);

pub(crate) fn split_struct_chunk(
    chunk: StructArray,
    src_col_idx: usize,
    dst_col_idx: usize,
    time_col_idx: usize,
) -> (GraphChunk, PropsChunk) {
    let (fields, cols, _) = chunk.into_data();
    split_chunk(cols, src_col_idx, dst_col_idx, time_col_idx, fields.into())
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

    fn len(&self) -> usize {
        self.srcs.len()
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
