use arrow2::{
    array::{Array, MutablePrimitiveArray, PrimitiveArray, Utf8Array},
    chunk::Chunk,
    datatypes::{DataType, Field},
};

pub mod col_graph2;
pub(crate) mod columnar_graph;
pub(crate) mod edge_frame_builder;
pub(crate) mod mmap;
pub(crate) mod vertex_frame_builder;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow2::error::Error),
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
    #[error("Bad data type for vertex column: {0:?}")]
    DType(DataType),
    #[error("Graph directory is not empty before loading")]
    GraphDirNotEmpty,
    #[error("Invalid type for column: {0}")]
    InvalidTypeColumn(String),
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
}

const OUTBOUND_COLUMN: &str = "outbound";
const INBOUND_COLUMN: &str = "inbound";

const V_ADDITIONS_COLUMN: &str = "additions";
const E_ADDITIONS_COLUMN: &str = "additions";
const E_DELETIONS_COLUMN: &str = "deletions";

const NAME_COLUMN: &str = "name";
const TEMPORAL_PROPS_COLUMN: &str = "t_props";

const GID_COLUMN: &str = "global_vertex_id";
const SRC_COLUMN: &str = "src";
const DST_COLUMN: &str = "dst";

pub(crate) const V_COLUMN: &str = "v";
pub(crate) const E_COLUMN: &str = "e";

#[inline]
pub fn adj_schema() -> DataType {
    DataType::Struct(vec![
        Field::new(V_COLUMN, DataType::UInt64, false),
        Field::new(E_COLUMN, DataType::UInt64, false),
    ])
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum GID {
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

pub(crate) struct LoadChunk {
    data: Chunk<Box<dyn Array>>,
    src_col_idx: usize,
    dst_col_idx: usize,
    time_col_idx: usize,
}

impl LoadChunk {
    pub(crate) fn new<I: IntoIterator<Item = Box<dyn Array>>>(
        columns_in_chunk: I,
        src_col_idx: usize,
        dst_col_idx: usize,
        time_col_idx: usize,
    ) -> Self {
        Self {
            data: Chunk::new(columns_in_chunk.into_iter().collect()),
            src_col_idx,
            dst_col_idx,
            time_col_idx,
        }
    }

    fn sources(&self) -> Result<impl Iterator<Item = GID>, Error> {
        array_as_id_iter(&self.data[self.src_col_idx])
    }

    fn destinations(&self) -> Result<impl Iterator<Item = GID>, Error> {
        array_as_id_iter(&self.data[self.dst_col_idx])
    }

    fn timestamps(&self) -> Result<impl Iterator<Item = i64>, Error> {
        let arr = &self.data[self.time_col_idx];
        let times = arr
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .ok_or_else(|| {
                Error::InvalidTypeColumn(format!("expected i64 column, got {:?}", arr.data_type()))
            })?
            .clone();
        Ok(times.into_iter().flatten())
    }

    fn timestamp_arr(&self) -> Result<PrimitiveArray<i64>, Error> {
        let arr = &self.data[self.time_col_idx];
        let times = arr
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .ok_or_else(|| {
                Error::InvalidTypeColumn(format!("expected i64 column, got {:?}", arr.data_type()))
            })?
            .clone();
        Ok(times)
    }

    fn properties(&self) -> impl Iterator<Item = Box<dyn Array>> + '_ {
        self.data
            .iter()
            .enumerate()
            .filter(|(i, _)| {
                *i != self.src_col_idx && *i != self.dst_col_idx && *i != self.time_col_idx
            })
            .map(|(_, col)| col.clone())
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
