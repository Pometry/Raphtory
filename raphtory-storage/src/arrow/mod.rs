use arrow2::{
    array::{Array, PrimitiveArray, StructArray, Utf8Array},
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
};
use itertools::Itertools;

pub mod col_graph2;
pub(crate) mod columnar_graph;
pub(crate) mod edge_chunk;
pub(crate) mod vertex_chunk;
pub(crate) mod edge_frame_builder;
pub(crate) mod list_buffer;
pub(crate) mod mmap;
pub(crate) mod vertex_frame_builder;

pub type Time = i64;

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

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
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
    graph_cols: Vec<Box<dyn Array>>,
    t_prop_cols: Option<StructArray>,
}

impl LoadChunk {
    pub(crate) fn new<I: IntoIterator<Item = Box<dyn Array>>>(
        columns_in_chunk: I,
        src_col_idx: usize,
        dst_col_idx: usize,
        time_col_idx: usize,
        chunk_schema: Schema,
    ) -> Self {
        let mut temporal_props = vec![];

        let all_cols = columns_in_chunk.into_iter().collect_vec();

        // shove time as the first column in temporal_props
        temporal_props.push(all_cols[time_col_idx].clone());

        for (i, column) in all_cols.iter().enumerate() {
            if !(i == src_col_idx || i == dst_col_idx || i == time_col_idx) {
                temporal_props.push(column.clone());
            }
        }

        let mut graph_cols =
            vec![PrimitiveArray::<i32>::from_vec(Vec::with_capacity(0)).boxed(); 3];

        for (i, col) in all_cols.into_iter().enumerate() {
            if i == src_col_idx {
                graph_cols[0] = col;
            } else if i == dst_col_idx {
                graph_cols[1] = col;
            } else if i == time_col_idx {
                graph_cols[2] = col;
            }
        }

        let first_len = graph_cols.first().unwrap().len();
        if graph_cols.iter().any(|arr| arr.len() != first_len) {
            panic!("All arrays in a chunk must have the same length");
        }

        if temporal_props.iter().any(|arr| arr.len() != first_len) {
            panic!("All arrays in a chunk must have the same length");
        }

        if temporal_props.is_empty() {
            Self {
                graph_cols,
                t_prop_cols: None,
            }
        } else {
            let mut props_only_schema = chunk_schema
                .filter(|i, _| !(i == src_col_idx || i == dst_col_idx || i == time_col_idx));
            // put time as the first column in the struct
            props_only_schema
                .fields
                .insert(0, Field::new(TIME_COLUMN, DataType::Int64, false));
            let data_type = DataType::Struct(props_only_schema.fields);
            let t_prop_cols = Some(StructArray::new(data_type, temporal_props, None));
            Self {
                graph_cols,
                t_prop_cols,
            }
        }
    }

    pub(crate) fn from_chunk(
        chunk: Chunk<Box<dyn Array>>,
        src_col_idx: usize,
        dst_col_idx: usize,
        time_col_idx: usize,
        chunk_schema: Schema,
    ) -> Self {
        Self::new(
            chunk.into_arrays(),
            src_col_idx,
            dst_col_idx,
            time_col_idx,
            chunk_schema,
        )
    }

    fn sources(&self) -> Result<impl Iterator<Item = GID>, Error> {
        array_as_id_iter(&self.graph_cols[0])
    }

    fn destinations(&self) -> Result<impl Iterator<Item = GID>, Error> {
        array_as_id_iter(&self.graph_cols[1])
    }

    fn t_prop_cols(&self) -> Option<&StructArray> {
        self.t_prop_cols.as_ref()
    }

    fn timestamp_arr(&self) -> Result<PrimitiveArray<i64>, Error> {
        let arr = &self.graph_cols[2];
        let times = arr
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .ok_or_else(|| {
                Error::InvalidTypeColumn(format!("expected i64 column, got {:?}", arr.data_type()))
            })?
            .clone();
        Ok(times)
    }

    fn split_timestamps_at(&mut self, split_at: usize) -> PrimitiveArray<i64> {
        let time_arr = &mut self.graph_cols[2];
        let time_arr = time_arr
            .as_any_mut()
            .downcast_mut::<PrimitiveArray<i64>>()
            .unwrap();
        let out = time_arr.clone().sliced(0, split_at);
        time_arr.slice(split_at, time_arr.len() - split_at);
        out
    }

    fn split_t_props_at(&mut self, split_at: usize) -> Option<StructArray> {
        let t_prop_cols = self.t_prop_cols.as_mut()?;
        let out = t_prop_cols.clone().sliced(0, split_at);
        t_prop_cols.slice(split_at, t_prop_cols.len() - split_at);
        Some(out)
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
