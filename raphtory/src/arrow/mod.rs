use arrow2::{
    array::{Array, PrimitiveArray, StructArray, Utf8Array},
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
};
use itertools::Itertools;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

pub(crate) mod chunked_array;
pub mod col_graph2;
pub(crate) mod columnar_graph;
pub mod edge;
pub(crate) mod edge_chunk;
pub(crate) mod edge_frame_builder;
pub(crate) mod flat_edge_frame_builder;
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

        let time_d_type = all_cols[time_col_idx].data_type().clone();
        assert_eq!(time_d_type, DataType::Int64, "time column must be i64");

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
                .insert(0, Field::new(TIME_COLUMN, time_d_type, false));
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
        if split_at == 0 {
            None
        } else {
            let t_prop_cols = self.t_prop_cols.as_mut()?;
            let out = t_prop_cols.clone().sliced(0, split_at);
            t_prop_cols.slice(split_at, t_prop_cols.len() - split_at);
            Some(out)
        }
    }

    fn len(&self) -> usize {
        self.graph_cols[0].len()
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

fn to_arrow_datatype(d_type: &DataType) -> arrow_schema::DataType {
    match d_type {
        DataType::Null => arrow_schema::DataType::Null,
        DataType::Boolean => arrow_schema::DataType::Boolean,
        DataType::Int8 => arrow_schema::DataType::Int8,
        DataType::Int16 => arrow_schema::DataType::Int16,
        DataType::Int32 => arrow_schema::DataType::Int32,
        DataType::Int64 => arrow_schema::DataType::Int64,
        DataType::UInt8 => arrow_schema::DataType::UInt8,
        DataType::UInt16 => arrow_schema::DataType::UInt16,
        DataType::UInt32 => arrow_schema::DataType::UInt32,
        DataType::UInt64 => arrow_schema::DataType::UInt64,
        DataType::Float16 => arrow_schema::DataType::Float16,
        DataType::Float32 => arrow_schema::DataType::Float32,
        DataType::Float64 => arrow_schema::DataType::Float64,
        DataType::Utf8 => arrow_schema::DataType::Utf8,
        DataType::LargeUtf8 => arrow_schema::DataType::LargeUtf8,
        DataType::List(field) => arrow_schema::DataType::List(Arc::new(to_arrow_field(&field))),
        DataType::LargeList(field) => {
            arrow_schema::DataType::LargeList(Arc::new(to_arrow_field(&field)))
        }
        DataType::Struct(fields) => {
            arrow_schema::DataType::Struct(fields.iter().map(to_arrow_field).collect())
        }
        // later
        DataType::Timestamp(_, _) => todo!(),
        DataType::Union(_, _, _) => todo!(),
        DataType::Map(_, _) => todo!(),
        DataType::Dictionary(_, _, _) => todo!(),
        DataType::Decimal(_, _) => todo!(),
        DataType::Decimal256(_, _) => todo!(),
        DataType::Extension(_, _, _) => todo!(),
        DataType::FixedSizeList(_, _) => todo!(),
        DataType::Date32 => todo!(),
        DataType::Date64 => todo!(),
        DataType::Time32(_) => todo!(),
        DataType::Time64(_) => todo!(),
        DataType::Duration(_) => todo!(),
        DataType::Interval(_) => todo!(),
        DataType::Binary => todo!(),
        DataType::FixedSizeBinary(_) => todo!(),
        DataType::LargeBinary => todo!(),
    }
}

fn to_arrow_field(field: &Field) -> arrow_schema::Field {
    let data_type = to_arrow_datatype(field.data_type());
    arrow_schema::Field::new(&field.name, data_type, field.is_nullable)
}

fn to_arrow_schema(schema: &Schema) -> arrow_schema::SchemaRef {
    let fields: Vec<_> = schema.fields.iter().map(to_arrow_field).collect();
    Arc::new(arrow_schema::Schema::new(fields))
}
