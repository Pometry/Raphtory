use arrow2::datatypes::{DataType, Field};

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

pub const ADJ_SCHEMA: DataType = DataType::Struct(vec![
    Field::new(V_COLUMN, DataType::UInt64, false),
    Field::new(E_COLUMN, DataType::UInt64, false),
]);

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
