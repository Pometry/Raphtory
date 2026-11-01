use crate::{
    arrow_loader::{
        dataframe::{DFChunk, SecondaryIndexCol, TimeCol},
        df_loaders::secondary_index_at,
        node_col::NodeCol,
        prop_handler::PropCols,
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::{properties::prop::PropRef, LayerId},
    storage::timeindex::EventTime,
};
use raphtory_core::entities::{GidRef, VID};

use super::{budget::Slot, columns::EdgeColumns};

/// A chunk of input rows on its way through the owners, shared by every message about it.
/// Dropping the last reference returns its slot to the in-flight budget.
pub(super) struct Chunk {
    pub rows: Rows,
    pub endpoints: Endpoints,
    pub times: EventTimes,
    pub layers: Vec<usize>,
    pub properties: RowProperties,
    pub _slot: Slot,
}

pub(super) struct Rows {
    pub data: DFChunk,
    pub columns: EdgeColumns,
}

pub(super) struct Endpoints {
    pub src: Vec<VID>,
    pub dst: Vec<VID>,
}

pub(super) struct EventTimes {
    pub time: TimeCol,
    pub event_ids: SecondaryIndexCol,
}

pub(super) struct RowProperties {
    pub temporal: PropCols,
    pub metadata: PropCols,
}

/// One row of a chunk, as an edge update.
#[derive(Clone, Copy)]
pub(super) struct EdgeRow {
    pub src: VID,
    pub dst: VID,
    pub time: EventTime,
    pub layer: LayerId,
}

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Endpoint {
    Src,
    Dst,
}

/// A node that resolving a chunk's endpoints created.
#[derive(Clone, Copy)]
pub(super) struct NewNode {
    pub endpoint: Endpoint,
    pub row: u32,
    pub vid: VID,
}

/// A chunk's endpoint columns, to read new nodes' ids from.
pub(super) struct NodeIds {
    src: NodeCol,
    dst: NodeCol,
}

impl Chunk {
    pub fn len(&self) -> usize {
        self.endpoints.src.len()
    }

    pub fn edge(&self, row: u32) -> EdgeRow {
        let row = row as usize;
        EdgeRow {
            src: self.endpoints.src[row],
            dst: self.endpoints.dst[row],
            time: self.times.at(row),
            layer: LayerId(self.layers[row]),
        }
    }

    pub fn temporal_properties(&self, row: u32) -> impl Iterator<Item = (usize, PropRef<'_>)> {
        self.properties.temporal.iter_row(row as usize)
    }

    pub fn metadata(&self, row: u32) -> impl Iterator<Item = (usize, PropRef<'_>)> {
        self.properties.metadata.iter_row(row as usize)
    }
}

impl Rows {
    pub fn node_ids(&self) -> Result<NodeIds, GraphError> {
        Ok(NodeIds {
            src: self.data.node_col(self.columns.src)?,
            dst: self.data.node_col(self.columns.dst)?,
        })
    }
}

impl NodeIds {
    pub fn of(&self, node: &NewNode) -> GidRef<'_> {
        let column = match node.endpoint {
            Endpoint::Src => &self.src,
            Endpoint::Dst => &self.dst,
        };
        column
            .get(node.row as usize)
            .expect("a new node's row is within its chunk")
    }
}

impl EventTimes {
    fn at(&self, row: usize) -> EventTime {
        EventTime(self.time[row], secondary_index_at(&self.event_ids, row))
    }
}
