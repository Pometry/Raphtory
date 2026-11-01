use crate::{
    arrow_loader::dataframe::{DFChunk, SecondaryIndexCol},
    errors::GraphError,
};
use db4_graph::TemporalGraph;
use parking_lot::Mutex;
use std::ops::Range;
use storage::Extension;

/// The rows per chunk when the caller doesn't choose: big enough that node resolution's fixed
/// cost per call stays small, small enough to keep the pipeline busy.
pub const DEFAULT_CHUNK_ROWS: usize = 256 * 1024;

/// A run of input rows taken as one piece, in input order: either chunks already decoded, or
/// row groups of a parquet file that the reader taking the unit decodes.
pub struct InputUnit {
    pub rows: usize,
    pub decode: Box<dyn FnOnce() -> Result<Vec<DFChunk>, GraphError> + Send>,
}

impl InputUnit {
    pub fn decoded(chunk: DFChunk) -> Self {
        Self {
            rows: chunk.len(),
            decode: Box::new(move || Ok(vec![chunk])),
        }
    }
}

/// The input, shared by every reader. A unit and its event ids are taken together, so event ids
/// follow input order whichever reader decodes the unit.
pub(super) struct Input<'a, I> {
    units: Mutex<I>,
    graph: &'a TemporalGraph<Extension>,
    event_id_column: Option<usize>,
}

/// Where a unit's chunks get their event ids: the input's own column, or a range reserved for
/// the unit when it was taken.
pub(super) enum EventIds<'a> {
    FromColumn {
        column: usize,
        graph: &'a TemporalGraph<Extension>,
    },
    Reserved(Range<usize>),
}

impl<'a, I: Iterator<Item = Result<InputUnit, GraphError>>> Input<'a, I> {
    pub fn new(
        units: I,
        graph: &'a TemporalGraph<Extension>,
        event_id_column: Option<usize>,
    ) -> Self {
        Self {
            units: Mutex::new(units),
            graph,
            event_id_column,
        }
    }

    pub fn next(&self) -> Option<Result<(InputUnit, EventIds<'a>), GraphError>> {
        let mut units = self.units.lock();
        let unit = units.next()?;
        Some(unit.map(|unit| {
            let event_ids = self.event_ids_for(&unit);
            (unit, event_ids)
        }))
    }

    fn event_ids_for(&self, unit: &InputUnit) -> EventIds<'a> {
        match self.event_id_column {
            Some(column) => EventIds::FromColumn {
                column,
                graph: self.graph,
            },
            None => {
                let start = self.graph.storage().reserve_event_ids(unit.rows);
                EventIds::Reserved(start..start + unit.rows)
            }
        }
    }
}

impl EventIds<'_> {
    pub fn for_next_chunk(&mut self, chunk: &DFChunk) -> Result<SecondaryIndexCol, GraphError> {
        match self {
            EventIds::FromColumn { column, graph } => {
                let ids = chunk.secondary_index_col(*column)?;
                graph.storage().set_max_event_id(ids.max());
                Ok(ids)
            }
            EventIds::Reserved(range) => {
                let ids = range.start..range.start + chunk.len();
                if ids.end > range.end {
                    return Err(unit_size_mismatch());
                }
                range.start = ids.end;
                Ok(SecondaryIndexCol::new_from_range(ids.start, ids.end))
            }
        }
    }

    pub fn ensure_all_used(&self) -> Result<(), GraphError> {
        match self {
            EventIds::Reserved(range) if !range.is_empty() => Err(unit_size_mismatch()),
            _ => Ok(()),
        }
    }
}

fn unit_size_mismatch() -> GraphError {
    GraphError::LoadFailure(
        "an input unit decoded to a different number of rows than it reserved event ids for"
            .to_string(),
    )
}
