mod group_by;
mod node_state;
mod node_state_earliest_date_time;
mod node_state_history;
mod node_state_latest_date_time;

use crate::{
    add_classes,
    python::{
        graph::node_state::{
            group_by::PyNodeGroups,
            node_state_earliest_date_time::EarliestDateTimeView,
            node_state_history::{HistoryView, NodeStateHistory},
            node_state_latest_date_time::LatestDateTimeView,
        },
        types::wrappers::iterables::UsizeIterable,
    },
};
pub use node_state::*;
use pyo3::prelude::*;

pub fn base_node_state_module(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    let m = PyModule::new(py, "node_state")?;
    add_classes!(
        &m,
        PyNodeGroups,
        DegreeView,
        NodeStateUsize,
        NodeStateOptionUsize,
        NodeStateU64,
        NodeStateOptionI64,
        NodeStateOptionEventTime,
        NodeStateOptionDateTime,
        IdView,
        NodeStateGID,
        EarliestTimeView,
        EarliestTimestampView,
        EarliestEventIdView,
        EarliestDateTimeView,
        LatestTimeView,
        LatestTimestampView,
        LatestEventIdView,
        LatestDateTimeView,
        NameView,
        NodeStateString,
        HistoryView,
        HistoryTimestampView,
        HistoryDateTimeView,
        HistoryEventIdView,
        IntervalsView,
        EdgeHistoryCountView,
        UsizeIterable,
        NodeTypeView,
        NodeStateOptionStr,
        NodeStateListDateTime,
        NodeStateWeightedSP,
        NodeStateF64,
        NodeStateNodes,
        NodeStateReachability,
        NodeStateListF64,
        NodeStateMotifs,
        NodeStateHits,
        NodeStateHistory,
        NodeStateHistoryTimestamp,
        NodeStateHistoryDateTime,
        NodeStateHistoryEventId,
        NodeStateIntervals,
        NodeStateSEIR,
        NodeLayout,
        NodeStateF64String,
    );
    Ok(m)
}
