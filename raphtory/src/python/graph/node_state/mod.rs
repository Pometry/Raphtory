mod group_by;
mod output_node_state;
mod node_state;
mod node_state_earliest_time;
mod node_state_history;
mod node_state_intervals;
mod node_state_latest_time;

use crate::{
    add_classes,
    python::{
        graph::node_state::{
            group_by::PyNodeGroups,
            node_state_earliest_time::{
                EarliestDateTimeView, EarliestEventIdView, EarliestTimeView, EarliestTimestampView,
            },
            node_state_history::{
                HistoryDateTimeView, HistoryEventIdView, HistoryTimestampView, HistoryView,
                NodeStateHistory, NodeStateHistoryDateTime, NodeStateHistoryEventId,
                NodeStateHistoryTimestamp,
            },
            node_state_intervals::{
                IntervalsFloatView, IntervalsIntegerView, IntervalsView, NodeStateIntervals,
            },
            node_state_latest_time::{
                LatestDateTimeView, LatestEventIdView, LatestTimeView, LatestTimestampView,
            },
        },
        types::wrappers::iterables::UsizeIterable,
    },
};
pub use node_state::*;
pub use output_node_state::*;
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
        IntervalsFloatView,
        IntervalsIntegerView,
        EdgeHistoryCountView,
        UsizeIterable,
        NodeTypeView,
        NodeStateOptionStr,
        NodeStateListDateTime,
        NodeStateWeightedSP,
        NodeStateF64,
        NodeStateOptionF64,
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
