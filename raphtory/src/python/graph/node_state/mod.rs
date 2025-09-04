mod group_by;
mod lazy_node_state_earliest_date_time;
mod lazy_node_state_history;
mod lazy_node_state_latest_date_time;
mod node_state;
mod node_state_history;

use crate::{
    add_classes,
    python::{
        graph::node_state::{
            group_by::PyNodeGroups, lazy_node_state_earliest_date_time::EarliestDateTimeView,
            lazy_node_state_history::HistoryView,
            lazy_node_state_latest_date_time::LatestDateTimeView,
            node_state_history::NodeStateHistory,
        },
        types::wrappers::iterables::UsizeIterable,
    },
};
pub use node_state::*;
use pyo3::prelude::*;

pub fn base_node_state_module(py: Python<'_>) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "node_state")?;
    add_classes!(
        &m,
        PyNodeGroups,
        DegreeView,
        NodeStateUsize,
        NodeStateOptionUsize,
        NodeStateU64,
        NodeStateOptionI64,
        NodeStateOptionTimeIndexEntry,
        NodeStateOptionDateTime,
        IdView,
        NodeStateGID,
        EarliestTimeView,
        EarliestTimestampView,
        EarliestSecondaryIndexView,
        EarliestDateTimeView,
        LatestTimeView,
        LatestTimestampView,
        LatestSecondaryIndexView,
        LatestDateTimeView,
        NameView,
        NodeStateString,
        HistoryView,
        HistoryTimestampView,
        HistoryDateTimeView,
        HistorySecondaryIndexView,
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
        NodeStateHistorySecondaryIndex,
        NodeStateIntervals,
        NodeStateSEIR,
        NodeLayout,
        NodeStateF64String,
    );
    Ok(m)
}
