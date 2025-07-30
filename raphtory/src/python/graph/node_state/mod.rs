mod group_by;
// mod lazy_node_state_earliest_date_time;
// mod lazy_node_state_latest_date_time;
mod lazy_node_state_history;
mod node_state;
mod node_state_history;
// mod node_state_result_option_datetime;

use crate::{
    add_classes,
    python::graph::node_state::{group_by::PyNodeGroups, lazy_node_state_history::HistoryView},
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
        NodeStateU64,
        NodeStateOptionI64,
        NodeStateOptionRaphtoryTime,
        IdView,
        NodeStateGID,
        EarliestTimeView,
        LatestTimeView,
        NameView,
        NodeStateString,
        // EarliestDateTimeView,
        // LatestDateTimeView,
        // NodeStateOptionDateTime,
        HistoryView,
        EdgeHistoryCountView,
        // NodeStateListI64,
        // HistoryDateTimeView,
        // NodeStateOptionListDateTime,
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
        NodeStateSEIR,
        NodeLayout,
        NodeStateF64String,
    );
    Ok(m)
}
