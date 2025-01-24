mod group_by;
mod node_state;
use crate::{add_classes, python::graph::node_state::group_by::PyNodeGroups};
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
        IdView,
        NodeStateGID,
        EarliestTimeView,
        LatestTimeView,
        NameView,
        NodeStateString,
        EarliestDateTimeView,
        LatestDateTimeView,
        NodeStateOptionDateTime,
        HistoryView,
        NodeStateListI64,
        HistoryDateTimeView,
        NodeStateOptionListDateTime,
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
    );
    Ok(m)
}
