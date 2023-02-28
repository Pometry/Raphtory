pub mod wrappers;
pub mod graph;
pub mod graph_window;

use pyo3::prelude::*;

use crate::wrappers::Direction;
use crate::wrappers::TEdge;
use crate::graph::Graph;

#[pymodule]
fn pyraphtory(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Direction>()?;
    m.add_class::<Graph>()?;
    m.add_class::<TEdge>()?;
    Ok(())
}
