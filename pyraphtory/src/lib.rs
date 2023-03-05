pub mod wrappers;
pub mod graph;
pub mod graph_window;
pub mod algorithms;

use pyo3::prelude::*;

use crate::wrappers::Direction;
use crate::graph::Graph;
use crate::algorithms::triangle_count;

#[pymodule]
fn pyraphtory(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Direction>()?;
    m.add_class::<Graph>()?;
    let algorithm_module = PyModule::new(py, "algorithms")?;
    algorithm_module.add_function(wrap_pyfunction!(triangle_count, algorithm_module)?)?;
    m.add_submodule(algorithm_module)?;
    Ok(())
}
