mod rdf;
#[cfg(feature = "shacl")]
mod shacl;

use pyo3::prelude::*;
use rdf::{PyRdfGraph, PyRdfView};

/// Adds the RDF classes (`RdfView` and `RdfGraph`) to `m`.
pub fn add_rdf_classes(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyRdfView>()?;
    m.add_class::<PyRdfGraph>()?;
    Ok(())
}
