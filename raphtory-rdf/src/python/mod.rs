mod rdf;
mod shacl;

use crate::shacl::ShaclWarning;
use pyo3::prelude::*;
use rdf::{PyRdfGraph, PyRdfView};

/// Adds the RDF classes (`RdfView`, `RdfGraph` and `ShaclWarning`) to `m`.
pub fn add_rdf_classes(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyRdfView>()?;
    m.add_class::<PyRdfGraph>()?;
    m.add_class::<ShaclWarning>()?;
    Ok(())
}
