//! Python bindings for SHACL validation.
use crate::{
    python::rdf::{PyRdfView, RdfSource},
    shacl::{ShaclOptions, ShaclResult, ShaclShapes},
    RdfFormat,
};
use pyo3::{prelude::*, IntoPyObjectExt};

#[derive(IntoPyObject)]
struct PyShaclReport {
    conforms: bool,
    results: Vec<PyShaclResult>,
}

#[derive(IntoPyObject)]
struct PyShaclResult {
    focus_node: String,
    path: Option<String>,
    value: Option<String>,
    source_shape: Option<String>,
    constraint_component: String,
    severity: String,
    messages: Vec<String>,
}

impl From<ShaclResult> for PyShaclResult {
    fn from(result: ShaclResult) -> Self {
        let ShaclResult {
            focus_node,
            constraint_component,
            path,
            value,
            source_shape,
            severity,
            messages,
        } = result;
        Self {
            focus_node,
            path,
            value,
            source_shape,
            constraint_component,
            severity,
            messages,
        }
    }
}

#[pymethods]
impl PyRdfView {
    /// Validates the triples of this view (those to_rdf() writes) against a SHACL shapes graph.
    ///
    /// Arguments:
    ///     shapes (bytes | str | PathLike): The shapes graph: the document as bytes, or the path of a file to read.
    ///     format (str, optional): The RDF format of the shapes; by default the one a path's extension names, or Turtle for bytes. Defaults to None.
    ///     base_iri (str, optional): The IRI against which relative IRIs of the shapes are resolved. Defaults to None.
    ///     report_format (str, optional): If given, the report is returned as an sh:ValidationReport document in this RDF format. Defaults to None.
    ///
    /// Returns:
    ///     Union[dict[str, Any], str]: The report as a dict of conforms and results (dicts of focus_node, path, value, source_shape, constraint_component, severity and messages, terms as Raphtory names), or as a document if report_format is given.
    ///
    /// Raises:
    ///     GraphError: If the shapes are not a valid shapes graph or the validation fails.
    ///     ValueError: If the shapes do not parse or a format is unknown.
    ///     TypeError: If shapes is neither bytes nor a path.
    #[pyo3(signature = (shapes, format = None, base_iri = None, report_format = None))]
    fn validate_shacl(
        &self,
        py: Python<'_>,
        shapes: RdfSource,
        format: Option<&str>,
        base_iri: Option<&str>,
        report_format: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        let format = format.map(RdfFormat::parse).transpose()?;
        let report_format = report_format.map(RdfFormat::parse).transpose()?;
        let report = py.detach(|| {
            let shapes = match shapes {
                RdfSource::Bytes(bytes) => ShaclShapes::parse(bytes.as_ref(), format, base_iri),
                RdfSource::Path(path) => ShaclShapes::parse_path(path, format, base_iri),
            }?;
            self.view.validate_shacl(&shapes, &ShaclOptions::default())
        })?;
        let Some(report_format) = report_format else {
            return PyShaclReport {
                conforms: report.conforms(),
                results: report.results().into_iter().map(Into::into).collect(),
            }
            .into_py_any(py);
        };
        let document = py.detach(|| report.to_rdf_string(report_format))?;
        document.into_py_any(py)
    }
}
