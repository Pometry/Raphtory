//! Python bindings for SHACL validation: `validate_shacl` on `RdfView`. Like `sparql()`, the
//! results are plain dicts and lists, with RDF terms given as the Raphtory names they stand for
//! (see [`ShaclResult::names`](crate::shacl::ShaclResult::names)), so a focus node can be passed
//! straight to `graph.node(..)`.
use crate::{
    python::rdf::{PyRdfView, RdfSource},
    shacl::{ShaclOptions, ShaclResultNames, ShaclShapes, ShaclWarning},
    RdfFormat,
};
use pyo3::{prelude::*, IntoPyObjectExt};

#[derive(IntoPyObject)]
struct PyShaclReport {
    conforms: bool,
    warnings: Vec<ShaclWarning>,
    results: Vec<ShaclResultNames>,
}

#[pymethods]
impl PyRdfView {
    /// Validates the RDF triples of this view against a SHACL shapes graph.
    ///
    /// The data graph is the triples to_rdf() writes and sparql() matches: one per edge and
    /// layer visible in this view, so rg.valid().validate_shacl(..) validates the current state
    /// and rg.at(t).validate_shacl(..) the state as of t. Node and layer names are RDF terms as
    /// described in sparql(): the node Alice of a graph that was not loaded from RDF is the IRI
    /// raphtory:Alice, which shapes can name with @prefix raphtory: <raphtory:> .
    ///
    /// SHACL Core is supported, and shapes that use other parts of SHACL raise an error.
    ///
    /// The report is a dict with conforms (bool, True if there are no results of any severity),
    /// results (a list of dicts with focus_node, path, value, source_shape, constraint_component,
    /// severity and messages) and warnings (a list[ShaclWarning], holding
    /// ShaclWarning.PartialSubclassReasoning if partial subclass reasoning may affect the results
    /// and ShaclWarning.RecursiveShapes if recursive shapes may). Terms are Raphtory names, as in
    /// sparql(): focus_node and value can be passed to graph.node(), path is the predicate's
    /// name, as sparql() returns it, for a single predicate and otherwise a SPARQL property path
    /// such as (<http://ex/a> / <http://ex/b>), constraint_component and severity are SHACL IRIs
    /// such as http://www.w3.org/ns/shacl#MinCountConstraintComponent, and messages are the
    /// values of the result messages. path, value and source_shape are None when the result has
    /// none. Results are in a deterministic order, and shape blank nodes get the same labels each
    /// time. A literal the validator rewrites in canonical form (such as "1"^^xsd:boolean as
    /// "true"^^xsd:boolean) comes back as written in the graph when that is unambiguous, and
    /// otherwise as rewritten, which graph.node() may not find.
    ///
    /// Arguments:
    ///     shapes (bytes | str | PathLike): The shapes graph: the document as bytes, or the path of a file to read.
    ///     format (str, optional): The RDF format of the shapes, as a file extension, name or media type (such as "ttl", "nt", "jsonld" or "rdf"). If not given, it is taken from the extension of a path (a missing or unknown extension raises an error), and is Turtle for bytes. Defaults to None.
    ///     base_iri (str, optional): The IRI against which relative IRIs of the shapes are resolved. Defaults to None.
    ///     report_format (str, optional): If given, the report is returned as a W3C sh:ValidationReport document in this RDF format (such as "ttl", "nt", "jsonld" or "rdf"), holding RDF terms rather than Raphtory names, and without the warnings. RDF/XML ("rdf") raises a GraphError if the report has a literal with a control character other than tab and line feed. Defaults to None.
    ///
    /// Returns:
    ///     Union[dict[str, Any], str]: The report as a dict, or as a document if report_format is given.
    ///
    /// Raises:
    ///     GraphError: If the shapes use an unsupported SHACL feature or are not a valid shapes graph, if validation fails, or if RDF/XML cannot hold the report.
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
                conforms: report.conforms,
                warnings: report.warnings,
                results: report.results.iter().map(|result| result.names()).collect(),
            }
            .into_py_any(py);
        };
        let document = py.detach(|| report.to_rdf_string(report_format))?;
        document.into_py_any(py)
    }
}
