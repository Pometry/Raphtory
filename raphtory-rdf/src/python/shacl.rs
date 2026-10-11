//! Python bindings for SHACL validation (feature `shacl`): `validate_shacl` and
//! `validate_shacl_at` on `RdfView`. Like `sparql()`, the results are plain dicts and lists, with
//! RDF terms given as the Raphtory names they stand for (see [`term_name`]), so a focus node can be
//! passed straight to `graph.node(..)`.
use crate::{
    parse_rdf_format,
    python::rdf::{term_name, PyRdfView, RdfSource},
    shacl::{ShaclPath, ShaclReport, ShaclShapes, ShaclWarning},
    RdfError,
};
use pyo3::{prelude::*, IntoPyObjectExt};
use raphtory::{db::api::view::TimeOps, python::types::iterable::FromIterable};
use raphtory_api::core::storage::timeindex::EventTime;

#[derive(IntoPyObject)]
struct PyShaclReport {
    conforms: bool,
    warnings: Vec<ShaclWarning>,
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

// The Python form of a path: the name of its predicate (its IRI) for a predicate path, otherwise
// the path in SPARQL property path syntax.
fn path_name(path: &ShaclPath) -> String {
    match path {
        ShaclPath::Predicate(predicate) => term_name(predicate.as_ref().into()),
        path => path.to_string(),
    }
}

impl From<&ShaclReport> for PyShaclReport {
    fn from(report: &ShaclReport) -> Self {
        Self {
            conforms: report.conforms,
            warnings: report.warnings.clone(),
            results: report
                .results
                .iter()
                .map(|result| PyShaclResult {
                    focus_node: term_name(result.focus_node.as_ref()),
                    path: result.path.as_ref().map(path_name),
                    value: result.value.as_ref().map(|value| term_name(value.as_ref())),
                    source_shape: result
                        .source_shape
                        .as_ref()
                        .map(|shape| term_name(shape.as_ref())),
                    constraint_component: term_name(result.constraint_component.as_ref().into()),
                    severity: term_name(result.severity.as_ref().into()),
                    messages: result
                        .messages
                        .iter()
                        .map(|message| message.value().to_owned())
                        .collect(),
                })
                .collect(),
        }
    }
}

// Parses the shapes graph without the GIL.
fn parse_shapes(
    py: Python<'_>,
    shapes: RdfSource,
    format: Option<&str>,
    base_iri: Option<&str>,
) -> PyResult<ShaclShapes> {
    let format = shapes.format(format)?;
    Ok(py.detach(|| shapes.read(|data| ShaclShapes::parse(data, format, base_iri)))?)
}

#[pymethods]
impl PyRdfView {
    /// Validates the RDF triples of this view against a SHACL shapes graph.
    ///
    /// The data graph is the triples to_rdf() writes and sparql() matches: one per edge and
    /// layer visible in this view, so rg.valid().validate_shacl(..) validates the current state
    /// and rg.at(t).validate_shacl(..) the state as of t (see also validate_shacl_at()). Node and
    /// layer names are RDF terms as described in sparql(): the node Alice of a graph that was not
    /// loaded from RDF is the IRI raphtory:Alice, which shapes can name with
    /// @prefix raphtory: <raphtory:> .
    ///
    /// SHACL Core is supported, and shapes that use other parts of SHACL raise an error. The user
    /// guide page Validating with SHACL describes what is supported and how the validator
    /// deviates from SHACL.
    ///
    /// The report is a dict with conforms (bool, True if there are no results of any severity),
    /// results (a list of dicts with focus_node, path, value, source_shape, constraint_component,
    /// severity and messages) and warnings (a list[ShaclWarning], holding
    /// ShaclWarning.PartialSubclassReasoning if partial subclass reasoning may affect the results
    /// and ShaclWarning.RecursiveShapes if recursive shapes may). Terms are Raphtory names, as in
    /// sparql(): focus_node and value can be passed to graph.node(), path is the predicate's IRI
    /// for a single predicate and otherwise a SPARQL property path such as
    /// (<http://ex/a> / <http://ex/b>), constraint_component and severity are SHACL IRIs such as
    /// http://www.w3.org/ns/shacl#MinCountConstraintComponent, and messages are the values of
    /// the result messages. path, value and source_shape are None when the result has none.
    /// Results are in a deterministic order, and shape blank nodes get the same labels each time.
    /// A literal the validator rewrites in canonical form (such as "1"^^xsd:boolean as
    /// "true"^^xsd:boolean) comes back as written in the graph when that is unambiguous, and
    /// otherwise as rewritten, which graph.node() may not find.
    ///
    /// Arguments:
    ///     shapes (bytes | str | PathLike): The shapes graph: the document as bytes, or the path of a file to read.
    ///     format (str, optional): The RDF format of the shapes, as a file extension, name or media type (such as "ttl", "nt", "jsonld" or "rdf"). If not given, it is taken from the extension of a path (an unknown extension raises an error), and is Turtle for bytes and for a path without an extension. Defaults to None.
    ///     base_iri (str, optional): The IRI against which relative IRIs of the shapes are resolved. Defaults to None.
    ///     report_format (str, optional): If given, the report is returned as a W3C sh:ValidationReport document in this RDF format (such as "ttl", "nt", "jsonld" or "rdf"), holding RDF terms rather than Raphtory names, and without the warnings. RDF/XML ("rdf") raises a GraphError if the report has a literal with a control character other than tab and line feed. Defaults to None.
    ///
    /// Returns:
    ///     dict[str, Any] | str: The report as a dict, or as a document if report_format is given.
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
        let report_format = report_format.map(parse_rdf_format).transpose()?;
        let shapes = parse_shapes(py, shapes, format, base_iri)?;
        let view = &self.view;
        let Some(report_format) = report_format else {
            let report = py.detach(|| {
                view.validate_shacl(&shapes)
                    .map(|report| PyShaclReport::from(&report))
            })?;
            return report.into_py_any(py);
        };
        let document = py.detach(|| {
            let report = view.validate_shacl(&shapes)?;
            let mut document = Vec::new();
            report.write(&mut document, report_format, &[])?;
            String::from_utf8(document).map_err(RdfError::from)
        })?;
        document.into_py_any(py)
    }

    /// Validates this view as of each of several times against a SHACL shapes graph.
    ///
    /// For each time t, in the order given, validates snapshot_at(t) as validate_shacl() does,
    /// parsing the shapes once. That is the state as of t, so this tells when the data started or
    /// stopped conforming. On a view with valid() each snapshot holds only the triples that
    /// still hold at the end of the view.
    ///
    /// Arguments:
    ///     shapes (bytes | str | PathLike): The shapes graph: the document as bytes, or the path of a file to read.
    ///     times (list[TimeInput]): The times to validate at.
    ///     format (str, optional): The RDF format of the shapes, as in validate_shacl(). Defaults to None.
    ///     base_iri (str, optional): The IRI against which relative IRIs of the shapes are resolved. Defaults to None.
    ///
    /// Returns:
    ///     list[tuple[EventTime, dict[str, Any]]]: Each time, as given, with the report of validate_shacl() for it.
    ///
    /// Raises:
    ///     GraphError: If the shapes use an unsupported SHACL feature or are not a valid shapes graph, or if validation fails.
    ///     ValueError: If the shapes do not parse or the format is unknown.
    ///     TypeError: If shapes is neither bytes nor a path.
    #[pyo3(signature = (shapes, times, format = None, base_iri = None))]
    fn validate_shacl_at(
        &self,
        py: Python<'_>,
        shapes: RdfSource,
        times: FromIterable<EventTime>,
        format: Option<&str>,
        base_iri: Option<&str>,
    ) -> PyResult<Vec<(EventTime, PyShaclReport)>> {
        let shapes = parse_shapes(py, shapes, format, base_iri)?;
        let view = &self.view;
        Ok(py.detach(|| {
            times
                .iter()
                .map(|&t| {
                    let report = view.snapshot_at(t).validate_shacl(&shapes)?;
                    Ok((t, PyShaclReport::from(&report)))
                })
                .collect::<Result<Vec<_>, RdfError>>()
        })?)
    }
}
