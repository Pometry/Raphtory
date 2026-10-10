//! Python bindings for SHACL validation (feature `shacl`): `validate_shacl` and
//! `validate_shacl_at` on `RdfView`. Like `sparql()`, the results are plain dicts and lists, with
//! RDF terms given as the Raphtory names they stand for (see [`term_name`]), so a focus node can be
//! passed straight to `graph.node(..)`.
use crate::{
    errors::GraphError,
    python::{
        packages::rdf::{term_name, PyRdfView, RdfSource},
        types::iterable::FromIterable,
    },
    rdf::{
        parse_rdf_format,
        shacl::{ShaclPath, ShaclReport, ShaclShapes},
    },
};
use pyo3::{prelude::*, IntoPyObjectExt};
use raphtory_api::core::storage::timeindex::EventTime;

/// A SHACL report as the dict Python gets.
#[derive(IntoPyObject)]
struct PyShaclReport {
    conforms: bool,
    warnings: Vec<String>,
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

/// The Python form of a path: the name of its predicate (its IRI) for a predicate path, otherwise
/// the path in SPARQL property path syntax.
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
            warnings: report.warnings.iter().map(ToString::to_string).collect(),
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

/// Parses the shapes graph `shapes` (bytes, or the path of a file) without the GIL. A `shapes`
/// that is neither raises the `TypeError` of [`RdfSource::extract`].
fn parse_shapes(
    shapes: &Bound<'_, PyAny>,
    format: Option<&str>,
    base_iri: Option<&str>,
) -> PyResult<ShaclShapes> {
    let document = RdfSource::extract(shapes)?;
    let format = document.format(format).map_err(GraphError::from)?;
    Ok(shapes
        .py()
        .detach(|| document.read(|data| ShaclShapes::parse(data, format, base_iri)))?)
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
    /// All of SHACL Core is supported. Shapes raise an error if they use SHACL-SPARQL, SHACL
    /// Advanced Features, SHACL-JS, sh:entailment, SHACL 1.2 features, the inverse of a path that
    /// is not a predicate (write ^(ex:p / ex:q) as (^ex:q / ^ex:p)), or a literal in sh:hasValue
    /// or sh:in that the validator would rewrite (such as "1"^^xsd:boolean). Other unknown sh:
    /// terms are ignored. Shapes with an RDF list of more than 10,000 members, or nested more
    /// than 256 deep, raise an error too. owl:imports is not followed.
    ///
    /// sh:targetClass targets direct rdf:type instances only, and implicit class targets and
    /// sh:class follow a single rdfs:subClassOf step (the report warns if the data has
    /// rdfs:subClassOf triples). Recursive shapes have least-fixpoint semantics: a node whose
    /// conformance depends on itself through a cycle is reported, with a warning.
    ///
    /// The report is a dict with conforms (bool, True if there are no results of any severity),
    /// results (a list of dicts with focus_node, path, value, source_shape, constraint_component,
    /// severity and messages) and warnings (a list of str). Terms are Raphtory names, as in
    /// sparql(): focus_node and value can be passed to graph.node(), path is the predicate's IRI
    /// for a single predicate and otherwise a SPARQL property path such as
    /// (<http://ex/a> / <http://ex/b>), constraint_component and severity are SHACL IRIs such as
    /// http://www.w3.org/ns/shacl#MinCountConstraintComponent, and messages are the values of
    /// the result messages. path, value and source_shape are None when the result has none.
    /// Results are in a deterministic order, and shape blank nodes get the same labels each time.
    /// Some literals are rewritten in canonical form (booleans, date-times, integer types other
    /// than xsd:integer and xsd:int, region language tags); they come back as written in the
    /// graph when unambiguous, otherwise as rewritten, which graph.node() may not find.
    ///
    /// Validation releases the GIL.
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
    ///     GraphError: If the shapes do not parse, use an unsupported SHACL feature or are not a valid shapes graph, if a format is unknown, if validation fails, or if RDF/XML cannot hold the report.
    ///     TypeError: If shapes is neither bytes nor a path, or is a str that looks like a document rather than a path.
    #[pyo3(signature = (shapes, format = None, base_iri = None, report_format = None))]
    fn validate_shacl(
        &self,
        py: Python<'_>,
        shapes: &Bound<'_, PyAny>,
        format: Option<&str>,
        base_iri: Option<&str>,
        report_format: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        let report_format = report_format
            .map(parse_rdf_format)
            .transpose()
            .map_err(GraphError::from)?;
        let shapes = parse_shapes(shapes, format, base_iri)?;
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
            String::from_utf8(document).map_err(|error| {
                GraphError::IOErrorMsg(format!("SHACL report is not UTF-8: {error}"))
            })
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
    ///     GraphError: If the shapes do not parse, use an unsupported SHACL feature or are not a valid shapes graph, if the format is unknown, or if validation fails.
    ///     TypeError: If shapes is neither bytes nor a path, or is a str that looks like a document rather than a path.
    #[pyo3(signature = (shapes, times, format = None, base_iri = None))]
    fn validate_shacl_at(
        &self,
        py: Python<'_>,
        shapes: &Bound<'_, PyAny>,
        times: FromIterable<EventTime>,
        format: Option<&str>,
        base_iri: Option<&str>,
    ) -> PyResult<Vec<(EventTime, PyShaclReport)>> {
        let shapes = parse_shapes(shapes, format, base_iri)?;
        let view = &self.view;
        Ok(py.detach(|| {
            view.validate_shacl_at(&shapes, times).map(|reports| {
                reports
                    .iter()
                    .map(|(t, report)| (*t, PyShaclReport::from(report)))
                    .collect::<Vec<_>>()
            })
        })?)
    }
}
