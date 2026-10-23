use crate::{
    db::api::view::DynamicGraph,
    python::graph::views::graph_view::{PyGraphView, TemplateConfig},
};
use pyo3::{exceptions::PyException, pymethods, Bound, Py, PyAny, PyResult};
use std::sync::OnceLock;

/// Signature of the `GraphView.vectorise` implementation.
pub type VectoriseFn = for<'py> fn(
    DynamicGraph,
    &Bound<'py, PyAny>,
    TemplateConfig,
    TemplateConfig,
    bool,
) -> PyResult<Py<PyAny>>;

static VECTORISE: OnceLock<VectoriseFn> = OnceLock::new();

/// Called by `raphtory-vectors` when its python module is initialised: raphtory itself does
/// not depend on the vector stack, so `GraphView.vectorise` forwards to this implementation.
pub fn register_vectorise(vectorise: VectoriseFn) {
    let _ = VECTORISE.set(vectorise);
}

#[pymethods]
impl PyGraphView {
    /// Create a VectorisedGraph from the current graph.
    ///
    /// Every node and edge is rendered into a text document by a template, and the document is what gets embedded.
    ///
    /// Args:
    ///   model (VectorCache): Cache wrapping the embedding model used to embed documents.
    ///   nodes (bool | str): True to embed nodes with the default document template, False not to embed them, or a Jinja (minijinja) document template to render each node with. Defaults to True.
    ///   edges (bool | str): True to embed edges with the default document template, False not to embed them, or a Jinja (minijinja) document template to render each edge with. Defaults to True.
    ///   verbose (bool): Enable to print logs reporting progress. Defaults to False.
    ///
    /// Returns:
    ///   VectorisedGraph: A VectorisedGraph with all the documents and their embeddings, with an initial empty selection.
    ///
    /// Note:
    ///   A template string is rendered as it is, so a bare word such as `"description"` becomes the literal document `description` for every entity; to embed a property, interpolate it: `"{{ properties.description }}"`.
    ///
    ///   A node template can use `name`, `node_type`, `properties`, `metadata` and `temporal_properties` (a mapping from property name to a list of `(time, value)` pairs). An edge template can use `src` and `dst` (each with the node variables above, e.g. `src.name`), `history` (the update times), `layers`, `properties`, `metadata` and `temporal_properties`. A `datetimeformat` filter formats a timestamp, as in `{{ time|datetimeformat }}`.
    ///
    /// Example:
    ///   >>> vg = g.vectorise(cache, nodes="{{ name }} is a {{ node_type }}", edges="{{ src.name }} -> {{ dst.name }}: {{ properties.description }}")
    #[pyo3(signature = (model, nodes = TemplateConfig::Bool(true), edges = TemplateConfig::Bool(true), verbose = false))]
    fn vectorise(
        &self,
        model: &Bound<'_, PyAny>,
        nodes: TemplateConfig,
        edges: TemplateConfig,
        verbose: bool,
    ) -> PyResult<Py<PyAny>> {
        let vectorise = VECTORISE.get().ok_or_else(|| {
            PyException::new_err(
                "vectorise is not available: raphtory was built without vector support",
            )
        })?;
        vectorise(self.graph.clone(), model, nodes, edges, verbose)
    }
}
