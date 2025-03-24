use crate::{
    core::Lifespan,
    db::api::view::DynamicGraph,
    python::{
        graph::views::graph_view::PyGraphView,
        types::repr::{Repr, StructReprBuilder},
    },
    vectors::{Document, DocumentEntity, Embedding},
};
use pyo3::{prelude::*, types::PyNone, IntoPyObjectExt};

impl<'py> IntoPyObject<'py> for Lifespan {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(match self {
            Lifespan::Inherited => PyNone::get(py).to_owned().into_any(),
            Lifespan::Event { time } => time.into_pyobject(py)?.into_any(),
            Lifespan::Interval { start, end } => (start, end).into_pyobject(py)?.into_any(),
        })
    }
}

impl Repr for Lifespan {
    fn repr(&self) -> String {
        match self {
            Lifespan::Interval { start, end } => (start, end).repr(),
            Lifespan::Event { time } => time.repr(),
            Lifespan::Inherited => "None".to_string(),
        }
    }
}

/// A Document
///
/// Args:
///     content (str): the document content
///     life (int | Tuple[int, int], optional): the optional lifespan for the document (single value
///                                             corresponds to an event, a tuple corresponds to a
///                                             window).
#[pyclass(name = "Document", module = "raphtory.vectors", frozen)]
#[derive(Clone)]
pub struct PyDocument(pub(crate) Document<DynamicGraph>);

impl From<PyDocument> for Document<DynamicGraph> {
    fn from(value: PyDocument) -> Self {
        value.0
    }
}

#[pymethods]
impl PyDocument {
    /// the document content
    ///
    /// Returns:
    ///     str:
    #[getter]
    fn content(&self) -> &str {
        &self.0.content
    }

    /// the entity corresponding to the document
    ///
    /// Returns:
    ///     Optional[Any]:
    #[getter]
    fn entity(&self, py: Python) -> PyResult<PyObject> {
        match &self.0.entity {
            DocumentEntity::Graph { graph, .. } => graph.clone().into_py_any(py),
            DocumentEntity::Node(entity) => entity.clone().into_py_any(py),
            DocumentEntity::Edge(entity) => entity.clone().into_py_any(py),
        }
    }

    /// the embedding
    ///
    /// Returns:
    ///     Optional[Embedding]: the embedding for the document if it was computed
    #[getter]
    fn embedding(&self) -> PyEmbedding {
        PyEmbedding(self.0.embedding.clone())
    }

    /// the life span
    ///
    /// Returns:
    ///     Optional[Union[int | Tuple[int, int]]]:
    #[getter]
    fn life(&self) -> Lifespan {
        self.0.life
    }
}

#[pyclass(name = "Embedding", module = "raphtory.vectors", frozen)]
#[derive(Clone)]
pub struct PyEmbedding(pub Embedding);

impl Repr for PyEmbedding {
    fn repr(&self) -> String {
        format!("Embedding({})", self.0.repr())
    }
}

#[pymethods]
impl PyEmbedding {
    fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyDocument {
    fn repr(&self) -> String {
        let repr = StructReprBuilder::new("Document");
        let with_entity = match &self.0.entity {
            DocumentEntity::Graph { graph, .. } => {
                let graph = graph.clone();
                repr.add_field("entity", PyGraphView { graph })
            }
            DocumentEntity::Node(node) => repr.add_field("entity", node),
            DocumentEntity::Edge(edge) => repr.add_field("entity", edge),
        };
        with_entity
            .add_field("content", &self.content())
            .add_field("embedding", &self.embedding())
            .add_field("life", &self.life())
            .finish()
    }
}

#[pymethods]
impl PyDocument {
    fn __repr__(&self) -> String {
        self.repr()
    }
}
