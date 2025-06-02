use crate::{
    db::api::view::DynamicGraph,
    python::types::repr::{Repr, StructReprBuilder},
    vectors::{Document, DocumentEntity, Embedding},
};
use pyo3::{prelude::*, IntoPyObjectExt};

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
            DocumentEntity::Node(node) => repr.add_field("entity", node),
            DocumentEntity::Edge(edge) => repr.add_field("entity", edge),
        };
        with_entity
            .add_field("content", &self.content())
            .add_field("embedding", &self.embedding())
            .finish()
    }
}

#[pymethods]
impl PyDocument {
    fn __repr__(&self) -> String {
        self.repr()
    }
}
