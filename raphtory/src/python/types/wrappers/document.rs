use crate::{
    core::Lifespan,
    db::api::view::DynamicGraph,
    python::types::repr::{Repr, StructReprBuilder},
    vectors::{Document, DocumentOps, Embedding},
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
        &self.0.content()
    }

    /// the entity corresponding to the document
    ///
    /// Returns:
    ///     Optional[Any]:
    #[getter]
    fn entity(&self, py: Python) -> PyResult<PyObject> {
        match &self.0 {
            Document::Graph { entity, .. } => entity.clone().into_py_any(py),
            Document::Node { entity, .. } => entity.clone().into_py_any(py),
            Document::Edge { entity, .. } => entity.clone().into_py_any(py),
        }
    }

    /// the embedding
    ///
    /// Returns:
    ///     Optional[Embedding]: the embedding for the document if it was computed
    #[getter]
    fn embedding(&self) -> PyEmbedding {
        let embedding = match &self.0 {
            Document::Graph { embedding, .. } => embedding.clone(),
            Document::Node { embedding, .. } => embedding.clone(),
            Document::Edge { embedding, .. } => embedding.clone(),
        };
        PyEmbedding(embedding)
    }

    /// the life span
    ///
    /// Returns:
    ///     Optional[Union[int | Tuple[int, int]]]:
    #[getter]
    fn life(&self) -> Lifespan {
        match self.0 {
            Document::Graph { life, .. } => life,
            Document::Node { life, .. } => life,
            Document::Edge { life, .. } => life,
        }
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

// impl PyEmbedding {
//     pub fn embedding(&self) -> Embedding {
//         self.0.clone()
//     }
// }

// impl From<DocumentInput> for PyDocument {
//     fn from(value: DocumentInput) -> Self {
//         Self {
//             content: value.content,
//             entity: None,
//             embedding: None,
//             life: value.life,
//         }
//     }
// }

impl Repr for PyDocument {
    fn repr(&self) -> String {
        let entity = match &self.0 {
            Document::Graph { .. } => "graph".to_owned(), // FIXME:
            Document::Node { entity, .. } => entity.repr(),
            Document::Edge { entity, .. } => entity.repr(),
        };
        StructReprBuilder::new("Document")
            .add_field("content", &self.content())
            .add_field("entity", entity)
            .add_field("embedding", &self.embedding())
            .add_field("life", &self.life())
            .finish()
    }
}

#[pymethods]
impl PyDocument {
    // #[new]
    // #[pyo3(signature = (content, life=None))]
    // fn new(content: String, life: Option<&Bound<PyAny>>) -> PyResult<Self> {
    //     let life = match life {
    //         None => Lifespan::Inherited,
    //         Some(life) => {
    //             if let Ok(time) = life.extract::<i64>() {
    //                 Lifespan::Event { time }
    //             } else if let Ok(life) = life.downcast::<PyTuple>() {
    //                 match life.iter().collect_vec().as_slice() {
    //                     [start, end] => Lifespan::Interval {
    //                         start: start.extract::<i64>()?,
    //                         end: end.extract::<i64>()?,
    //                     },
    //                     _ => Err(PyAttributeError::new_err(
    //                         "if life is a tuple it has to have two elements",
    //                     ))?,
    //                 }
    //             } else {
    //                 Err(PyAttributeError::new_err(
    //                     "life has to be an int or a tuple with two numbers",
    //                 ))?
    //             }
    //         }
    //     };
    //     Ok(Self {
    //         content,
    //         entity: None,
    //         embedding: None,
    //         life,
    //     })
    // }

    fn __repr__(&self) -> String {
        self.repr()
    }
}
