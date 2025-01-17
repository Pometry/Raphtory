use crate::{
    core::{DocumentInput, Lifespan},
    python::types::repr::{Repr, StructReprBuilder},
    vectors::Embedding,
};
use itertools::Itertools;
use pyo3::{
    exceptions::PyAttributeError,
    prelude::*,
    types::{PyNone, PyTuple},
};

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

#[pyclass(name = "Document", frozen)]
pub struct PyDocument {
    pub(crate) content: String,
    pub(crate) entity: Option<PyObject>,
    pub(crate) embedding: Option<PyEmbedding>,
    pub(crate) life: Lifespan,
}

#[pymethods]
impl PyDocument {
    /// the document content
    ///
    /// Returns:
    ///     str:
    #[getter]
    fn content(&self) -> &str {
        &self.content
    }

    /// the entity corresponding to the document
    ///
    /// Returns:
    ///     Optional[Any]:
    #[getter]
    fn entity<'py>(&self) -> Option<&PyObject> {
        self.entity.as_ref()
    }

    /// the embedding
    #[getter]
    fn embedding(&self) -> Option<PyEmbedding> {
        self.embedding.clone()
    }

    /// the life span
    ///
    /// Returns:
    ///     None | int | Tuple[int, int]
    #[getter]
    fn life(&self) -> Lifespan {
        self.life
    }
}

impl Clone for PyDocument {
    fn clone(&self) -> Self {
        let entity = self
            .entity
            .as_ref()
            .map(|entity| Python::with_gil(|py| entity.clone_ref(py)));
        Self {
            content: self.content.clone(),
            entity: entity,
            embedding: self.embedding.clone(),
            life: self.life.clone(),
        }
    }
}

#[pyclass(name = "Embedding", frozen)]
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

impl PyEmbedding {
    pub fn embedding(&self) -> Embedding {
        self.0.clone()
    }
}

impl From<DocumentInput> for PyDocument {
    fn from(value: DocumentInput) -> Self {
        Self {
            content: value.content,
            entity: None,
            embedding: None,
            life: value.life,
        }
    }
}

impl Repr for PyDocument {
    fn repr(&self) -> String {
        StructReprBuilder::new("Document")
            .add_field("content", &self.content)
            .add_field("entity", &self.entity)
            .add_field("embedding", &self.embedding)
            .add_field("life", &self.life)
            .finish()
    }
}

#[pymethods]
impl PyDocument {
    #[new]
    #[pyo3(signature = (content, life=None))]
    fn new(content: String, life: Option<&Bound<PyAny>>) -> PyResult<Self> {
        let life = match life {
            None => Lifespan::Inherited,
            Some(life) => {
                if let Ok(time) = life.extract::<i64>() {
                    Lifespan::Event { time }
                } else if let Ok(life) = life.downcast::<PyTuple>() {
                    match life.iter().collect_vec().as_slice() {
                        [start, end] => Lifespan::Interval {
                            start: start.extract::<i64>()?,
                            end: end.extract::<i64>()?,
                        },
                        _ => Err(PyAttributeError::new_err(
                            "if life is a tuple it has to have two elements",
                        ))?,
                    }
                } else {
                    Err(PyAttributeError::new_err(
                        "life has to be an int or a tuple with two numbers",
                    ))?
                }
            }
        };
        Ok(Self {
            content,
            entity: None,
            embedding: None,
            life,
        })
    }

    fn __repr__(&self) -> String {
        self.repr()
    }
}
