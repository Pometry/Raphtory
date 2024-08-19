use crate::{
    core::{DocumentInput, Lifespan},
    vectors::Embedding,
};
use itertools::Itertools;
use pyo3::{exceptions::PyAttributeError, prelude::*, types::PyTuple};

impl IntoPy<PyObject> for Lifespan {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            Lifespan::Inherited => py.None(),
            Lifespan::Event { time } => time.into_py(py),
            Lifespan::Interval { start, end } => (start, end).into_py(py),
        }
    }
}

#[derive(Clone)]
#[pyclass(name = "Document", frozen, get_all)]
pub struct PyDocument {
    pub(crate) content: String,
    pub(crate) entity: Option<PyObject>,
    pub(crate) embedding: Option<Embedding>,
    pub(crate) life: Lifespan,
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

#[pymethods]
impl PyDocument {
    #[new]
    fn new(content: String, life: Option<&PyAny>) -> PyResult<Self> {
        let life = match life {
            None => Lifespan::Inherited,
            Some(life) => {
                if let Ok(time) = life.extract::<i64>() {
                    Lifespan::Event { time }
                } else if let Ok(life) = life.extract::<&PyTuple>() {
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

    fn __repr__(&self, py: Python) -> String {
        let entity_repr = match &self.entity {
            None => "None".to_owned(),
            Some(entity) => match entity.call_method0(py, "__repr__") {
                Ok(repr) => repr.extract::<String>(py).unwrap_or("None".to_owned()),
                Err(_) => "None".to_owned(),
            },
        };

        let py_content = self.content.clone().into_py(py);
        let content_repr = match py_content.call_method0(py, "__repr__") {
            Ok(repr) => repr.extract::<String>(py).unwrap_or("''".to_owned()),
            Err(_) => "''".to_owned(),
        };

        let py_embedding = self.content.clone().into_py(py);
        let embedding_repr = match py_embedding.call_method0(py, "__repr__") {
            Ok(repr) => repr.extract::<String>(py).unwrap_or("''".to_owned()),
            Err(_) => "''".to_owned(),
        };

        let life_repr = match self.life {
            Lifespan::Inherited => "None".to_owned(),
            Lifespan::Event { time } => time.to_string(),
            Lifespan::Interval { start, end } => format!("({start}, {end})"),
        };

        format!(
            "Document(content={content_repr}, entity={entity_repr}, embedding={embedding_repr} life={life_repr})"
        )
    }
}
