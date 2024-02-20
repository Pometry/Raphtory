use crate::core::{DocumentInput, Lifespan};
use itertools::Itertools;
use pyo3::{exceptions::PyAttributeError, prelude::*, types::PyTuple};

#[derive(Clone)]
#[pyclass(name = "Document", frozen, get_all)]
pub struct PyDocument {
    pub(crate) content: String,
    pub(crate) entity: Option<PyObject>,
    pub(crate) life: Lifespan,
}

impl From<DocumentInput> for PyDocument {
    fn from(value: DocumentInput) -> Self {
        Self {
            content: value.content,
            entity: None,
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
        let life_repr = match self.life {
            Lifespan::Inherited => "None".to_owned(),
            Lifespan::Event { time } => time.to_string(),
            Lifespan::Interval { start, end } => format!("({start}, {end})"),
        };
        format!(
            "Document(content={}, entity={}, life={})",
            content_repr, entity_repr, life_repr
        )
    }
}
