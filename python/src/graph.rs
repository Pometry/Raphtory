use crate::dynamic::DynamicGraph;
use crate::graph_view::PyGraphView;
use crate::util::adapt_result;
use crate::wrappers::Prop;
use docbrown_core as dbc;
use docbrown_core::vertex::InputVertex;
use docbrown_db::graph::Graph;
use itertools::Itertools;
use pyo3::exceptions::{PyException, PyTypeError};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[pyclass(name="Graph", extends=PyGraphView)]
pub struct PyGraph {
    pub(crate) graph: Graph,
}

impl From<Graph> for PyGraph {
    fn from(value: Graph) -> Self {
        Self { graph: value }
    }
}

impl PyGraph {
    pub fn py_from_db_graph(db_graph: Graph) -> PyResult<Py<PyGraph>> {
        Python::with_gil(|py| {
            Py::new(
                py,
                (PyGraph::from(db_graph.clone()), PyGraphView::from(db_graph)),
            )
        })
    }
}

#[pymethods]
impl PyGraph {
    #[new]
    #[pyo3(signature = (nr_shards=1))]
    pub fn py_new(nr_shards: usize) -> (Self, PyGraphView) {
        let graph = Graph::new(nr_shards);
        (
            Self {
                graph: graph.clone(),
            },
            PyGraphView::from(DynamicGraph::from(graph)),
        )
    }

    //******  Graph Updates  ******//

    pub fn add_vertex(
        &self,
        timestamp: i64,
        id: &PyAny,
        properties: Option<HashMap<String, Prop>>,
    ) -> PyResult<()> {
        let v = Self::extract_id(id)?;
        let result = self
            .graph
            .add_vertex(timestamp, v, &Self::transform_props(properties));
        adapt_result(result)
    }

    pub fn add_vertex_properties(
        &self,
        id: &PyAny,
        properties: HashMap<String, Prop>,
    ) -> PyResult<()> {
        let v = Self::extract_id(id)?;
        let result = self
            .graph
            .add_vertex_properties(v, &Self::transform_props(Some(properties)));
        adapt_result(result)
    }

    pub fn add_edge(
        &self,
        timestamp: i64,
        src: &PyAny,
        dst: &PyAny,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
    ) -> PyResult<()> {
        let src = Self::extract_id(src)?;
        let dst = Self::extract_id(dst)?;
        adapt_result(self.graph.add_edge(
            timestamp,
            src,
            dst,
            &Self::transform_props(properties),
            layer,
        ))
    }

    pub fn add_edge_properties(
        &self,
        src: &PyAny,
        dst: &PyAny,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> PyResult<()> {
        let src = Self::extract_id(src)?;
        let dst = Self::extract_id(dst)?;
        let result = self.graph.add_edge_properties(
            src,
            dst,
            &Self::transform_props(Some(properties)),
            layer,
        );
        adapt_result(result)
    }

    //******  Saving And Loading  ******//

    // Alternative constructors are tricky, see: https://gist.github.com/redshiftzero/648e4feeff3843ffd9924f13625f839c
    #[staticmethod]
    pub fn load_from_file(path: String) -> PyResult<Py<PyGraph>> {
        let file_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), &path].iter().collect();

        match Graph::load_from_file(file_path) {
            Ok(g) => Self::py_from_db_graph(g),
            Err(e) => Err(PyException::new_err(format!(
                "Failed to load graph from the files. Reason: {}",
                e
            ))),
        }
    }

    pub fn save_to_file(&self, path: String) -> PyResult<()> {
        match self.graph.save_to_file(Path::new(&path)) {
            Ok(()) => Ok(()),
            Err(e) => Err(PyException::new_err(format!(
                "Failed to save graph to the files. Reason: {}",
                e
            ))),
        }
    }
}

impl PyGraph {
    fn transform_props(props: Option<HashMap<String, Prop>>) -> Vec<(String, dbc::Prop)> {
        props
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect_vec()
    }

    pub(crate) fn extract_id(id: &PyAny) -> PyResult<InputVertexBox> {
        match id.extract::<String>() {
            Ok(string) => Ok(InputVertexBox::new(string)),
            Err(_) => {
                let msg = "IDs need to be strings or an unsigned integers";
                let number = id.extract::<u64>().map_err(|_| PyTypeError::new_err(msg))?;
                Ok(InputVertexBox::new(number))
            }
        }
    }
}

#[derive(Clone)]
pub struct InputVertexBox {
    id: u64,
    name_prop: Option<dbc::Prop>,
}

impl InputVertexBox {
    pub(crate) fn new<T>(vertex: T) -> InputVertexBox
    where
        T: InputVertex,
    {
        InputVertexBox {
            id: vertex.id(),
            name_prop: vertex.name_prop(),
        }
    }
}

impl InputVertex for InputVertexBox {
    fn id(&self) -> u64 {
        self.id
    }
    fn name_prop(&self) -> Option<dbc::Prop> {
        self.name_prop.clone()
    }
}
