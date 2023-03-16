use docbrown_core as dbc;
use docbrown_db::view_api::*;
use docbrown_db::{graph, perspective};
use pyo3::exceptions;
use pyo3::prelude::*;
use pyo3::types::PyIterator;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::graph_window::{GraphWindowSet, WindowedGraph};
use crate::wrappers::{PerspectiveSet, Prop};
use crate::Perspective;

#[pyclass]
pub struct Graph {
    pub(crate) graph: graph::Graph,
}

impl Graph {
    pub fn from_db_graph(db_graph: graph::Graph) -> Self {
        Self { graph: db_graph }
    }
}

#[pymethods]
impl Graph {
    #[new]
    #[pyo3(signature = (nr_shards=1))]
    pub fn new(nr_shards: usize) -> Self {
        Self {
            graph: graph::Graph::new(nr_shards),
        }
    }

    pub fn earliest_time(&self) -> Option<i64> {
        self.graph.earliest_time()
    }

    pub fn latest_time(&self) -> Option<i64> {
        self.graph.latest_time()
    }

    pub fn window(&self, t_start: i64, t_end: i64) -> WindowedGraph {
        WindowedGraph::new(self, t_start, t_end)
    }

    fn through(&self, perspectives: &PyAny) -> PyResult<GraphWindowSet> {
        struct PyPerspectiveIterator {
            pub iter: Py<PyIterator>,
        }
        unsafe impl Send for PyPerspectiveIterator {} // iter is used by holding the GIL
        impl Iterator for PyPerspectiveIterator {
            type Item = perspective::Perspective;
            fn next(&mut self) -> Option<Self::Item> {
                Python::with_gil(|py| {
                    let item = self.iter.as_ref(py).next()?.ok()?;
                    Some(item.extract::<Perspective>().ok()?.into())
                })
            }
        }

        let result = match perspectives.extract::<PerspectiveSet>() {
            Ok(perspective_set) => self.graph.through_perspectives(perspective_set.ps),
            Err(_) => {
                let iter = PyPerspectiveIterator {
                    iter: Py::from(perspectives.iter()?),
                };
                self.graph.through_iter(Box::new(iter))
            }
        };
        Ok(result.into())
    }

    #[staticmethod]
    pub fn load_from_file(path: String) -> PyResult<Self> {
        let file_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), &path].iter().collect();

        match graph::Graph::load_from_file(file_path) {
            Ok(g) => Ok(Graph { graph: g }),
            Err(e) => Err(exceptions::PyException::new_err(format!(
                "Failed to load graph from the files. Reason: {}",
                e.to_string()
            ))),
        }
    }

    pub fn save_to_file(&self, path: String) -> PyResult<()> {
        match self.graph.save_to_file(Path::new(&path)) {
            Ok(()) => Ok(()),
            Err(e) => Err(exceptions::PyException::new_err(format!(
                "Failed to save graph to the files. Reason: {}",
                e.to_string()
            ))),
        }
    }

    pub fn len(&self) -> usize {
        self.graph.num_vertices()
    }

    pub fn edges_len(&self) -> usize {
        self.graph.num_edges()
    }

    pub fn has_vertex(&self, v: u64) -> bool {
        self.graph.has_vertex(v)
    }

    pub fn add_vertex(&self, t: i64, v: u64, props: HashMap<String, Prop>) {
        self.graph.add_vertex(
            t,
            v,
            &props
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect::<Vec<(String, dbc::Prop)>>(),
        )
    }

    pub fn add_edge(&self, t: i64, src: u64, dst: u64, props: HashMap<String, Prop>) {
        self.graph.add_edge(
            t,
            src,
            dst,
            &props
                .into_iter()
                .map(|f| (f.0.clone(), f.1.into()))
                .collect::<Vec<(String, dbc::Prop)>>(),
        )
    }
}
