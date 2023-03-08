use docbrown_core as dbc;
use docbrown_db::graph;
use pyo3::exceptions;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::graph_window::WindowedGraph;
use crate::wrappers::Prop;

#[pyclass]
pub struct Graph {
    pub(crate) graph: graph::Graph,
}

#[pymethods]
impl Graph {
    #[new]
    pub fn new(nr_shards: usize) -> Self {
        Self {
            graph: graph::Graph::new(nr_shards),
        }
    }

    pub fn window(&self, t_start: i64, t_end: i64) -> WindowedGraph {
        WindowedGraph::new(self, t_start, t_end)
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
        self.graph.len()
    }

    pub fn edges_len(&self) -> usize {
        self.graph.edges_len()
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
