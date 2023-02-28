use docbrown_core as dbc;
use docbrown_db::graph;
use pyo3::exceptions;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::wrappers::Direction;
use crate::wrappers::EdgeIterator;
use crate::wrappers::Prop;
use crate::wrappers::VertexIdsIterator;
use crate::wrappers::VertexIterator;

use crate::graph_window::WindowedGraph;

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

    pub fn contains(&self, v: u64) -> bool {
        self.graph.contains(v)
    }

    pub fn contains_window(&self, v: u64, t_start: i64, t_end: i64) -> bool {
        self.graph.contains_window(v, t_start, t_end)
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

    pub fn degree(&self, v: u64, d: Direction) -> usize {
        self.graph.degree(v, d.into())
    }

    pub fn degree_window(&self, v: u64, t_start: i64, t_end: i64, d: Direction) -> usize {
        self.graph.degree_window(v, t_start, t_end, d.into())
    }

    pub fn vertex_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: self.graph.vertex_ids(),
        }
    }

    pub fn vertices(&self) -> VertexIterator {
        let iter = self.graph.vertices().map(|tv| tv.into());

        VertexIterator {
            iter: Box::new(iter),
        }
    }

    pub fn vertices_window(&self, t_start: i64, t_end: i64) -> VertexIterator {
        let iter = self
            .graph
            .vertices_window(t_start, t_end)
            .map(|tv| tv.into());

        VertexIterator {
            iter: Box::new(iter),
        }
    }

    pub fn neighbours(&self, v: u64, d: Direction) -> EdgeIterator {
        let iter = self.graph.neighbours(v, d.into()).map(|te| te.into());

        EdgeIterator {
            iter: Box::new(iter),
        }
    }

    pub fn neighbours_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> EdgeIterator {
        let iter = self
            .graph
            .neighbours_window(v, t_start, t_end, d.into())
            .map(|te| te.into());

        EdgeIterator {
            iter: Box::new(iter),
        }
    }

    pub fn neighbours_window_t(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> EdgeIterator {
        let iter = self
            .graph
            .neighbours_window_t(v, t_start, t_end, d.into())
            .map(|te| te.into());

        EdgeIterator {
            iter: Box::new(iter),
        }
    }
}
