use docbrown_core as dbc;
use docbrown_core::vertex::InputVertex;
use docbrown_db::view_api::*;
use docbrown_db::{graph, perspective};
use pyo3::exceptions;
use pyo3::exceptions::{PyException, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyInt, PyIterator, PyString};
use std::collections::HashMap;
use std::fmt::Display;
use std::iter;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use itertools::Itertools;

use crate::graph_window::{GraphWindowSet, WindowedEdge, WindowedGraph, WindowedVertex};
use crate::wrappers::{PerspectiveSet, Prop, VertexIdsIterator, WindowedEdgeIterator, WindowedVertices};
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

    //******  Graph Updates  ******//

    pub fn add_vertex(&self, timestamp: i64, id: &PyAny, properties: Option<HashMap<String, Prop>>) -> PyResult<()> {
        let v = Self::extract_id(id)?;
        let result = self.graph.add_vertex(timestamp, v, &Self::transform_props(properties));
        Self::adapt_err(result)
    }

    pub fn add_vertex_properties(&self, id: &PyAny, props: HashMap<String, Prop>) -> PyResult<()> {
        let v = Self::extract_id(id)?;
        let result = self.graph.add_vertex_properties(v, &Self::transform_props(Some(props)));
        Self::adapt_err(result)
    }

    pub fn add_edge(&self, timestamp: i64, src: &PyAny, dst: &PyAny, properties: Option<HashMap<String, Prop>>) -> PyResult<()> {
        let src = Self::extract_id(src)?;
        let dst = Self::extract_id(dst)?;
        Ok(self.graph.add_edge(timestamp, src, dst, &Self::transform_props(properties)))
    }

    pub fn add_edge_properties(&self, src: &PyAny, dst: &PyAny, props: HashMap<String, Prop>) -> PyResult<()> {
        let src = Self::extract_id(src)?;
        let dst = Self::extract_id(dst)?;
        let result = self.graph.add_edge_properties(src, dst, &Self::transform_props(Some(props)));
        Self::adapt_err(result)
    }

    //******  Perspective APIS  ******//

    pub fn window(&self, t_start: i64, t_end: i64) -> WindowedGraph {
        WindowedGraph::new(self, t_start, t_end)
    }

    pub fn at(&self, end: i64) -> WindowedGraph { self.graph.at(end).into() }

    pub fn latest(&self) -> WindowedGraph {
        match self.latest_time(){
            None =>  self.at(0),
            Some(time) => self.at(time)
        }
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

    //******  Saving And Loading  ******//

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

    //******  Metrics APIs ******//

    pub fn earliest_time(&self) -> Option<i64> {
        self.graph.earliest_time()
    }

    pub fn latest_time(&self) -> Option<i64> {
        self.graph.latest_time()
    }

    pub fn num_edges(&self) -> usize {
        self.graph.num_edges()
    }

    pub fn num_vertices(&self) -> usize {
        self.graph.num_vertices()
    }

    pub fn has_vertex(&self, v: &PyAny) -> bool {
        if let Ok(v) = v.extract::<String>() {
            self.graph.has_vertex(v)
        } else if let Ok(v) = v.extract::<u64>() {
            self.graph.has_vertex(v)
        } else {
            panic!("Input must be a string or integer.")
        }
    }

    pub fn has_edge(&self, src: &PyAny, dst: &PyAny) -> bool {
        if let (Ok(src), Ok(dst)) = (src.extract::<String>(), dst.extract::<String>()) {
            self.graph.has_edge(
                src,
                dst,
            )
        } else if let (Ok(src), Ok(dst)) = (src.extract::<u64>(), dst.extract::<u64>()) {
            self.graph
                .has_edge(src, dst)
        } else {
            //FIXME This probably should just throw an error not fully panic
            panic!("Types of src and dst must be the same (either Int or str)")
        }
    }

    //******  Getter APIs ******//
    //TODO Implement LatestVertex/Edge
    //FIXME These are just placeholders for now and do not work because of the pyRef
//     pub fn vertex(&self, v: u64) -> Option<WindowedVertex> {
//         match self.latest_time(){
//             None =>  None,
//             Some(time) =>self.vertex(v)
//         }
//     }
//
//     pub fn vertex_ids(&self) -> VertexIdsIterator {
//         match self.latest_time(){
//             None =>  { self.at(0).vertex_ids()
//             },
//             Some(time) => self.at(time).vertex_ids()
//         }
//     }
//slf: PyRef<'_, Self>
//     pub fn vertices(&self) -> WindowedVertices {
//         match self.latest_time(){
//             None =>  {
//                 self.at(0).vertices()
//             },
//             Some(time) => self.at(time).vertices()
//         }
//     }
//
//     pub fn edge(&self, src: u64, dst: u64) -> Option<WindowedEdge> {
//         match self.latest_time(){
//             None =>  {
//                 None
//             },
//             Some(time) => self.at(time).edge(src,dst)
//         }
//     }
//
//     pub fn edges(&self) -> WindowedEdgeIterator {
//         match self.latest_time(){
//             None =>  {
//                 self.at(0).edges()
//             },
//             Some(time) => self.at(time).edges()
//         }
//     }
}

impl Graph {
    fn transform_props(props: Option<HashMap<String, Prop>>) -> Vec<(String, dbc::Prop)> {
        props.unwrap_or_default().into_iter().map(|(key, value)| (key, value.into())).collect_vec()
    }

    fn extract_id(id: &PyAny) -> PyResult<InputVertexBox> {
        match id.extract::<String>() {
            Ok(string) => Ok(InputVertexBox::new(string)),
            Err(_) => {
                let msg = "IDs need to be strings or an unsigned integers";
                let number = id.extract::<u64>().map_err(|_| PyTypeError::new_err(msg))?;
                Ok(InputVertexBox::new(number))
            },
        }
    }

    fn adapt_err<E>(result: Result<(), E>) -> PyResult<()>
    where
        E: std::error::Error
    {
        result.map_err(|e| {
            let error_log = display_error_chain::DisplayErrorChain::new(&e).to_string();
            PyException::new_err(error_log)
        })
    }
}

pub struct InputVertexBox {
    id: u64,
    name_prop: Option<dbc::Prop>,
}

impl InputVertexBox {
    pub(crate) fn new<T>(vertex: T) -> InputVertexBox
    where
        T: InputVertex
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
