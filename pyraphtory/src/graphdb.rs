use docbrown_core as dbc;
use docbrown_db::graphdb as gdb;
use pyo3::exceptions;
use pyo3::FromPyObject;
use pyo3::PyRef;
use pyo3::PyRefMut;
use pyo3::{pyclass, pymethods, pymodule, types::PyModule, PyResult, Python};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[pyclass]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum Direction {
    OUT,
    IN,
    BOTH,
}

impl Direction {
    fn convert(&self) -> dbc::Direction {
        match self {
            Direction::OUT => dbc::Direction::OUT,
            Direction::IN => dbc::Direction::IN,
            Direction::BOTH => dbc::Direction::BOTH,
        }
    }
}

#[derive(FromPyObject, Debug)]
pub enum Prop {
    Str(String),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
}

impl Prop {
    fn convert(&self) -> dbc::Prop {
        match self {
            Prop::Str(string) => dbc::Prop::Str(string.clone()),
            Prop::I64(i64) => dbc::Prop::I64(*i64),
            Prop::U64(u64) => dbc::Prop::U64(*u64),
            Prop::F64(f64) => dbc::Prop::F64(*f64),
            Prop::Bool(bool) => dbc::Prop::Bool(*bool),
        }
    }
}

#[pyclass]
pub struct TEdge {
    #[pyo3(get)]
    pub src: u64,
    #[pyo3(get)]
    pub dst: u64,
    #[pyo3(get)]
    pub t: Option<i64>,
    #[pyo3(get)]
    pub is_remote: bool,
}

impl TEdge {
    fn convert(edge: dbc::tpartition::TEdge) -> TEdge {
        let dbc::tpartition::TEdge {
            src,
            dst,
            t,
            is_remote,
        } = edge;
        TEdge {
            src,
            dst,
            t,
            is_remote,
        }
    }
}

#[pyclass]
pub struct VertexIterator {
    iter: std::vec::IntoIter<u64>,
}

#[pymethods]
impl VertexIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<u64> {
        slf.iter.next()
    }
}

#[pyclass]
pub struct EdgeIterator {
    iter: std::vec::IntoIter<TEdge>,
}

#[pymethods]
impl EdgeIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<TEdge> {
        slf.iter.next()
    }
}

#[pyclass]
pub struct GraphDB {
    pub(crate) graphdb: gdb::GraphDB,
}

#[pymethods]
impl GraphDB {
    #[new]
    pub fn new(nr_shards: usize) -> Self {
        Self {
            graphdb: gdb::GraphDB::new(nr_shards),
        }
    }

    #[staticmethod]
    pub fn load_from_file(path: String) -> PyResult<Self> {
        let file_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), &path].iter().collect();

        match gdb::GraphDB::load_from_file(file_path) {
            Ok(g) => Ok(GraphDB { graphdb: g }),
            Err(e) => Err(exceptions::PyException::new_err(format!(
                "Failed to load graph from the files. Reason: {}",
                e.to_string()
            ))),
        }
    }

    pub fn save_to_file(&self, path: String) -> PyResult<()> {
        match self.graphdb.save_to_file(Path::new(&path)) {
            Ok(()) => Ok(()),
            Err(e) => Err(exceptions::PyException::new_err(format!(
                "Failed to save graph to the files. Reason: {}",
                e.to_string()
            ))),
        }
    }

    pub fn len(&self) -> usize {
        self.graphdb.len()
    }

    pub fn edges_len(&self) -> usize {
        self.graphdb.edges_len()
    }

    pub fn contains(&self, v: u64) -> bool {
        self.graphdb.contains(v)
    }

    pub fn contains_window(&self, v: u64, t_start: i64, t_end: i64) -> bool {
        self.graphdb.contains_window(v, t_start, t_end)
    }

    pub fn add_vertex(&self, v: u64, t: i64, props: HashMap<String, Prop>) {
        self.graphdb.add_vertex(
            v,
            t,
            &props
                .into_iter()
                .map(|(key, value)| (key, value.convert()))
                .collect::<Vec<(String, dbc::Prop)>>(),
        )
    }

    pub fn add_edge(&self, src: u64, dst: u64, t: i64, props: HashMap<String, Prop>) {
        self.graphdb.add_edge(
            src,
            dst,
            t,
            &props
                .into_iter()
                .map(|f| (f.0.clone(), f.1.convert()))
                .collect::<Vec<(String, dbc::Prop)>>(),
        )
    }

    pub fn degree(&self, v: u64, d: Direction) -> usize {
        self.graphdb.degree(v, d.convert())
    }

    pub fn degree_window(&self, v: u64, t_start: i64, t_end: i64, d: Direction) -> usize {
        self.graphdb.degree_window(v, t_start, t_end, d.convert())
    }

    pub fn vertices(&self) -> VertexIterator {
        VertexIterator {
            iter: self.graphdb.vertices().collect::<Vec<_>>().into_iter(),
        }
    }

    pub fn neighbours(&self, v: u64, d: Direction) -> EdgeIterator {
        EdgeIterator {
            iter: self
                .graphdb
                .neighbours(v, d.convert())
                .map(|f| TEdge::convert(f))
                .collect::<Vec<_>>()
                .into_iter(),
        }
    }

    pub fn neighbours_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> EdgeIterator {
        EdgeIterator {
            iter: self
                .graphdb
                .neighbours_window(v, t_start, t_end, d.convert())
                .map(|f| TEdge::convert(f))
                .collect::<Vec<_>>()
                .into_iter(),
        }
    }

    pub fn neighbours_window_t(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> EdgeIterator {
        EdgeIterator {
            iter: self
                .graphdb
                .neighbours_window_t(v, t_start, t_end, d.convert())
                .map(|f| TEdge::convert(f))
                .collect::<Vec<_>>()
                .into_iter(),
        }
    }
}

#[pymodule]
fn pyraphtory(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Direction>()?;
    m.add_class::<GraphDB>()?;
    m.add_class::<TEdge>()?;
    Ok(())
}
