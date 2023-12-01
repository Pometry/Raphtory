use pyo3::{exceptions, prelude::*, AsPyPointer};
use raphtory_core::{db::api::view::MaterializedGraph, python::utils::errors::adapt_err_value};
use raphtory_graphql::{url_decode_graph, url_encode_graph, RaphtoryServer};
use std::{collections::HashMap, ops::Deref, path::PathBuf, sync::Arc, thread};

use pyo3::{
    exceptions::PyException,
    prelude::*,
    types::{PyFunction, PyList},
};
use raphtory_core::python::{packages::vectors::PyDocumentTemplate, utils::spawn_async_task};
use raphtory_graphql::server::RunningRaphtoryServer;

#[pyclass(name = "RaphtoryServer")]
pub(crate) struct PyRaphtoryServer(Option<RaphtoryServer>);

impl PyRaphtoryServer {
    fn new(server: RaphtoryServer) -> Self {
        Self(Some(server))
    }
}

#[pymethods]
impl PyRaphtoryServer {
    #[staticmethod]
    fn from_map(graphs: HashMap<String, MaterializedGraph>) -> Self {
        Self::new(RaphtoryServer::from_map(graphs))
    }

    #[staticmethod]
    fn from_directory(graph_directory: &str) -> Self {
        Self::new(RaphtoryServer::from_directory(graph_directory))
    }

    #[staticmethod]
    fn from_map_and_directory(
        graphs: HashMap<String, MaterializedGraph>,
        graph_directory: &str,
    ) -> Self {
        Self::new(RaphtoryServer::from_map_and_directory(
            graphs,
            graph_directory,
        ))
    }

    fn with_vectorised(
        slf: PyRefMut<Self>,
        graph_names: Vec<String>,
        embedding: &PyFunction,
        cache: String,
        node_document: Option<String>,
        edge_document: Option<String>,
    ) -> PyResult<Self> {
        let embedding: Py<PyFunction> = embedding.into();
        let template = PyDocumentTemplate::new(node_document, edge_document);

        let server = take_sever_ownership(slf)?;
        spawn_async_task(move || async move {
            let new_server = server
                .with_vectorised(
                    graph_names,
                    embedding.clone(),
                    &PathBuf::from(cache),
                    Some(template),
                )
                .await;
            Ok(Self::new(new_server))
        })
    }

    // // TODO: this is doable!!!
    // pub fn register_algorithm(self, name: String, algorithm: &PyFunction) -> RaphtoryServer {
    //     self.0.register_algorithm(???)
    // }

    pub fn start(slf: PyRefMut<Self>) -> PyResult<PyRunningRaphtoryServer> {
        let server = take_sever_ownership(slf)?;
        Ok(PyRunningRaphtoryServer::new(server.start()))
    }

    pub fn start_with_port(slf: PyRefMut<Self>, port: u16) -> PyResult<PyRunningRaphtoryServer> {
        let server = take_sever_ownership(slf)?;
        Ok(PyRunningRaphtoryServer::new(server.start_with_port(port)))
    }

    pub fn run(slf: PyRefMut<Self>) -> PyResult<()> {
        // let server = take_sever_ownership(slf)?;
        wait_server(&mut Self::start(slf)?.0)
    }

    pub fn run_with_port(slf: PyRefMut<Self>, port: u16) -> PyResult<()> {
        // Self::start_with_port(slf, port)?.wait()
        wait_server(&mut Self::start_with_port(slf, port)?.0)
    }
}

fn take_sever_ownership(mut server: PyRefMut<PyRaphtoryServer>) -> PyResult<RaphtoryServer> {
    let new_server = server.0.take().ok_or_else(|| {
        PyException::new_err(
            "Server object has already been used, please create another one from scratch",
        )
    })?;
    Ok(new_server)
}

const RUNNING_SERVER_CONSUMED_MSG: &str =
    "Running server object has already been used, please create another one from scratch";

#[pyclass(name = "RunningRaphtoryServer")]
pub(crate) struct PyRunningRaphtoryServer(Option<RunningRaphtoryServer>);

impl PyRunningRaphtoryServer {
    fn new(running_server: RunningRaphtoryServer) -> Self {
        Self(Some(running_server))
    }
}

fn wait_server(running_server: &mut Option<RunningRaphtoryServer>) -> PyResult<()> {
    let owned_running_server = running_server
        .take()
        .ok_or_else(|| PyException::new_err(RUNNING_SERVER_CONSUMED_MSG))?;
    spawn_async_task(move || async move {
        let result = owned_running_server.wait().await;
        result.map_err(|e| adapt_err_value(&e))
    })
}

#[pymethods]
impl PyRunningRaphtoryServer {
    pub(crate) fn stop(&self) -> PyResult<()> {
        match &self.0 {
            Some(running_server) => {
                let sender = running_server._get_sender().clone();
                spawn_async_task(move || async move {
                    let _ignored = sender.send(()).await;
                    Ok(())
                })
            }
            None => Err(PyException::new_err(RUNNING_SERVER_CONSUMED_MSG)),
        }
    }

    pub(crate) fn wait(mut slf: PyRefMut<Self>) -> PyResult<()> {
        wait_server(&mut slf.0)
    }
}

#[pyfunction]
pub fn encode_graph(graph: MaterializedGraph) -> PyResult<String> {
    let result = url_encode_graph(graph);
    match result {
        Ok(s) => Ok(s),
        Err(e) => Err(exceptions::PyValueError::new_err(format!(
            "Error encoding: {:?}",
            e
        ))),
    }
}

#[pyfunction]
pub fn decode_graph(py: Python, encoded_graph: String) -> PyResult<PyObject> {
    let result = url_decode_graph(encoded_graph);
    match result {
        Ok(s) => Ok(s.into_py(py)),
        Err(e) => Err(exceptions::PyValueError::new_err(format!(
            "Error decoding: {:?}",
            e
        ))),
    }
}
