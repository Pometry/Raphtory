use pyo3::prelude::*;
use raphtory_core::{
    db::api::view::internal::DynamicGraph,
    python::{graph::graph::PyGraph, utils::errors::adapt_err_value},
};
use raphtory_graphql::{RaphtoryServer, server::TokioRuntime};
use std::{
    collections::HashMap,
    sync::mpsc::Sender,
};

#[pyfunction]
pub fn from_map(
    py: Python,
    graphs: HashMap<String, PyGraph>,
    port: Option<u16>,
) -> PyResult<ShutdownHandler> {
    let graphs: HashMap<String, DynamicGraph> = graphs
        .into_iter()
        .map(|(key, value)| (key, value.into()))
        .collect();
    let (server, receiver) = RaphtoryServer::from_map(graphs);
    let mut handler = ShutdownHandler::new(server.sender());
    let port = port.unwrap_or(1736);

    let runtime = server.run_and_forget(port, Some(receiver));
    handler.set_runtime(runtime);
    Ok(handler)
}

#[pyfunction]
pub fn from_directory(py: Python, path: String, port: Option<u16>) -> PyResult<ShutdownHandler> {
    let (server, receiver) = RaphtoryServer::from_directory(path.as_str());
    let port = port.unwrap_or(1736);
    let mut handler = ShutdownHandler::new(server.sender());

    let runtime = server.run_and_forget(port, Some(receiver));
    handler.set_runtime(runtime);
    Ok(handler)
}

#[pyfunction]
pub fn from_map_and_directory(
    py: Python,
    graphs: HashMap<String, PyGraph>,
    path: String,
    port: Option<u16>,
) -> PyResult<ShutdownHandler> {
    let graphs: HashMap<String, DynamicGraph> = graphs
        .into_iter()
        .map(|(key, value)| (key, value.into()))
        .collect();
    let port = port.unwrap_or(1736);
    let (server, receiver) = RaphtoryServer::from_map_and_directory(graphs, path.as_str());
    let mut handler = ShutdownHandler::new(server.sender());
    let runtime = server.run_and_forget(port, Some(receiver));
    handler.set_runtime(runtime);
    Ok(handler)
}

#[pyclass]
pub struct ShutdownHandler {
    sender: Sender<()>,
    runtime: Option<TokioRuntime>,
}

#[pymethods]
impl ShutdownHandler {
    fn shutdown(&self) -> PyResult<()> {
        println!("Shutting down server");
        self.sender.send(()).map_err(|err| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to send shutdown signal: {}",
                err
            ))
        })?;
        Ok(())
    }
}

impl ShutdownHandler {
    fn new(sender: Sender<()>) -> Self {
        Self { sender, runtime: None }
    }

    fn set_runtime(&mut self, runtime: TokioRuntime) {
        self.runtime = Some(runtime);
    }
}
