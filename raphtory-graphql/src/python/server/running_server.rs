use crate::python::{
    client::raphtory_client::PyRaphtoryClient,
    server::{is_online, wait_server, BridgeCommand},
    RUNNING_SERVER_CONSUMED_MSG, WAIT_CHECK_INTERVAL_MILLIS,
};
use crossbeam_channel::Sender as CrossbeamSender;
use pyo3::{exceptions::PyException, pyclass, pymethods, Py, PyObject, PyResult, Python};
use std::{
    thread::{sleep, JoinHandle},
    time::Duration,
};
use tokio::{self, io::Result as IoResult};
use tracing::error;

/// A Raphtory server handler that also enables querying the server
#[pyclass(name = "RunningGraphServer", module = "raphtory.graphql")]
pub struct PyRunningGraphServer {
    pub(crate) server_handler: Option<ServerHandler>,
}

pub(crate) struct ServerHandler {
    pub(crate) join_handle: JoinHandle<IoResult<()>>,
    sender: CrossbeamSender<BridgeCommand>,
    port: u16,
}

impl PyRunningGraphServer {
    pub(crate) fn new(
        join_handle: JoinHandle<IoResult<()>>,
        sender: CrossbeamSender<BridgeCommand>,
        port: u16,
    ) -> PyResult<Self> {
        let server_handler = Some(ServerHandler {
            join_handle,
            sender,
            port,
        });
        Ok(PyRunningGraphServer { server_handler })
    }

    fn apply_if_alive<O, F>(&self, function: F) -> PyResult<O>
    where
        F: FnOnce(&ServerHandler) -> PyResult<O>,
    {
        match &self.server_handler {
            Some(handler) => function(handler),
            None => Err(PyException::new_err(RUNNING_SERVER_CONSUMED_MSG)),
        }
    }

    pub(crate) fn wait_for_server_online(url: &String, timeout_ms: u64) -> PyResult<()> {
        let num_intervals = timeout_ms / WAIT_CHECK_INTERVAL_MILLIS;

        for _ in 0..num_intervals {
            if is_online(url) {
                return Ok(());
            } else {
                sleep(Duration::from_millis(WAIT_CHECK_INTERVAL_MILLIS))
            }
        }

        Err(PyException::new_err(format!(
            "Failed to start server in {} milliseconds",
            timeout_ms
        )))
    }

    pub(crate) fn stop_server(&mut self, py: Python) -> PyResult<()> {
        Self::apply_if_alive(self, |handler| {
            match handler.sender.send(BridgeCommand::StopServer) {
                Ok(_) => {}
                Err(e) => {
                    error!("Failed to establish Channel with server, this could be because you already have Raphtory running on this port. {}",e.to_string())
                }
            }
            Ok(())
        })?;
        let server = &mut self.server_handler;
        py.allow_threads(|| wait_server(server))
    }
}

#[pymethods]
impl PyRunningGraphServer {
    /// Get the client for the server
    ///
    /// Returns:
    /// RaphtoryClient: the client
    pub(crate) fn get_client(&self) -> PyResult<PyRaphtoryClient> {
        // TODO: return an authenticated server with rw access to everything?
        self.apply_if_alive(|handler| {
            let port = handler.port;
            let url = format!("http://localhost:{port}");
            Ok(PyRaphtoryClient::new(url, None)?)
        })
    }

    /// Stop the server and wait for it to finish
    ///
    /// Returns:
    /// None:
    pub(crate) fn stop(&mut self, py: Python) -> PyResult<()> {
        self.stop_server(py)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        py: Python,
        _exc_type: PyObject,
        _exc_val: PyObject,
        _exc_tb: PyObject,
    ) -> PyResult<()> {
        self.stop_server(py)
    }
}
