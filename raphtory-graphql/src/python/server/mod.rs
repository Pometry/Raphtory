use crate::python::{server::running_server::ServerHandler, RUNNING_SERVER_CONSUMED_MSG};
use pyo3::{exceptions::PyException, PyResult};
use raphtory_api::python::error::adapt_err_value;

pub mod running_server;
pub mod server;

pub(crate) enum BridgeCommand {
    StopServer,
    StopListening,
}

pub(crate) fn wait_server(running_server: &mut Option<ServerHandler>) -> PyResult<()> {
    let owned_running_server = running_server
        .take()
        .ok_or_else(|| PyException::new_err(RUNNING_SERVER_CONSUMED_MSG))?;
    owned_running_server
        .join_handle
        .join()
        .expect("error when waiting for the server thread to complete")
        .map_err(|e| adapt_err_value(&e))
}

pub(crate) fn is_online(url: &String) -> bool {
    reqwest::blocking::get(url)
        .map(|response| response.status().as_u16() == 200)
        .unwrap_or(false)
}
