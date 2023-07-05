use std::collections::HashMap;
use std::fmt::Error;
use pyo3::prelude::*;
use raphtory_core::prelude::*;
use raphtory_core::python::utils::errors::adapt_err_value;
use raphtory_graphql::RaphtoryServer;
/// The Raphtory GraphQL server
#[pyclass(name = "RaphtoryServer")]
pub struct PyServer {
    server: RaphtoryServer,
}

#[pymethods]
impl PyServer {
    pub fn new(graphs: HashMap<String,Graph>) -> Self {
        let server = RaphtoryServer::from_map(graphs);
        Self { server }
    }

    pub fn from_file(path: &str) -> Self {
        let server = RaphtoryServer::new(path);
        Self { server }
    }

    pub fn run(&self, py: Python, port: Option<u16>) -> PyResult<&PyAny> {
        pyo3_asyncio::tokio::future_into_py(py, async move {
            match port {
                Some(port) => self.server.run_with_port(port).await.map_err(|&e| adapt_err_value(e)),
                None => self.server.run().await.map_err(&|e|adapt_err_value(e))
            }
        })



    }
}