use pyo3::prelude::*;
use raphtory_core::{prelude::*, python::utils::errors::adapt_err_value};
use raphtory_graphql::RaphtoryServer;
use std::collections::HashMap;


#[pyfunction]
pub fn run_from_dict(py: Python, graphs: HashMap<String, Graph>) -> PyResult<&PyAny> {
    let server = RaphtoryServer::from_map(graphs);
    pyo3_asyncio::tokio::future_into_py(py, async {
        server.run().await
        .map_err(|e| adapt_err_value(&e))
    })
}

#[pyfunction]
pub fn run_from_file(py: Python, path: String) -> PyResult<&PyAny> {
    let server = RaphtoryServer::new(path.as_str());
    pyo3_asyncio::tokio::future_into_py(py, async {
        server.run().await
            .map_err(|e| adapt_err_value(&e))
    })
}
    //
    // #[staticmethod]
    // pub fn run_from_file(py: Python, path: &str, port: Option<u16>) {
    //     let server = RaphtoryServer::new(path);
    //     future_into_py(py, async move {
    //         match port {
    //             Some(port) => server.run_with_port(port).await,
    //             None => server.run().await,
    //         }
    //         .map_err(|e| adapt_err_value(&e))
    //     })
    //     .expect("Failed to run server");
    // }

