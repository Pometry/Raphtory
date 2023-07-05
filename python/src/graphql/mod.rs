use std::collections::HashMap;
use pyo3::prelude::*;
use raphtory_core::prelude::*;
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

    pub async fn run(self, port: Option<u16>) -> std::io::Result<()> {
        match port {
            Some(port) => self.server.run_with_port(port).await,
            None => self.server.run().await
        }
    }
}