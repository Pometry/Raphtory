use std::collections::HashMap;
use pyo3::prelude::*;
use raphtory_core::prelude::*;
use raphtory_graphql::RaphtoryServer;

/// The Raphtory GraphQL server
#[pyclass(name = "RaphtoryServer")]
#[derive(Clone)]
pub struct PyServer {
    server: RaphtoryServer,
}

impl PyServer {
    pub fn new(graphs: HashMap<String,Graph>) -> Self {
        let server = RaphtoryServer::from_map(graphs);
        Self { server }
    }

    pub fn from_file(path: &str) -> Self {
        let server = RaphtoryServer::new(path);
        Self { server }
    }

    pub fn run(&self, port: Option<u16>) {
        match port {
            Some(port) => self.server.run_with_port(port),
            None => self.server.run()
        }
    }

    pub fn

}