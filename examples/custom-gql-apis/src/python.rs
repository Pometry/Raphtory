use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use raphtory_core::py_algorithm_result_base;
use raphtory_core::python::types::repr::{Repr, StructReprBuilder};
use raphtory_graphql::python::server::server::PyGraphServer;
use raphtory_graphql::python::server::take_server_ownership;
use crate::hello_world::HelloWorld;

#[pyfunction(name = "add_custom_gql_apis")]
pub fn py_add_custom_gql_apis(
    server: PyRefMut<PyGraphServer>,
) -> Result<PyGraphServer, PyErr> {
    let server = take_server_ownership(server)?;
    let server = server.register_query_plugin::<_, HelloWorld>("hello_world");
    Ok(PyGraphServer::new(server))
}
