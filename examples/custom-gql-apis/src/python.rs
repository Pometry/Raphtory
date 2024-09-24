use crate::{mutation::HelloMutation, query::HelloQuery};
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use raphtory_core::{
    py_algorithm_result_base,
    python::types::repr::{Repr, StructReprBuilder},
};
use raphtory_graphql::python::server::{server::PyGraphServer, take_server_ownership};

#[pyfunction(name = "add_custom_gql_apis")]
pub fn py_add_custom_gql_apis(server: PyRefMut<PyGraphServer>) -> Result<PyGraphServer, PyErr> {
    let server = take_server_ownership(server)?;
    let server = server.register_query_plugin::<_, HelloQuery>("hello_query");
    let server = server.register_mutation_plugin::<_, HelloMutation>("hello_mutation");
    Ok(PyGraphServer::new(server))
}
