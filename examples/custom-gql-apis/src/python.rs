use crate::{mutation::HelloMutation, query::HelloQuery};
use pyo3::prelude::*;
use raphtory_graphql::server::{register_mutation_plugin, register_query_plugin};

#[pyfunction(name = "add_custom_gql_apis")]
pub fn py_add_custom_gql_apis() -> Result<(), PyErr> {
    register_query_plugin::<_, HelloQuery>("hello_query");
    register_mutation_plugin::<_, HelloMutation>("hello_mutation");
    Ok(())
}
