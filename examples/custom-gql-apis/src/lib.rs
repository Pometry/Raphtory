use crate::{
    mutation::HelloMutation,
    query::{FancyAlgorithm, HelloQuery},
};
use dynamic_graphql::internal::Registry;
use pyo3::prelude::*;
use raphtory::python::{
    filter::base_filter_module,
    packages::base_modules::{
        add_raphtory_classes, base_algorithm_module, base_graph_gen_module,
        base_graph_loader_module,
    },
};
use raphtory_graphql::{
    plugin::schema::{register_schema_plugin, RegisterPlugin},
    python::pymodule::base_graphql_module,
};

mod mutation;
mod query;

#[derive(Clone)]
struct SchemaPlugin;

impl RegisterPlugin for SchemaPlugin {
    fn register(&self, registry: Registry) -> Registry {
        registry
            .register::<HelloMutation>()
            .register::<HelloQuery<'static>>()
            .register::<FancyAlgorithm<'static>>()
    }
}

#[pymodule]
fn _raphtory_custom(py: Python<'_>, m: &Bound<PyModule>) -> PyResult<()> {
    let _ = add_raphtory_classes(m);

    register_schema_plugin(SchemaPlugin);

    let graphql_module = base_graphql_module(py)?;
    let algorithm_module = base_algorithm_module(py)?;
    let graph_loader_module = base_graph_loader_module(py)?;
    let graph_gen_module = base_graph_gen_module(py)?;
    let filter_module = base_filter_module(py)?;
    m.add_submodule(&graphql_module)?;
    m.add_submodule(&algorithm_module)?;
    m.add_submodule(&graph_loader_module)?;
    m.add_submodule(&graph_gen_module)?;
    m.add_submodule(&filter_module)?;

    Ok(())
}
