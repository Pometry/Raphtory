#[cfg(feature = "dhat-heap")]
use std::sync::Mutex;

// #[cfg(not(target_env = "msvc"))]
// use tikv_jemallocator::Jemalloc;
// #[cfg(not(target_env = "msvc"))]
// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use clam_core::python::py_gql::base_gql_module;
use pyo3::prelude::*;
use raphtory::python::{
    filter::base_filter_module,
    graph::node_state::base_node_state_module,
    packages::base_modules::{
        add_raphtory_classes, base_algorithm_module, base_graph_gen_module,
        base_graph_loader_module, base_iterables_module, base_vectors_module,
    },
};
use raphtory_graphql::python::pymodule::base_graphql_module;

#[cfg(feature = "dhat-heap")]
static PROFILER: Mutex<Option<dhat::Profiler>> = Mutex::new(None);


#[cfg(feature = "dhat-heap")]
#[pyfunction]
fn stop_profiler() {
    println!("Dropping profiler...");
    let mut lock = PROFILER.lock().unwrap();
    *lock = None; // this actually runs Drop
}

#[cfg(feature = "dhat-heap")]
extern "C" fn on_exit() {
    {
        println!("atexit: dropping profiler");
        let mut lock = PROFILER.lock().unwrap();
        *lock = None; // triggers Drop
    }
}


/// Raphtory graph analytics library
#[pymodule]
fn _raphtory(py: Python<'_>, m: &Bound<PyModule>) -> PyResult<()> {
    let _ = add_raphtory_classes(m);
    #[cfg(feature = "dhat-heap")]
    {
        *PROFILER.lock().unwrap() = Some(dhat::Profiler::new_heap());
        unsafe { libc::atexit(on_exit); }
        m.add_function(wrap_pyfunction!(stop_profiler, m)?)?;
    }


    let graphql_module = base_graphql_module(py)?;
    let algorithm_module = base_algorithm_module(py)?;
    let graph_loader_module = base_graph_loader_module(py)?;
    let graph_gen_module = base_graph_gen_module(py)?;
    let vectors_module = base_vectors_module(py)?;
    let node_state_module = base_node_state_module(py)?;
    let filter_module = base_filter_module(py)?;
    let iterables = base_iterables_module(py)?;
    m.add_submodule(&graphql_module)?;
    m.add_submodule(&algorithm_module)?;
    m.add_submodule(&graph_loader_module)?;
    m.add_submodule(&graph_gen_module)?;
    m.add_submodule(&vectors_module)?;
    m.add_submodule(&node_state_module)?;
    m.add_submodule(&filter_module)?;
    m.add_submodule(&iterables)?;

    let gql_module = base_gql_module(py)?;
    m.add_submodule(&gql_module)?;

    Ok(())
}
