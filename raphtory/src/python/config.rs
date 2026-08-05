use pyo3::{Borrowed, BoundObject, FromPyObject, PyAny};
use pythonize::{depythonize, PythonizeError};
use storage::{persist::args::ArgsOps, Args};

pub struct PyArgs(pub Args);

impl<'a, 'py> FromPyObject<'a, 'py> for PyArgs {
    type Error = PythonizeError;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let args: Args = depythonize(&obj.into_bound())?;
        let mut args_from_env = Args::from_env();
        // sync args_with_env base config with args since args_from_env.update does not do it
        if let Some(max_node_page_len) = args.max_node_page_len() {
            args_from_env = args_from_env.with_max_node_page_len(max_node_page_len);
        }
        if let Some(max_edge_page_len) = args.max_edge_page_len() {
            args_from_env = args_from_env.with_max_edge_page_len(max_edge_page_len);
        }

        // Read values from env first, then apply args on top.
        args_from_env.update(args);
        Ok(PyArgs(args_from_env))
    }
}
