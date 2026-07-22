use pyo3::{
    types::{PyAnyMethods, PyDict, PyDictMethods, PyListMethods},
    Borrowed, BoundObject, FromPyObject, PyAny,
};
use pythonize::{depythonize, PythonizeError};
use storage::Config;
use std::collections::{HashMap, HashSet};

pub struct PyConfig(pub Config); 


const CONFIG_SECTIONS: &[&str] = &["persistence", "merge"];

impl<'a, 'py> FromPyObject<'a, 'py> for PyConfig {
    type Error = PythonizeError;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let mut attributes_specified: HashMap<String, HashSet<String>> = HashMap::new();

        if let Ok(dict) = obj.cast::<PyDict>() {
            for section in CONFIG_SECTIONS {
                if let Ok(Some(nested)) = dict.get_item(section) {
                    if let Ok(nested_dict) = nested.cast::<PyDict>() {
                        let keys = nested_dict
                            .keys()
                            .iter()
                            .filter_map(|k| k.extract::<String>().ok())
                            .collect();
                        attributes_specified.insert(section.to_string(), keys);
                    }
                }
            }
        }
        let mut config: Config = depythonize(&obj.into_bound())?;
        config.attributes_specified = attributes_specified;
        Ok(PyConfig(config))
    }
}
