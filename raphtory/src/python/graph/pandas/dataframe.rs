use crate::core::utils::errors::GraphError;
use arrow2::{
    array::{Array, BooleanArray, PrimitiveArray, Utf8Array},
    ffi,
    offset::Offset,
    types::NativeType,
};
use itertools::Itertools;
use pyo3::{
    create_exception, exceptions::PyException, ffi::Py_uintptr_t, types::IntoPyDict, PyAny, PyErr,
    PyResult, Python,
};

pub(crate) struct PretendDF {
    pub(crate) names: Vec<String>,
    pub(crate) arrays: Vec<Vec<Box<dyn Array>>>,
}

impl PretendDF {
    pub fn check_cols_exist(&self, cols: &[&str]) -> Result<(), GraphError> {
        let non_cols: Vec<&&str> = cols
            .iter()
            .filter(|c| !self.names.contains(&c.to_string()))
            .collect();
        if non_cols.len() > 0 {
            return Err(GraphError::ColumnDoesNotExist(non_cols.iter().join(", ")));
        }

        Ok(())
    }

    pub(crate) fn iter_col<T: NativeType>(
        &self,
        name: &str,
    ) -> Option<impl Iterator<Item = Option<&T>> + '_> {
        let idx = self.names.iter().position(|n| n == name)?;

        let _ = (&self.arrays[0])[idx]
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()?;

        let iter = self.arrays.iter().flat_map(move |arr| {
            let arr = &arr[idx];
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
            arr.iter()
        });

        Some(iter)
    }

    pub fn utf8<O: Offset>(&self, name: &str) -> Option<impl Iterator<Item = Option<&str>> + '_> {
        let idx = self.names.iter().position(|n| n == name)?;
        // test that it's actually a utf8 array
        let _ = (&self.arrays[0])[idx]
            .as_any()
            .downcast_ref::<Utf8Array<O>>()?;

        let iter = self.arrays.iter().flat_map(move |arr| {
            let arr = &arr[idx];
            let arr = arr.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
            arr.iter()
        });

        Some(iter)
    }

    pub fn bool(&self, name: &str) -> Option<impl Iterator<Item = Option<bool>> + '_> {
        let idx = self.names.iter().position(|n| n == name)?;

        let _ = (&self.arrays[0])[idx]
            .as_any()
            .downcast_ref::<BooleanArray>()?;

        let iter = self.arrays.iter().flat_map(move |arr| {
            let arr = &arr[idx];
            let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.iter()
        });

        Some(iter)
    }
}

fn is_jupyter(py: Python) {
    let code = r#"
try:
    shell = get_ipython().__class__.__name__
    if shell == 'ZMQInteractiveShell':
        result = True   # Jupyter notebook or qtconsole
    elif shell == 'TerminalInteractiveShell':
        result = False  # Terminal running IPython
    else:
        result = False  # Other type, assuming not a Jupyter environment
except NameError:
    result = False      # Probably standard Python interpreter
"#;

    if let Err(e) = py.run(code, None, None) {
        println!("Error checking if running in a jupyter notebook: {}", e);
        return;
    }

    match py.eval("result", None, None) {
        Ok(x) => {
            if let Ok(x) = x.extract() {
                kdam::set_notebook(x);
            }
        }
        Err(e) => {
            println!("Error checking if running in a jupyter notebook: {}", e);
        }
    };
}

pub(crate) fn process_pandas_py_df(
    df: &PyAny,
    py: Python,
    _size: usize,
    col_names: Vec<&str>,
) -> PyResult<PretendDF> {
    is_jupyter(py);
    py.import("pandas")?;
    let module = py.import("pyarrow")?;
    let pa_table = module.getattr("Table")?;

    let df_columns: Vec<String> = df.getattr("columns")?.extract()?;

    let cols_to_drop: Vec<String> = df_columns
        .into_iter()
        .filter(|x| !col_names.contains(&x.as_str()))
        .collect();

    let dropped_df = if !cols_to_drop.is_empty() {
        let drop_method = df.getattr("drop")?;
        drop_method.call((cols_to_drop,), Some(vec![("axis", 1)].into_py_dict(py)))?
    } else {
        df
    };

    let _df_columns: Vec<String> = dropped_df.getattr("columns")?.extract()?;

    let table = pa_table.call_method("from_pandas", (dropped_df,), None)?;

    let rb = table.call_method0("to_batches")?.extract::<Vec<&PyAny>>()?;
    let names: Vec<String> = if let Some(batch0) = rb.get(0) {
        let schema = batch0.getattr("schema")?;
        schema.getattr("names")?.extract::<Vec<String>>()?
    } else {
        vec![]
    }
    .into_iter()
    .filter(|x| col_names.contains(&x.as_str()))
    .collect();

    let arrays = rb
        .iter()
        .map(|rb| {
            (0..names.len())
                .map(|i| {
                    let array = rb.call_method1("column", (i,))?;
                    let arr = array_to_rust(array)?;
                    Ok::<Box<dyn Array>, PyErr>(arr)
                })
                .collect::<Result<Vec<_>, PyErr>>()
        })
        .collect::<Result<Vec<_>, PyErr>>()?;

    let df = PretendDF { names, arrays };
    Ok(df)
}

// pub(crate) fn process_pandas_py_df(df: &PyAny, py: Python, _size: usize,col_names:Vec<&str>) -> PyResult<PretendDF> {
//     is_jupyter(py);
//     let globals = PyDict::new(py);
//     globals.set_item("df", df)?;
//     let module = py.import("pyarrow")?;
//     let pa_table = module.getattr("Table")?;
//
//     let table = pa_table.call_method("from_pandas", (df,), None)?;
//
//     let rb = table.call_method0("to_batches")?.extract::<Vec<&PyAny>>()?;
//     let names:Vec<String> = if let Some(batch0) = rb.get(0) {
//         let schema = batch0.getattr("schema")?;
//         schema.getattr("names")?.extract::<Vec<String>>()?
//     } else {
//         vec![]
//     }.into_iter().filter(|x| col_names.contains(&x.as_str())).collect();
//
//     let arrays = rb
//         .iter()
//         .map(|rb| {
//             (0..names.len())
//                 .map(|i| {
//                     let array = rb.call_method1("column", (i,))?;
//                     let arr = array_to_rust(array)?;
//                     Ok::<Box<dyn Array>, PyErr>(arr)
//                 })
//                 .collect::<Result<Vec<_>, PyErr>>()
//         })
//         .collect::<Result<Vec<_>, PyErr>>()?;
//
//     let df = PretendDF { names, arrays };
//     Ok(df)
// }

pub fn array_to_rust(obj: &PyAny) -> PyResult<ArrayRef> {
    // prepare a pointer to receive the Array struct
    let array = Box::new(ffi::ArrowArray::empty());
    let schema = Box::new(ffi::ArrowSchema::empty());

    let array_ptr = &*array as *const ffi::ArrowArray;
    let schema_ptr = &*schema as *const ffi::ArrowSchema;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    obj.call_method1(
        "_export_to_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    unsafe {
        let field = ffi::import_field_from_c(schema.as_ref())
            .map_err(|e| ArrowErrorException::new_err(format!("{:?}", e)))?;
        let array = ffi::import_array_from_c(*array, field.data_type)
            .map_err(|e| ArrowErrorException::new_err(format!("{:?}", e)))?;
        Ok(array)
    }
}

pub type ArrayRef = Box<dyn Array>;

create_exception!(exceptions, ArrowErrorException, PyException);
create_exception!(exceptions, GraphLoadException, PyException);
