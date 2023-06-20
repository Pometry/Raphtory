// Internal macro for generating the iterator struct (with or without name)
macro_rules! _py_iterator_struct {
    ($name:ident, $pyitem:ty) => {
        #[pyclass]
        pub struct $name {
            iter: Box<dyn Iterator<Item = $pyitem> + Send>,
        }
    };
    ($name:ident, $pyname:literal, $pyitem:ty) => {
        #[pyclass(name=$pyname)]
        pub struct $name {
            iter: Box<dyn Iterator<Item = $pyitem> + Send>,
        }
    };
}

// internal macro for adding methods to iterators
macro_rules! _py_iterator_methods {
    ($name:ident, $item:ty, $pyitem:ty) => {
        #[pymethods]
        impl $name {
            fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }
            fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<$pyitem> {
                slf.iter.next()
            }
        }

        impl From<Box<dyn Iterator<Item = $item> + Send>> for $name {
            fn from(value: Box<dyn Iterator<Item = $item> + Send>) -> Self {
                let iter = Box::new(value.map(|v| v.into()));
                Self { iter }
            }
        }

        impl IntoIterator for $name {
            type Item = $pyitem;
            type IntoIter = Box<dyn Iterator<Item = $pyitem> + Send>;

            fn into_iter(self) -> Self::IntoIter {
                self.iter
            }
        }
    };
}

/// Construct a python Iterator struct
///
/// # Arguments
///
/// * `name` - The identifier for the new struct
/// * `item` - The type of `Item` for the wrapped iterator
/// * `pyitem` - The type of the python wrapper for `Item` (optional if `item` implements `IntoPy`)
/// * `pyname` - The python-side name for the iterator (optional, defaults to `name`)
macro_rules! py_iterator {
    ($name:ident, $item:ty) => {
        _py_iterator_struct!($name, $item);
        _py_iterator_methods!($name, $item, $item);
    };
    ($name:ident, $item:ty, $pyitem:ty) => {
        _py_iterator_struct!($name, $pyitem);
        _py_iterator_methods!($name, $item, $pyitem);
    };
    ($name:ident, $item:ty, $pyitem:ty, $pyname:literal) => {
        _py_iterator_struct!($name, $pyname, $pyitem);
        _py_iterator_methods!($name, $item, $pyitem);
    };
}
