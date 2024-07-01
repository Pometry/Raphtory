#[macro_export]
macro_rules! add_functions {
    ($module:expr, $($func:ident),* $(,)?) => {
        $(
            $module.add_function(pyo3::wrap_pyfunction!($func, $module)?)?;
        )*
    };
}
#[macro_export]
macro_rules! add_classes {
    ($module:expr, $($cls:ty),* $(,)?) => {
        $(
            $module.add_class::<$cls>()?;
        )*
    };
}