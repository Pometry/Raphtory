#[cfg(not(any(test, feature = "panic-on-drop")))]
pub use tracing::error;

#[cfg(any(test, feature = "panic-on-drop"))]
#[macro_export]
macro_rules! drop_error {
    ($($arg:tt)*) => {{
        panic!($($arg)*)
    }};
}

#[cfg(not(any(test, feature = "panic-on-drop")))]
#[macro_export]
macro_rules! drop_error {
    ($($arg:tt)*) => {{
        $crate::error!($($arg)*)
    }};
}

#[test]
#[should_panic]
fn test_drop_error() {
    drop_error!("failed");
}
