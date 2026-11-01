use crate::errors::GraphError;
use parking_lot::Mutex;
use std::{
    any::Any,
    sync::atomic::{AtomicBool, Ordering},
};

/// The first error any thread of the load hit; the others stop taking new work once it's set.
#[derive(Default)]
pub(super) struct Failure {
    first: Mutex<Option<GraphError>>,
    happened: AtomicBool,
}

impl Failure {
    pub fn record(&self, error: GraphError) {
        self.happened.store(true, Ordering::Relaxed);
        self.first.lock().get_or_insert(error);
    }

    pub fn record_panic(&self, thread: &str, panic: &(dyn Any + Send)) {
        let message = panic
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".to_string());
        self.record(GraphError::LoadFailure(format!(
            "{thread} panicked: {message}"
        )));
    }

    pub fn has_happened(&self) -> bool {
        self.happened.load(Ordering::Relaxed)
    }

    pub fn into_result(self) -> Result<(), GraphError> {
        match self.first.into_inner() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
