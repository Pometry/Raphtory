use std::{ops::Deref, rc::Rc, sync::Arc};

pub mod edge_ops;
pub mod vertex_ops;

pub trait RefCount<T>: Clone + Deref {
    fn new(t: T) -> Self;
}

impl<T> RefCount<T> for Rc<T> {
    fn new(t: T) -> Self {
        Rc::new(t)
    }
}

impl<T> RefCount<T> for Arc<T> {
    fn new(t: T) -> Self {
        Arc::new(t)
    }
}
