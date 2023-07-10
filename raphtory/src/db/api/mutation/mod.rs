mod addition_ops;
mod deletion_ops;
pub mod internal;
mod property_addition_ops;

pub use addition_ops::AdditionOps;
pub use deletion_ops::DeletionOps;
pub use property_addition_ops::PropertyAdditionOps;

use crate::prelude::Prop;

pub trait Properties {
    fn collect_properties(self) -> Vec<(String, Prop)>;
}

impl<S:AsRef<str>, P:Into<Prop>, PI> Properties for PI
where
    PI: IntoIterator<Item = (S, P)>,
{
    fn collect_properties(self) -> Vec<(String, Prop)> {
        self.into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.into()))
            .collect()
    }
}