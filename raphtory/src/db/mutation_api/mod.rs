mod addition_ops;
mod deletion_ops;
pub mod internal;
mod property_addition_ops;

use crate::core::Prop;
pub use addition_ops::AdditionOps;
pub use deletion_ops::DeletionOps;
pub use property_addition_ops::PropertyAdditionOps;

pub trait Properties {
    fn collect_properties(self) -> Vec<(String, Prop)>;
}

impl<T> Properties for T
where
    T: IntoIterator<Item = (String, Prop)>,
{
    fn collect_properties(self) -> Vec<(String, Prop)> {
        self.into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect()
    }
}
