use crate::{
    core::{
        utils::{
            errors::GraphError,
            time::{error::ParseTimeError, TryIntoTime},
        },
        PropType,
    },
    prelude::Prop,
};

mod addition_ops;
mod deletion_ops;
mod import_ops;
pub mod internal;
mod property_addition_ops;

pub use addition_ops::AdditionOps;
pub use deletion_ops::DeletionOps;
pub use import_ops::ImportOps;
pub use property_addition_ops::PropertyAdditionOps;

/// Used to handle automatic injection of secondary index if not explicitly provided
pub enum InputTime {
    Simple(i64),
    Indexed(i64, usize),
}

pub trait TryIntoInputTime {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError>;
}

impl TryIntoInputTime for InputTime {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError> {
        Ok(self)
    }
}

impl<T: TryIntoTime> TryIntoInputTime for T {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError> {
        Ok(InputTime::Simple(self.try_into_time()?))
    }
}

impl<T: TryIntoTime> TryIntoInputTime for (T, usize) {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError> {
        Ok(InputTime::Indexed(self.0.try_into_time()?, self.1))
    }
}

pub trait CollectProperties {
    fn collect_properties<F: Fn(&str, PropType) -> Result<usize, GraphError>, G: Fn(Prop) -> Prop>(
        self,
        id_resolver: F,
        value_processor: G,
    ) -> Result<Vec<(usize, Prop)>, GraphError>;
}

impl<S: AsRef<str>, P: Into<Prop>, PI> CollectProperties for PI
where
    PI: IntoIterator<Item = (S, P)>,
{
    fn collect_properties<F: Fn(&str, PropType) -> Result<usize, GraphError>, G: Fn(Prop) -> Prop>(
        self,
        id_resolver: F,
        value_processor: G,
    ) -> Result<Vec<(usize, Prop)>, GraphError>
    where
        PI: IntoIterator<Item = (S, P)>,
    {
        let mut properties: Vec<(usize, Prop)> = Vec::new();
        for (key, value) in self {
            let value: Prop = value.into();
            let prop_id = id_resolver(key.as_ref(), value.dtype())?;
            properties.push((prop_id, value_processor(value)));
        }
        Ok(properties)
    }
}
