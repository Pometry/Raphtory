use crate::{
    entities::properties::tprop::{IllegalPropType, TProp},
    storage::{lazy_vec::IllegalSet, TPropColumnError},
};
use raphtory_api::core::entities::properties::prop::Prop;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TPropError {
    #[error(transparent)]
    IllegalSet(#[from] IllegalSet<TProp>),
    #[error(transparent)]
    IllegalPropType(#[from] IllegalPropType),
}

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("Attempted to change value of metadata, old: {old}, new: {new}")]
    IllegalUpdate { old: Prop, new: Prop },

    #[error(transparent)]
    IllegalPropType(#[from] IllegalPropType),
}

impl From<IllegalSet<Option<Prop>>> for MetadataError {
    fn from(value: IllegalSet<Option<Prop>>) -> Self {
        let old = value.previous_value.unwrap_or(Prop::str("NONE"));
        let new = value.new_value.unwrap_or(Prop::str("NONE"));
        MetadataError::IllegalUpdate { old, new }
    }
}

impl From<TPropColumnError> for MetadataError {
    fn from(value: TPropColumnError) -> Self {
        match value {
            TPropColumnError::IllegalSet(inner) => {
                let old = inner.previous_value;
                let new = inner.new_value;
                MetadataError::IllegalUpdate { old, new }
            }
            TPropColumnError::IllegalType(inner) => MetadataError::IllegalPropType(inner),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use raphtory_api::core::entities::properties::meta::PropMapper;
    use std::{sync::Arc, thread};

    #[test]
    fn test_prop_mapper_concurrent() {
        let values = [Prop::I64(1), Prop::U16(0), Prop::Bool(true), Prop::F64(0.0)];
        let input_len = values.len();

        let mapper = Arc::new(PropMapper::default());
        let threads: Vec<_> = values
            .into_iter()
            .map(move |v| {
                let mapper = mapper.clone();
                thread::spawn(move || mapper.get_or_create_and_validate("test", v.dtype()))
            })
            .flat_map(|t| t.join())
            .collect();

        assert_eq!(threads.len(), input_len); // no errors
        assert_eq!(threads.into_iter().flatten().count(), 1); // only one result (which ever was first)
    }
}
