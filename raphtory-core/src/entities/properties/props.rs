use crate::{
    entities::properties::tprop::{IllegalPropType, TProp},
    storage::{
        lazy_vec::{IllegalSet, LazyVec},
        timeindex::TimeIndexEntry,
    },
};
use raphtory_api::core::entities::properties::prop::Prop;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Serialize, Deserialize, Default, Debug, PartialEq)]
pub struct Props {
    // properties
    pub(crate) metadata: LazyVec<Option<Prop>>,
    pub(crate) temporal_props: LazyVec<TProp>,
}

#[derive(Error, Debug)]
pub enum TPropError {
    #[error(transparent)]
    IllegalSet(#[from] IllegalSet<TProp>),
    #[error(transparent)]
    IllegalPropType(#[from] IllegalPropType),
}

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("Attempted to change value of constant property, old: {old}, new: {new}")]
    IllegalUpdate { old: Prop, new: Prop },
}

impl From<IllegalSet<Option<Prop>>> for MetadataError {
    fn from(value: IllegalSet<Option<Prop>>) -> Self {
        let old = value.previous_value.unwrap_or(Prop::str("NONE"));
        let new = value.new_value.unwrap_or(Prop::str("NONE"));
        MetadataError::IllegalUpdate { old, new }
    }
}

impl Props {
    pub fn new() -> Self {
        Self {
            metadata: Default::default(),
            temporal_props: Default::default(),
        }
    }

    pub fn add_prop(
        &mut self,
        t: TimeIndexEntry,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), TPropError> {
        self.temporal_props.update(prop_id, |p| Ok(p.set(t, prop)?))
    }

    pub fn add_metadata(&mut self, prop_id: usize, prop: Prop) -> Result<(), MetadataError> {
        Ok(self.metadata.set(prop_id, Some(prop))?)
    }

    pub fn update_metadata(&mut self, prop_id: usize, prop: Prop) -> Result<(), MetadataError> {
        self.metadata.update(prop_id, |n| {
            *n = Some(prop);
            Ok(())
        })
    }

    pub fn metadata(&self, prop_id: usize) -> Option<&Prop> {
        let prop = self.metadata.get(prop_id)?;
        prop.as_ref()
    }

    pub fn temporal_prop(&self, prop_id: usize) -> Option<&TProp> {
        self.temporal_props.get(prop_id)
    }

    pub fn metadata_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.metadata.filled_ids()
    }

    pub fn temporal_prop_ids(&self) -> impl Iterator<Item = usize> + Send + Sync + '_ {
        self.temporal_props.filled_ids()
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
