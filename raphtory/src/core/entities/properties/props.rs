use crate::{
    core::{
        entities::properties::tprop::TProp,
        storage::{
            lazy_vec::{IllegalSet, LazyVec},
            timeindex::TimeIndexEntry,
        },
        utils::errors::GraphError,
        Prop,
    },
    db::api::storage::graph::tprop_storage_ops::TPropOps,
};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, hash::Hash};

pub use raphtory_api::core::entities::properties::props::*;

#[derive(Serialize, Deserialize, Default, Debug, PartialEq)]
pub struct Props {
    // properties
    pub(crate) constant_props: LazyVec<Option<Prop>>,
    pub(crate) temporal_props: LazyVec<TProp>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
enum PropId {
    Static(usize),
    Temporal(usize),
}

impl Props {
    pub fn new() -> Self {
        Self {
            constant_props: Default::default(),
            temporal_props: Default::default(),
        }
    }

    pub fn add_prop(
        &mut self,
        t: TimeIndexEntry,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), GraphError> {
        self.temporal_props.update(prop_id, |p| p.set(t, prop))
    }

    pub fn add_constant_prop(
        &mut self,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), IllegalSet<Option<Prop>>> {
        self.constant_props.set(prop_id, Some(prop))
    }

    pub fn update_constant_prop(&mut self, prop_id: usize, prop: Prop) -> Result<(), GraphError> {
        self.constant_props.update(prop_id, |n| {
            *n = Some(prop);
            Ok(())
        })
    }

    pub fn temporal_props(&self, prop_id: usize) -> Box<dyn Iterator<Item = (i64, Prop)> + '_> {
        let o = self.temporal_props.get(prop_id);
        if let Some(t_prop) = o {
            Box::new(t_prop.iter_t())
        } else {
            Box::new(std::iter::empty())
        }
    }

    pub fn temporal_props_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> Box<dyn Iterator<Item = (i64, Prop)> + '_> {
        let o = self.temporal_props.get(prop_id);
        if let Some(t_prop) = o {
            Box::new(t_prop.iter_window_t(start..end))
        } else {
            Box::new(std::iter::empty())
        }
    }

    pub fn const_prop(&self, prop_id: usize) -> Option<&Prop> {
        let prop = self.constant_props.get(prop_id)?;
        prop.as_ref()
    }

    pub fn temporal_prop(&self, prop_id: usize) -> Option<&TProp> {
        self.temporal_props.get(prop_id)
    }

    pub fn const_prop_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.constant_props.filled_ids()
    }

    pub fn temporal_prop_ids(&self) -> impl Iterator<Item = usize> + Send + Sync + '_ {
        self.temporal_props.filled_ids()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::core::entities::properties::props::PropMapper;
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
