use crate::core::{
    entities::{
        graph::tgraph::FxDashMap,
        properties::{
            props::{ArcReadLockedVec, DictMapper},
            tprop::TProp,
        },
    },
    storage::{lazy_vec::IllegalSet, locked_view::LockedView, timeindex::TimeIndexEntry},
    utils::errors::{GraphError, IllegalMutate, MutateGraphError},
    ArcStr, Prop,
};
use parking_lot::RwLockReadGuard;
use serde::{Deserialize, Serialize};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct GraphProps {
    constant_mapper: DictMapper,
    temporal_mapper: DictMapper,
    constant: FxDashMap<usize, Option<Prop>>,
    temporal: FxDashMap<usize, TProp>,
}

impl GraphProps {
    pub(crate) fn new() -> Self {
        Self {
            constant_mapper: DictMapper::default(),
            temporal_mapper: DictMapper::default(),
            constant: FxDashMap::default(),
            temporal: FxDashMap::default(),
        }
    }

    #[inline]
    pub(crate) fn resolve_property(&self, name: &str, is_static: bool) -> usize {
        if is_static {
            self.constant_mapper.get_or_create_id(name)
        } else {
            self.temporal_mapper.get_or_create_id(name)
        }
    }

    pub(crate) fn add_constant_prop(
        &self,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), MutateGraphError> {
        let mut prop_entry = self.constant.entry(prop_id).or_insert(None);
        match prop_entry.deref_mut() {
            Some(old_value) => {
                if !(old_value == &prop) {
                    return Err(MutateGraphError::IllegalGraphPropertyChange {
                        name: self
                            .constant_mapper
                            .get_name(prop_id)
                            .expect("property exists if it has a value")
                            .to_string(),
                        old_value: old_value.clone(),
                        new_value: prop,
                    });
                }
            }
            None => {
                (*prop_entry) = Some(prop);
            }
        }
        Ok(())
    }

    pub(crate) fn add_prop(
        &self,
        t: TimeIndexEntry,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), GraphError> {
        let mut prop_entry = self.temporal.entry(prop_id).or_insert(TProp::default());
        (*prop_entry).set(t, prop)
    }

    pub(crate) fn get_constant(&self, name: &str) -> Option<Prop> {
        let prop_id = self.constant_mapper.get_id(name)?;
        let entry = self.constant.get(&prop_id)?;
        entry.as_ref().cloned()
    }

    pub(crate) fn get_temporal(&self, name: &str) -> Option<LockedView<'_, TProp>> {
        let prop_id = self.temporal_mapper.get_id(&(name.to_owned()))?;
        let entry = self.temporal.get(&prop_id)?;
        Some(LockedView::DashMap(entry))
    }

    pub(crate) fn constant_names(&self) -> ArcReadLockedVec<ArcStr> {
        self.constant_mapper.get_keys()
    }

    pub(crate) fn temporal_names(&self) -> ArcReadLockedVec<ArcStr> {
        self.temporal_mapper.get_keys()
    }
}
