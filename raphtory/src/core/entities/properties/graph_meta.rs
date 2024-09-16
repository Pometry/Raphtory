use crate::core::{
    entities::properties::{props::PropMapper, tprop::TProp},
    storage::{locked_view::LockedView, timeindex::TimeIndexEntry},
    utils::errors::{GraphError, MutateGraphError},
    Prop, PropType,
};
use raphtory_api::core::storage::{
    arc_str::ArcStr,
    dict_mapper::{DictMapper, MaybeNew},
    locked_vec::ArcReadLockedVec,
    FxDashMap,
};
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

#[derive(Serialize, Deserialize, Debug)]
pub struct GraphMeta {
    constant_mapper: DictMapper,
    temporal_mapper: PropMapper,
    constant: FxDashMap<usize, Option<Prop>>,
    temporal: FxDashMap<usize, TProp>,
}

impl GraphMeta {
    pub(crate) fn new() -> Self {
        Self {
            constant_mapper: DictMapper::default(),
            temporal_mapper: PropMapper::default(),
            constant: FxDashMap::default(),
            temporal: FxDashMap::default(),
        }
    }

    pub fn deep_clone(&self) -> Self {
        Self {
            constant_mapper: self.constant_mapper.deep_clone(),
            temporal_mapper: self.temporal_mapper.deep_clone(),
            constant: self.constant.clone(),
            temporal: self.temporal.clone(),
        }
    }

    #[inline]
    pub fn const_prop_meta(&self) -> &DictMapper {
        &self.constant_mapper
    }

    #[inline]
    pub fn temporal_prop_meta(&self) -> &PropMapper {
        &self.temporal_mapper
    }

    #[inline]
    pub(crate) fn resolve_property(
        &self,
        name: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        if is_static {
            Ok(self.constant_mapper.get_or_create_id(name))
        } else {
            self.temporal_mapper.get_or_create_and_validate(name, dtype)
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
                        name: self.constant_mapper.get_name(prop_id).to_string(),
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

    pub(crate) fn update_constant_prop(
        &self,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), MutateGraphError> {
        let mut prop_entry = self.constant.entry(prop_id).or_insert(None);
        (*prop_entry) = Some(prop);
        Ok(())
    }

    pub(crate) fn add_prop(
        &self,
        t: TimeIndexEntry,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), GraphError> {
        let mut prop_entry = self.temporal.entry(prop_id).or_default();
        (*prop_entry).set(t, prop)
    }

    pub(crate) fn get_constant(&self, id: usize) -> Option<Prop> {
        let entry = self.constant.get(&id)?;
        entry.as_ref().cloned()
    }

    pub(crate) fn get_temporal_prop(&self, prop_id: usize) -> Option<LockedView<'_, TProp>> {
        let entry = self.temporal.get(&prop_id)?;
        Some(LockedView::DashMap(entry))
    }

    pub fn get_const_prop_id(&self, name: &str) -> Option<usize> {
        self.constant_mapper.get_id(name)
    }

    pub fn get_temporal_id(&self, name: &str) -> Option<usize> {
        self.temporal_mapper.get_id(name)
    }

    pub fn get_const_prop_name(&self, prop_id: usize) -> ArcStr {
        self.constant_mapper.get_name(prop_id).clone()
    }

    pub fn get_temporal_name(&self, prop_id: usize) -> ArcStr {
        self.temporal_mapper.get_name(prop_id).clone()
    }

    pub fn get_constant_dtype(&self, prop_id: usize) -> Option<PropType> {
        self.constant
            .get(&prop_id)
            .and_then(|v| v.as_ref().map(|v| v.dtype()))
    }

    pub fn get_temporal_dtype(&self, prop_id: usize) -> Option<PropType> {
        self.temporal_mapper.get_dtype(prop_id)
    }

    pub(crate) fn constant_names(&self) -> ArcReadLockedVec<ArcStr> {
        self.constant_mapper.get_keys()
    }

    pub(crate) fn const_prop_ids(&self) -> impl Iterator<Item = usize> {
        0..self.constant_mapper.len()
    }

    pub(crate) fn temporal_names(&self) -> ArcReadLockedVec<ArcStr> {
        self.temporal_mapper.get_keys()
    }

    pub(crate) fn temporal_ids(&self) -> impl Iterator<Item = usize> {
        0..self.temporal_mapper.len()
    }

    pub(crate) fn const_props(&self) -> impl Iterator<Item = (usize, Prop)> + '_ {
        self.constant
            .iter()
            .filter_map(|kv| kv.value().as_ref().map(|v| (*kv.key(), v.clone())))
    }

    pub(crate) fn temporal_props(
        &self,
    ) -> impl Iterator<Item = (usize, impl Deref<Target = TProp> + '_)> + '_ {
        (0..self.temporal_mapper.len()).filter_map(|id| self.temporal.get(&id).map(|v| (id, v)))
    }
}
