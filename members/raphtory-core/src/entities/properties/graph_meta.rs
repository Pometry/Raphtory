use crate::{
    entities::properties::{
        props::ConstPropError,
        tprop::{IllegalPropType, TProp},
    },
    storage::{locked_view::LockedView, timeindex::TimeIndexEntry},
};
use raphtory_api::core::{
    entities::properties::{
        meta::PropMapper,
        prop::{Prop, PropError, PropType},
    },
    storage::{arc_str::ArcStr, dict_mapper::MaybeNew, locked_vec::ArcReadLockedVec, FxDashMap},
};
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct GraphMeta {
    constant_mapper: PropMapper,
    temporal_mapper: PropMapper,
    constant: FxDashMap<usize, Option<Prop>>,
    temporal: FxDashMap<usize, TProp>,
}

impl GraphMeta {
    pub fn new() -> Self {
        Self {
            constant_mapper: PropMapper::default(),
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
    pub fn const_prop_meta(&self) -> &PropMapper {
        &self.constant_mapper
    }

    #[inline]
    pub fn temporal_prop_meta(&self) -> &PropMapper {
        &self.temporal_mapper
    }

    #[inline]
    pub fn resolve_property(
        &self,
        name: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, PropError> {
        let mapper = if is_static {
            &self.constant_mapper
        } else {
            &self.temporal_mapper
        };
        mapper.get_or_create_and_validate(name, dtype)
    }

    pub fn add_constant_prop(&self, prop_id: usize, prop: Prop) -> Result<(), ConstPropError> {
        let mut prop_entry = self.constant.entry(prop_id).or_insert(None);
        match prop_entry.deref_mut() {
            Some(old_value) => {
                if !(old_value == &prop) {
                    return Err(ConstPropError::IllegalUpdate {
                        old: old_value.clone(),
                        new: prop,
                    });
                }
            }
            None => {
                *prop_entry = Some(prop);
            }
        }
        Ok(())
    }

    pub fn update_constant_prop(&self, prop_id: usize, prop: Prop) {
        let mut prop_entry = self.constant.entry(prop_id).or_insert(None);
        *prop_entry = Some(prop);
    }

    pub fn add_prop(
        &self,
        t: TimeIndexEntry,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), IllegalPropType> {
        let mut prop_entry = self.temporal.entry(prop_id).or_default();
        (*prop_entry).set(t, prop)
    }

    pub fn get_constant(&self, id: usize) -> Option<Prop> {
        let entry = self.constant.get(&id)?;
        entry.as_ref().cloned()
    }

    pub fn get_temporal_prop(&self, prop_id: usize) -> Option<LockedView<'_, TProp>> {
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

    pub fn get_const_dtype(&self, prop_id: usize) -> Option<PropType> {
        self.constant_mapper.get_dtype(prop_id)
    }

    pub fn constant_names(&self) -> ArcReadLockedVec<ArcStr> {
        self.constant_mapper.get_keys()
    }

    pub fn const_prop_ids(&self) -> impl Iterator<Item = usize> {
        0..self.constant_mapper.len()
    }

    pub fn temporal_names(&self) -> ArcReadLockedVec<ArcStr> {
        self.temporal_mapper.get_keys()
    }

    pub fn temporal_ids(&self) -> impl Iterator<Item = usize> {
        0..self.temporal_mapper.len()
    }

    pub fn const_props(&self) -> impl Iterator<Item = (usize, Prop)> + '_ {
        self.constant
            .iter()
            .filter_map(|kv| kv.value().as_ref().map(|v| (*kv.key(), v.clone())))
    }

    pub fn temporal_props(
        &self,
    ) -> impl Iterator<Item = (usize, impl Deref<Target = TProp> + '_)> + '_ {
        (0..self.temporal_mapper.len()).filter_map(|id| self.temporal.get(&id).map(|v| (id, v)))
    }
}
