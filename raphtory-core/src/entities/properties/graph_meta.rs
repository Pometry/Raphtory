use crate::{
    entities::properties::{
        props::MetadataError,
        tprop::{IllegalPropType, TProp},
    },
    storage::{locked_view::LockedView, timeindex::EventTime},
};
use raphtory_api::core::{
    entities::properties::{
        meta::PropMapper,
        prop::{Prop, PropError, PropType},
    },
    storage::{
        arc_str::ArcStr,
        dict_mapper::{MaybeNew, PublicKeys},
        FxDashMap,
    },
};
use serde::Serialize;
use std::ops::{Deref, DerefMut};

#[derive(Serialize, Debug, Default)]
pub struct GraphMeta {
    metadata_mapper: PropMapper,
    temporal_mapper: PropMapper,
    metadata: FxDashMap<usize, Option<Prop>>,
    temporal: FxDashMap<usize, TProp>,
}

impl GraphMeta {
    pub fn new() -> Self {
        Self {
            metadata_mapper: PropMapper::default(),
            temporal_mapper: PropMapper::default(),
            metadata: FxDashMap::default(),
            temporal: FxDashMap::default(),
        }
    }

    pub fn deep_clone(&self) -> Self {
        Self {
            metadata_mapper: self.metadata_mapper.deep_clone(),
            temporal_mapper: self.temporal_mapper.deep_clone(),
            metadata: self.metadata.clone(),
            temporal: self.temporal.clone(),
        }
    }

    #[inline]
    pub fn metadata_mapper(&self) -> &PropMapper {
        &self.metadata_mapper
    }

    #[inline]
    pub fn temporal_mapper(&self) -> &PropMapper {
        &self.temporal_mapper
    }

    #[inline]
    pub fn resolve_property(
        &self,
        name: &str,
        dtype: PropType,
        is_metadata: bool,
    ) -> Result<MaybeNew<usize>, PropError> {
        let mapper = if is_metadata {
            &self.metadata_mapper
        } else {
            &self.temporal_mapper
        };
        mapper.get_or_create_and_validate(name, dtype)
    }

    pub fn add_metadata(&self, prop_id: usize, prop: Prop) -> Result<(), MetadataError> {
        let mut prop_entry = self.metadata.entry(prop_id).or_insert(None);
        match prop_entry.deref_mut() {
            Some(old_value) => {
                if !(old_value == &prop) {
                    return Err(MetadataError::IllegalUpdate {
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

    pub fn update_metadata(&self, prop_id: usize, prop: Prop) {
        let mut prop_entry = self.metadata.entry(prop_id).or_insert(None);
        *prop_entry = Some(prop);
    }

    pub fn add_prop(
        &self,
        t: EventTime,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), IllegalPropType> {
        let mut prop_entry = self.temporal.entry(prop_id).or_default();
        (*prop_entry).set(t, prop)
    }

    pub fn get_metadata(&self, id: usize) -> Option<Prop> {
        let entry = self.metadata.get(&id)?;
        entry.as_ref().cloned()
    }

    pub fn get_temporal_prop(&self, prop_id: usize) -> Option<LockedView<'_, TProp>> {
        let entry = self.temporal.get(&prop_id)?;
        Some(LockedView::DashMap(entry))
    }

    pub fn get_metadata_id(&self, name: &str) -> Option<usize> {
        self.metadata_mapper.get_id(name)
    }

    pub fn get_temporal_id(&self, name: &str) -> Option<usize> {
        self.temporal_mapper.get_id(name)
    }

    pub fn get_metadata_name(&self, prop_id: usize) -> ArcStr {
        self.metadata_mapper.get_name(prop_id).clone()
    }

    pub fn get_temporal_name(&self, prop_id: usize) -> ArcStr {
        self.temporal_mapper.get_name(prop_id).clone()
    }

    pub fn get_temporal_dtype(&self, prop_id: usize) -> Option<PropType> {
        self.temporal_mapper.get_dtype(prop_id)
    }

    pub fn get_metadata_dtype(&self, prop_id: usize) -> Option<PropType> {
        self.metadata_mapper.get_dtype(prop_id)
    }

    pub fn metadata_names(&self) -> PublicKeys<ArcStr> {
        self.metadata_mapper.keys()
    }

    pub fn metadata_ids(&self) -> impl Iterator<Item = usize> {
        self.metadata_mapper.ids()
    }

    pub fn temporal_names(&self) -> PublicKeys<ArcStr> {
        self.temporal_mapper.keys()
    }

    pub fn temporal_ids(&self) -> impl Iterator<Item = usize> {
        self.temporal_mapper.ids()
    }

    pub fn metadata(&self) -> impl Iterator<Item = (usize, Prop)> + '_ {
        self.metadata
            .iter()
            .filter_map(|kv| kv.value().as_ref().map(|v| (*kv.key(), v.clone())))
    }

    pub fn temporal_props(
        &self,
    ) -> impl Iterator<Item = (usize, impl Deref<Target = TProp> + '_)> + '_ {
        self.temporal_mapper
            .ids()
            .filter_map(|id| self.temporal.get(&id).map(|v| (id, v)))
    }
}
