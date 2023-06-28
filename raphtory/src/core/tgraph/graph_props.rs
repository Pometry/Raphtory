use serde::{Deserialize, Serialize};

use crate::core::{locked_view::LockedView, tprop::TProp, Prop};

use super::{props::DictMapper, tgraph::FxDashMap};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct GraphProps {
    static_mapper: DictMapper<String>,
    temporal_mapper: DictMapper<String>,
    static_props: FxDashMap<usize, Option<Prop>>,
    temporal_props: FxDashMap<usize, TProp>,
}

impl GraphProps {
    pub(crate) fn new() -> Self {
        Self {
            static_mapper: DictMapper::default(),
            temporal_mapper: DictMapper::default(),
            static_props: FxDashMap::default(),
            temporal_props: FxDashMap::default(),
        }
    }

    pub(crate) fn add_static_prop(&self, name: &str, prop: Prop) {
        let prop_id = self.static_mapper.get_or_create_id(name.to_owned());
        let mut prop_entry = self.static_props.entry(prop_id).or_insert(None);
        (*prop_entry) = Some(prop);
    }

    pub(crate) fn add_prop(&self, t: i64, name: &str, prop: Prop) {
        let prop_id = self.temporal_mapper.get_or_create_id(name.to_owned());
        let mut prop_entry = self
            .temporal_props
            .entry(prop_id)
            .or_insert(TProp::default());
        (*prop_entry).set(t, &prop);
    }

    pub(crate) fn get_static(&self, name: &str) -> Option<Prop> {
        let prop_id = self.static_mapper.get(&(name.to_owned()))?;
        let entry = self.static_props.get(&prop_id)?;
        entry.as_ref().cloned()
    }

    pub(crate) fn get_temporal(&self, name: &str) -> Option<LockedView<'_, TProp>> {
        let prop_id = self.temporal_mapper.get(&(name.to_owned()))?;
        let entry = self.temporal_props.get(&prop_id)?;
        Some(LockedView::DashMap(entry))
    }

    pub(crate) fn static_prop_names(&self) -> Vec<String> {
        self.static_mapper.get_keys()
    }

    pub(crate) fn temporal_prop_names(&self) -> Vec<String> {
        self.temporal_mapper.get_keys()
    }
}
