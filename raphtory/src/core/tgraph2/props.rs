use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

use crate::core::{lazy_vec::LazyVec, tprop::TProp, Prop};

use super::tgraph::FxDashMap;

#[derive(Serialize, Deserialize)]
pub(crate) struct Props {
    // properties
    static_props: LazyVec<Option<Prop>>,
    temporal_props: LazyVec<TProp>,
}

impl Props {
    pub fn new() -> Self {
        Self {
            static_props: LazyVec::Empty,
            temporal_props: LazyVec::Empty,
        }
    }

    pub(crate) fn add_prop(&mut self, t: i64, prop_id: usize, prop: Prop) {
        self.temporal_props
            .update_or_set(prop_id, |p| p.set(t, &prop), TProp::from(t, &prop))
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PropsMeta {
    meta: FxDashMap<String, usize>,
    len: AtomicUsize,
}

impl PropsMeta {
    pub(crate) fn new() -> Self {
        Self {
            meta: FxDashMap::default(),
            len: AtomicUsize::new(0),
        }
    }

    pub(crate) fn resolve_prop_ids(
        &self,
        props: Vec<(String, Prop)>,
    ) -> impl Iterator<Item = (usize, Prop)> + '_ {
        props.into_iter().map(|(name, prop)| {
            let prop_id = self.meta.get(&name).map(|x| *x).unwrap_or_else(|| {
                let id = self.increment_len();
                self.meta.insert(name, id);
                id
            });
            (prop_id, prop)
        })
    }

    fn increment_len(&self) -> usize {
        self.len.fetch_add(1, Ordering::SeqCst)
    }
}
