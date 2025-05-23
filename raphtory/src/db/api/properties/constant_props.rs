use crate::{
    core::utils::iter::GenLockedIter,
    db::api::{properties::internal::ConstantPropertiesOps, view::BoxedLIter},
};
use raphtory_api::core::{entities::properties::prop::Prop, storage::arc_str::ArcStr};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
};

pub struct ConstantProperties<'a, P: ConstantPropertiesOps> {
    pub(crate) props: P,
    _marker: std::marker::PhantomData<&'a P>,
}

impl<'a, P: ConstantPropertiesOps + Sync> Debug for ConstantProperties<'a, P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<'a, P: ConstantPropertiesOps + Sync> ConstantProperties<'a, P> {
    pub(crate) fn new(props: P) -> Self {
        Self {
            props,
            _marker: std::marker::PhantomData,
        }
    }
    pub fn keys(&self) -> BoxedLIter<ArcStr> {
        self.props.const_prop_keys()
    }

    pub fn values(&self) -> BoxedLIter<Option<Prop>> {
        self.props.const_prop_values()
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = (ArcStr, Prop)> + '_> {
        Box::new(self.into_iter())
    }

    pub fn iter_id(&self) -> impl Iterator<Item = (usize, Prop)> + '_ {
        self.props
            .const_prop_ids()
            .zip(self.props.const_prop_values())
            .filter_map(|(id, v)| Some((id, v?)))
    }

    pub fn get(&self, key: &str) -> Option<Prop> {
        let id = self.props.get_const_prop_id(key)?;
        self.props.get_const_prop(id)
    }

    pub fn get_by_id(&self, id: usize) -> Option<Prop> {
        self.props.get_const_prop(id)
    }

    pub fn contains(&self, key: &str) -> bool {
        self.props.get_const_prop_id(key).is_some()
    }

    pub fn as_map(&self) -> HashMap<ArcStr, Prop> {
        self.iter().collect()
    }
}

impl<'a, P: ConstantPropertiesOps + Sync + 'a> IntoIterator for ConstantProperties<'a, P> {
    type Item = (ArcStr, Prop);
    type IntoIter = BoxedLIter<'a, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(GenLockedIter::from(self, |const_prop| {
            let keys = const_prop.keys();
            let vals = const_prop.values();
            Box::new(
                keys.into_iter()
                    .zip(vals)
                    .filter_map(|(k, v)| Some((k, v?))),
            )
        }))
    }
}

impl<'a, P: ConstantPropertiesOps + Sync> IntoIterator for &'a ConstantProperties<'a, P> {
    type Item = (ArcStr, Prop);
    type IntoIter = Box<dyn Iterator<Item = (ArcStr, Prop)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        let keys = self.keys();
        let vals = self.values();
        Box::new(
            keys.into_iter()
                .zip(vals)
                .filter_map(|(k, v)| Some((k, v?))),
        )
    }
}

impl<'a, P: ConstantPropertiesOps + Sync> PartialEq for ConstantProperties<'a, P> {
    fn eq(&self, other: &Self) -> bool {
        self.as_map() == other.as_map()
    }
}
