use crate::{
    core::{ArcStr, Prop},
    db::api::properties::internal::ConstPropertiesOps,
};
use std::{collections::HashMap, iter::Zip};

pub struct ConstProperties<P: ConstPropertiesOps> {
    pub(crate) props: P,
}

impl<P: ConstPropertiesOps> ConstProperties<P> {
    pub(crate) fn new(props: P) -> Self {
        Self { props }
    }
    pub fn keys(&self) -> Vec<ArcStr> {
        self.props.const_prop_keys().map(|v| v.clone()).collect()
    }

    pub fn values(&self) -> Vec<Prop> {
        self.props.const_prop_values()
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = (ArcStr, Prop)> + '_> {
        Box::new(self.into_iter())
    }

    pub fn get(&self, key: &str) -> Option<Prop> {
        let id = self.props.get_const_prop_id(key)?;
        self.props.get_const_prop(id)
    }

    pub fn contains(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn as_map(&self) -> HashMap<ArcStr, Prop> {
        self.iter().collect()
    }
}

impl<P: ConstPropertiesOps> IntoIterator for ConstProperties<P> {
    type Item = (ArcStr, Prop);
    type IntoIter = Zip<std::vec::IntoIter<ArcStr>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys = self.keys();
        let vals = self.values();
        keys.into_iter().zip(vals)
    }
}

impl<P: ConstPropertiesOps> IntoIterator for &ConstProperties<P> {
    type Item = (ArcStr, Prop);
    type IntoIter = Zip<std::vec::IntoIter<ArcStr>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys = self.keys();
        let vals = self.values();
        keys.into_iter().zip(vals)
    }
}

impl<P: ConstPropertiesOps> PartialEq for ConstProperties<P> {
    fn eq(&self, other: &Self) -> bool {
        self.as_map() == other.as_map()
    }
}
