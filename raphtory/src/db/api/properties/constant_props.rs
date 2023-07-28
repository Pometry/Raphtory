use crate::{core::Prop, db::api::properties::internal::ConstPropertiesOps};
use std::{collections::HashMap, iter::Zip};

pub struct ConstProperties<P: ConstPropertiesOps> {
    pub(crate) props: P,
}

impl<P: ConstPropertiesOps> ConstProperties<P> {
    pub(crate) fn new(props: P) -> Self {
        Self { props }
    }
    pub fn keys(&self) -> Vec<String> {
        self.props
            .const_property_keys()
            .map(|v| v.clone())
            .collect()
    }

    pub fn values(&self) -> Vec<Prop> {
        self.props.const_property_values()
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = (String, Prop)> + '_> {
        Box::new(self.into_iter())
    }

    pub fn get<Q: AsRef<str>>(&self, key: Q) -> Option<Prop> {
        self.props.get_const_property(key.as_ref())
    }

    pub fn contains<Q: AsRef<str>>(&self, key: Q) -> bool {
        self.get(key).is_some()
    }

    pub fn as_map(&self) -> HashMap<String, Prop> {
        self.iter().collect()
    }
}

impl<P: ConstPropertiesOps> IntoIterator for ConstProperties<P> {
    type Item = (String, Prop);
    type IntoIter = Zip<std::vec::IntoIter<String>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys = self.keys();
        let vals = self.values();
        keys.into_iter().zip(vals)
    }
}

impl<P: ConstPropertiesOps> IntoIterator for &ConstProperties<P> {
    type Item = (String, Prop);
    type IntoIter = Zip<std::vec::IntoIter<String>, std::vec::IntoIter<Prop>>;

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
