use std::iter::Zip;
use crate::core::Prop;
use crate::db::api::properties::internal::StaticPropertiesOps;

pub struct StaticProperties<P: StaticPropertiesOps> {
    pub(crate) props: P,
}

impl<P: StaticPropertiesOps> StaticProperties<P> {
    pub(crate) fn new(props: P) -> Self {
        Self { props }
    }
    pub fn keys(&self) -> Vec<String> {
        self.props.static_property_keys()
    }

    pub fn values(&self) -> Vec<Prop> {
        self.props.static_property_values()
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = (String, Prop)> + '_> {
        Box::new(self.into_iter())
    }

    pub fn get<Q: AsRef<str>>(&self, key: Q) -> Option<Prop> {
        self.props.get_static_property(key.as_ref())
    }

    pub fn contains<Q: AsRef<str>>(&self, key: Q) -> bool {
        self.get(key).is_some()
    }
}

impl<P: StaticPropertiesOps> IntoIterator for StaticProperties<P> {
    type Item = (String, Prop);
    type IntoIter = Zip<std::vec::IntoIter<String>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys = self.keys();
        let vals = self.values();
        keys.into_iter().zip(vals)
    }
}

impl<P: StaticPropertiesOps> IntoIterator for &StaticProperties<P> {
    type Item = (String, Prop);
    type IntoIter = Zip<std::vec::IntoIter<String>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys = self.keys();
        let vals = self.values();
        keys.into_iter().zip(vals)
    }
}
