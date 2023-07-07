use crate::core::entities::properties::props::DictMapper;
use crate::core::entities::properties::tprop::TProp;
use crate::core::Prop;
use std::marker::PhantomData;

pub trait CorePropertiesOps {
    fn static_prop_meta(&self) -> &DictMapper<String>;
    fn temporal_prop_meta(&self) -> &DictMapper<String>;
    fn temporal_prop(&self, id: usize) -> Option<&TProp>;
    fn static_prop(&self, id: usize) -> Option<&Prop>;
}

pub trait TemporalPropertyViewOps<Key = String> {
    fn temporal_value(&self, id: &Key) -> Option<Prop>;
    fn temporal_history(&self, id: &Key) -> Vec<i64>;
    fn temporal_values(&self, id: &Key) -> Vec<Prop>;
    fn temporal_value_at(&self, id: &Key, t: i64) -> Option<Prop>;
}

pub trait StaticPropertiesOps {
    fn static_property_keys(&self) -> Vec<String>;
    fn static_property_values(&self) -> Vec<Prop> {
        self.static_property_keys()
            .into_iter()
            .map(|k| self.get_static_property(&k).expect("should exist"))
            .collect()
    }
    fn get_static_property(&self, key: &str) -> Option<Prop>;
}

pub trait TemporalPropertiesOps<Key = String>: TemporalPropertyViewOps<Key> {
    fn temporal_property_keys(&self) -> Vec<String>;
    fn temporal_property_values(&self) -> Box<dyn Iterator<Item = Key> + '_>;
    fn get_temporal_property(&self, key: &str) -> Option<Key>;
}

pub struct TemporalPropertyView<P: TemporalPropertyViewOps<K>, K = String> {
    pub(crate) id: K,
    pub(crate) props: P,
}

impl<P: TemporalPropertyViewOps<K>, K> TemporalPropertyView<P, K> {
    pub(crate) fn new(props: P, key: K) -> Self {
        TemporalPropertyView { props, id: key }
    }
    pub fn history(&self) -> Vec<i64> {
        self.props.temporal_history(&self.id)
    }
    pub fn values(&self) -> Vec<Prop> {
        self.props.temporal_values(&self.id)
    }
    pub fn pairs(&self) -> impl Iterator<Item = (i64, Prop)> {
        self.history().into_iter().zip(self.values())
    }
    pub fn at(&self, t: i64) -> Option<Prop> {
        self.props.temporal_value_at(&self.id, t)
    }
    pub fn value(&self) -> Option<Prop> {
        self.props.temporal_value(&self.id)
    }
}

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

    pub fn pairs(&self) -> Box<dyn Iterator<Item = (String, Prop)> + '_> {
        Box::new(
            self.props
                .static_property_keys()
                .into_iter()
                .zip(self.props.static_property_values()),
        )
    }

    pub fn get<Q: AsRef<str>>(&self, key: Q) -> Option<Prop> {
        self.props.get_static_property(key.as_ref())
    }
}

pub struct TemporalProperties<P: TemporalPropertiesOps<K> + Clone, K = String> {
    pub(crate) props: P,
    key_marker: PhantomData<K>,
}

impl<P: TemporalPropertiesOps<K> + Clone, K> TemporalProperties<P, K> {
    pub(crate) fn new(props: P) -> Self {
        Self {
            props,
            key_marker: PhantomData,
        }
    }
    pub fn keys(&self) -> Vec<String> {
        self.props.temporal_property_keys()
    }

    pub fn values(&self) -> Vec<TemporalPropertyView<P, K>> {
        self.props
            .temporal_property_values()
            .map(|k| TemporalPropertyView::new(self.props.clone(), k))
            .collect()
    }

    pub fn pairs(&self) -> Box<dyn Iterator<Item = (String, TemporalPropertyView<P, K>)> + '_> {
        Box::new(
            self.props
                .temporal_property_keys()
                .into_iter()
                .zip(self.values()),
        )
    }

    pub fn get<Q: AsRef<str>>(&self, key: Q) -> Option<TemporalPropertyView<P, K>> {
        self.props
            .get_temporal_property(key.as_ref())
            .map(|k| TemporalPropertyView::new(self.props.clone(), k))
    }
}
