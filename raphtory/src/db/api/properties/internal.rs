use crate::{
    core::{
        entities::properties::{props::DictMapper, tprop::TProp},
        storage::locked_view::LockedView,
        ArcStr, Prop,
    },
    db::api::view::{internal::Base, BoxedIter},
};
use enum_dispatch::enum_dispatch;
use std::sync::Arc;

pub type Key = String; //Fixme: This should really be the internal usize index but that means more reworking of the low-level api

#[enum_dispatch]
pub trait TemporalPropertyViewOps {
    fn temporal_value(&self, id: &Key) -> Option<Prop> {
        self.temporal_values(id).last().cloned()
    }
    fn temporal_history(&self, id: &Key) -> Vec<i64>;
    fn temporal_values(&self, id: &Key) -> Vec<Prop>;
    fn temporal_value_at(&self, id: &Key, t: i64) -> Option<Prop> {
        let history = self.temporal_history(id);
        match history.binary_search(&t) {
            Ok(index) => Some(self.temporal_values(id)[index].clone()),
            Err(index) => (index > 0).then(|| self.temporal_values(id)[index - 1].clone()),
        }
    }
}

#[enum_dispatch]
pub trait ConstPropertiesOps {
    fn const_property_keys(&self) -> Box<dyn Iterator<Item = ArcStr>>;
    fn const_property_values(&self) -> Vec<Prop> {
        self.const_property_keys()
            .map(|k| self.get_const_property(&k).expect("should exist"))
            .collect()
    }
    fn get_const_property(&self, key: &str) -> Option<Prop>;
}

#[enum_dispatch]
pub trait TemporalPropertiesOps {
    fn temporal_property_keys(&self) -> Box<dyn Iterator<Item = ArcStr> + '_>;
    fn temporal_property_values(&self) -> Box<dyn Iterator<Item = Key> + '_> {
        Box::new(
            self.temporal_property_keys()
                .flat_map(|k| self.get_temporal_property(&k)),
        )
    }
    fn get_temporal_property(&self, key: &str) -> Option<Key>;
}

pub trait PropertiesOps:
    TemporalPropertiesOps + TemporalPropertyViewOps + ConstPropertiesOps
{
}

impl<P: TemporalPropertiesOps + TemporalPropertyViewOps + ConstPropertiesOps> PropertiesOps for P {}

pub trait InheritTemporalPropertyViewOps: Base {}
pub trait InheritTemporalPropertiesOps: Base {}
pub trait InheritStaticPropertiesOps: Base {}
pub trait InheritPropertiesOps: Base {}

impl<P: InheritPropertiesOps> InheritStaticPropertiesOps for P {}
impl<P: InheritPropertiesOps> InheritTemporalPropertiesOps for P {}

impl<P: InheritTemporalPropertyViewOps> TemporalPropertyViewOps for P
where
    P::Base: TemporalPropertyViewOps,
{
    fn temporal_value(&self, id: &Key) -> Option<Prop> {
        self.base().temporal_value(id)
    }

    fn temporal_history(&self, id: &Key) -> Vec<i64> {
        self.base().temporal_history(id)
    }

    fn temporal_values(&self, id: &Key) -> Vec<Prop> {
        self.base().temporal_values(id)
    }

    fn temporal_value_at(&self, id: &Key, t: i64) -> Option<Prop> {
        self.base().temporal_value_at(id, t)
    }
}

impl<P: InheritTemporalPropertiesOps> InheritTemporalPropertyViewOps for P {}

impl<P: InheritTemporalPropertiesOps> TemporalPropertiesOps for P
where
    P::Base: TemporalPropertiesOps,
{
    fn temporal_property_keys(&self) -> Box<dyn Iterator<Item = ArcStr> + '_> {
        self.base().temporal_property_keys()
    }

    fn temporal_property_values(&self) -> Box<dyn Iterator<Item = Key> + '_> {
        self.base().temporal_property_values()
    }

    fn get_temporal_property(&self, key: &str) -> Option<Key> {
        self.base().get_temporal_property(key)
    }
}

impl<P: InheritStaticPropertiesOps> ConstPropertiesOps for P
where
    P::Base: ConstPropertiesOps,
{
    fn const_property_keys(&self) -> Box<dyn Iterator<Item = ArcStr>> {
        self.base().const_property_keys()
    }

    fn const_property_values(&self) -> Vec<Prop> {
        self.base().const_property_values()
    }

    fn get_const_property(&self, key: &str) -> Option<Prop> {
        self.base().get_const_property(key)
    }
}
