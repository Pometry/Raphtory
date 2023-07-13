use crate::core::entities::properties::props::DictMapper;
use crate::core::entities::properties::tprop::TProp;
use crate::core::storage::locked_view::LockedView;
use crate::core::Prop;
use crate::db::api::view::internal::Base;

pub trait CorePropertiesOps {
    fn static_prop_meta(&self) -> &DictMapper<String>;
    fn temporal_prop_meta(&self) -> &DictMapper<String>;
    fn temporal_prop(&self, id: usize) -> Option<&TProp>;
    fn static_prop(&self, id: usize) -> Option<&Prop>;
}

pub type Key = String; //Fixme: This should really be the internal usize index but that means more reworking of the low-level api

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

pub trait StaticPropertiesOps {
    fn static_property_keys<'a>(&'a self) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a>;
    fn static_property_values(&self) -> Vec<Prop> {
        self.static_property_keys()
            .into_iter()
            .map(|k| self.get_static_property(&k).expect("should exist"))
            .collect()
    }
    fn get_static_property(&self, key: &str) -> Option<Prop>;
}

pub trait TemporalPropertiesOps {
    fn temporal_property_keys<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a>;
    fn temporal_property_values(&self) -> Box<dyn Iterator<Item = Key> + '_> {
        Box::new(
            self.temporal_property_keys()
                .flat_map(|k| self.get_temporal_property(&k)),
        )
    }
    fn get_temporal_property(&self, key: &str) -> Option<Key>;
}

pub trait PropertiesOps:
    TemporalPropertiesOps + TemporalPropertyViewOps + StaticPropertiesOps
{
}

impl<P: TemporalPropertiesOps + TemporalPropertyViewOps + StaticPropertiesOps> PropertiesOps for P {}

pub trait InheritTempralPropertyViewOps: Base {}
pub trait InheritTemporalPropertiesOps: Base {}
pub trait InheritStaticPropertiesOps: Base {}
pub trait InheritPropertiesOps: Base {}

impl<P: InheritPropertiesOps> InheritStaticPropertiesOps for P {}
impl<P: InheritPropertiesOps> InheritTemporalPropertiesOps for P {}

impl<P: InheritTempralPropertyViewOps> TemporalPropertyViewOps for P
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

impl<P: InheritTemporalPropertiesOps> InheritTempralPropertyViewOps for P {}

impl<P: InheritTemporalPropertiesOps> TemporalPropertiesOps for P
where
    P::Base: TemporalPropertiesOps,
{
    fn temporal_property_keys<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
        self.base().temporal_property_keys()
    }

    fn temporal_property_values(&self) -> Box<dyn Iterator<Item = Key> + '_> {
        self.base().temporal_property_values()
    }

    fn get_temporal_property(&self, key: &str) -> Option<Key> {
        self.base().get_temporal_property(key)
    }
}

impl<P: InheritStaticPropertiesOps> StaticPropertiesOps for P
where
    P::Base: StaticPropertiesOps,
{
    fn static_property_keys<'a>(&'a self) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
        self.base().static_property_keys()
    }

    fn static_property_values(&self) -> Vec<Prop> {
        self.base().static_property_values()
    }

    fn get_static_property(&self, key: &str) -> Option<Prop> {
        self.base().get_static_property(key)
    }
}
