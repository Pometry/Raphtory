use crate::python::types::repr::Repr;
use raphtory_api::core::{
    entities::properties::prop::{Prop, PropUntagged},
    storage::timeindex::EventTime,
};

pub type PropValue = Option<Prop>;
pub type PropHistItems = Vec<(EventTime, Prop)>;
