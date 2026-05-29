use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};

pub type PropValue = Option<Prop>;
pub type PropHistItems = Vec<(EventTime, Prop)>;
