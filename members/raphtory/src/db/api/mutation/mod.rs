use crate::prelude::Prop;

mod addition_ops;
mod deletion_ops;
mod import_ops;
mod property_addition_ops;

use crate::errors::{into_graph_err, GraphError};
pub use addition_ops::AdditionOps;
pub use deletion_ops::DeletionOps;
pub use import_ops::ImportOps;
pub use property_addition_ops::PropertyAdditionOps;
use raphtory_api::core::{
    entities::properties::prop::PropType, storage::timeindex::TimeIndexEntry,
};
pub(crate) use raphtory_core::utils::time::{InputTime, TryIntoInputTime};
use raphtory_storage::mutation::addition_ops::InternalAdditionOps;

pub fn time_from_input<G: InternalAdditionOps<Error: Into<GraphError>>, T: TryIntoInputTime>(
    g: &G,
    t: T,
) -> Result<TimeIndexEntry, GraphError> {
    let t = t.try_into_input_time()?;
    Ok(match t {
        InputTime::Simple(t) => TimeIndexEntry::new(t, g.next_event_id().map_err(into_graph_err)?),
        InputTime::Indexed(t, s) => TimeIndexEntry::new(t, s),
    })
}

pub trait CollectProperties {
    fn collect_properties<F: Fn(&str, PropType) -> Result<usize, GraphError>>(
        self,
        id_resolver: F,
    ) -> Result<Vec<(usize, Prop)>, GraphError>;
}

impl<S: AsRef<str>, P: Into<Prop>, PI> CollectProperties for PI
where
    PI: IntoIterator<Item = (S, P)>,
{
    fn collect_properties<F: Fn(&str, PropType) -> Result<usize, GraphError>>(
        self,
        id_resolver: F,
    ) -> Result<Vec<(usize, Prop)>, GraphError>
    where
        PI: IntoIterator<Item = (S, P)>,
    {
        let mut properties: Vec<(usize, Prop)> = Vec::new();
        for (key, value) in self {
            let value: Prop = value.into();
            let prop_id = id_resolver(key.as_ref(), value.dtype())?;
            properties.push((prop_id, value));
        }
        Ok(properties)
    }
}
