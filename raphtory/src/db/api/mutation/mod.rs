use crate::prelude::Prop;

mod addition_ops;
mod deletion_ops;
mod import_ops;
#[cfg(feature = "search")]
pub mod index_ops;
mod property_addition_ops;

pub use addition_ops::AdditionOps;
pub use deletion_ops::DeletionOps;
pub use import_ops::ImportOps;
#[cfg(feature = "search")]
pub use index_ops::IndexMutationOps;
pub use property_addition_ops::PropertyAdditionOps;
use raphtory_api::core::{
    entities::properties::prop::PropType, storage::timeindex::EventTime,
};
pub(crate) use raphtory_core::utils::time::{InputTime, TryIntoInputTime};
use raphtory_storage::mutation::addition_ops::{InternalAdditionOps, SessionAdditionOps};

pub fn time_from_input<G: InternalAdditionOps<Error: Into<GraphError>>, T: TryIntoInputTime>(
    graph: &G,
    time: T,
) -> Result<EventTime, GraphError> {
    let input_time = time.try_into_input_time()?;
    let session = graph.write_session().map_err(|err| err.into())?;

    Ok(match input_time {
        InputTime::Simple(t) => {
            EventTime::new(t, session.next_event_id().map_err(into_graph_err)?)
        }
        InputTime::Indexed(t, secondary_index) => EventTime::new(t, secondary_index),
    })
}

pub fn time_from_input_session<
    G: SessionAdditionOps<Error: Into<GraphError>>,
    T: TryIntoInputTime,
>(
    graph: &G,
    time: T,
) -> Result<EventTime, GraphError> {
    let input_time = time.try_into_input_time()?;

    Ok(match input_time {
        InputTime::Simple(t) => {
            EventTime::new(t, graph.next_event_id().map_err(into_graph_err)?)
        }
        InputTime::Indexed(t, secondary_index) => {
            let _ = graph
                .set_max_event_id(secondary_index)
                .map_err(into_graph_err)?;

            EventTime::new(t, secondary_index)
        }
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
