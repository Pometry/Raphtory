mod addition_ops;
mod deletion_ops;
mod import_ops;
mod property_addition_ops;

use crate::errors::GraphError;
use raphtory_api::core::{
    storage::timeindex::EventTime,
    utils::time::{InputTime, TryIntoInputTime},
};
use raphtory_storage::mutation::addition_ops::{GraphMutationOps, InternalAdditionOps};

pub use addition_ops::AdditionOps;
use db4_graph::TemporalGraph;
pub use deletion_ops::DeletionOps;
pub use import_ops::ImportOps;
pub use property_addition_ops::PropertyAdditionOps;

pub fn time_from_input<G: InternalAdditionOps, T: TryIntoInputTime>(
    graph: &G,
    time: T,
) -> Result<EventTime, GraphError> {
    let input_time = time.try_into_input_time()?;
    let graph = graph.mutable()?;

    Ok(match input_time {
        InputTime::Simple(t) => EventTime::new(t, graph.next_event_id()),
        InputTime::Indexed(t, secondary_index) => EventTime::new(t, secondary_index),
    })
}

pub fn time_from_input_session<T: TryIntoInputTime>(
    graph: &TemporalGraph,
    time: T,
) -> Result<EventTime, GraphError> {
    let input_time = time.try_into_input_time()?;

    Ok(match input_time {
        InputTime::Simple(t) => EventTime::new(t, graph.next_event_id()),
        InputTime::Indexed(t, secondary_index) => {
            let _ = graph.set_max_event_id(secondary_index);

            EventTime::new(t, secondary_index)
        }
    })
}
