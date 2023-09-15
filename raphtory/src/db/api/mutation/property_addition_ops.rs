use crate::{
    core::{
        storage::timeindex::TimeIndexEntry,
        utils::{errors::GraphError, time::TryIntoTime},
    },
    db::api::mutation::{
        internal::{InternalAdditionOps, InternalPropertyAdditionOps},
        TryIntoInputTime,
    },
};

use super::CollectProperties;

pub trait PropertyAdditionOps {
    fn add_properties<T: TryIntoTime, PI: CollectProperties>(
        &self,
        t: T,
        props: PI,
    ) -> Result<(), GraphError>;

    fn add_constant_properties<PI: CollectProperties>(&self, props: PI) -> Result<(), GraphError>;
}

impl<G: InternalPropertyAdditionOps + InternalAdditionOps> PropertyAdditionOps for G {
    fn add_properties<T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        props: PI,
    ) -> Result<(), GraphError> {
        let ti = TimeIndexEntry::from_input(self, t)?;
        let properties: Vec<_> =
            props.collect_properties(|name, _| Ok(self.resolve_graph_property(name, false)))?;
        self.internal_add_properties(ti, properties)
    }

    fn add_constant_properties<PI: CollectProperties>(&self, props: PI) -> Result<(), GraphError> {
        let properties: Vec<_> =
            props.collect_properties(|name, _| Ok(self.resolve_graph_property(name, true)))?;
        self.internal_add_static_properties(properties)
    }
}
