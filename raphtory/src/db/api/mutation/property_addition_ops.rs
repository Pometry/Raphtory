use crate::{
    core::utils::errors::GraphError,
    db::api::mutation::{
        internal::{InternalAdditionOps, InternalPropertyAdditionOps},
        TryIntoInputTime,
    },
};

use super::{time_from_input, CollectProperties};

pub trait PropertyAdditionOps {
    fn add_properties<T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        props: PI,
    ) -> Result<(), GraphError>;

    fn add_constant_properties<PI: CollectProperties>(&self, props: PI) -> Result<(), GraphError>;
    fn update_constant_properties<PI: CollectProperties>(
        &self,
        props: PI,
    ) -> Result<(), GraphError>;
}

impl<G: InternalPropertyAdditionOps + InternalAdditionOps> PropertyAdditionOps for G {
    fn add_properties<T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        props: PI,
    ) -> Result<(), GraphError> {
        let ti = time_from_input(self, t)?;
        let properties: Vec<_> = props.collect_properties(|name, dtype| {
            Ok(self.resolve_graph_property(name, dtype, false)?.inner())
        })?;
        self.internal_add_properties(ti, &properties)
    }

    fn add_constant_properties<PI: CollectProperties>(&self, props: PI) -> Result<(), GraphError> {
        let properties: Vec<_> = props.collect_properties(|name, dtype| {
            Ok(self.resolve_graph_property(name, dtype, true)?.inner())
        })?;
        self.internal_add_constant_properties(&properties)
    }

    fn update_constant_properties<PI: CollectProperties>(
        &self,
        props: PI,
    ) -> Result<(), GraphError> {
        let properties: Vec<_> = props.collect_properties(|name, dtype| {
            Ok(self.resolve_graph_property(name, dtype, true)?.inner())
        })?;
        self.internal_update_constant_properties(&properties)
    }
}
