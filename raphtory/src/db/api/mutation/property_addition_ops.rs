use super::{time_from_input, CollectProperties};
use crate::{db::api::mutation::TryIntoInputTime, errors::GraphError};
use raphtory_storage::mutation::{
    addition_ops::InternalAdditionOps, property_addition_ops::InternalPropertyAdditionOps,
};

pub trait PropertyAdditionOps:
    InternalPropertyAdditionOps<Error = GraphError> + InternalAdditionOps<Error = GraphError>
{
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

impl<
        G: InternalPropertyAdditionOps<Error = GraphError> + InternalAdditionOps<Error = GraphError>,
    > PropertyAdditionOps for G
{
    fn add_properties<T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        props: PI,
    ) -> Result<(), GraphError> {
        let ti = time_from_input(self, t)?;
        let properties: Vec<_> = props.collect_properties(|name, dtype| {
            Ok(self.resolve_graph_property(name, dtype, false)?.inner())
        })?;
        self.internal_add_properties(ti, &properties)?;
        Ok(())
    }

    fn add_constant_properties<PI: CollectProperties>(&self, props: PI) -> Result<(), GraphError> {
        let properties: Vec<_> = props.collect_properties(|name, dtype| {
            Ok(self.resolve_graph_property(name, dtype, true)?.inner())
        })?;
        self.internal_add_constant_properties(&properties)?;
        Ok(())
    }

    fn update_constant_properties<PI: CollectProperties>(
        &self,
        props: PI,
    ) -> Result<(), GraphError> {
        let properties: Vec<_> = props.collect_properties(|name, dtype| {
            Ok(self.resolve_graph_property(name, dtype, true)?.inner())
        })?;
        self.internal_update_constant_properties(&properties)?;
        Ok(())
    }
}
