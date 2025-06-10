use super::CollectProperties;
use crate::{
    db::api::mutation::{time_from_input_session, TryIntoInputTime},
    errors::{into_graph_err, GraphError},
};
use raphtory_storage::mutation::{
    addition_ops::{InternalAdditionOps, SessionAdditionOps},
    property_addition_ops::InternalPropertyAdditionOps,
};

pub trait PropertyAdditionOps:
    InternalPropertyAdditionOps<Error: Into<GraphError>> + InternalAdditionOps<Error: Into<GraphError>>
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
        G: InternalPropertyAdditionOps<Error: Into<GraphError>>
            + InternalAdditionOps<Error: Into<GraphError>>,
    > PropertyAdditionOps for G
{
    fn add_properties<T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        props: PI,
    ) -> Result<(), GraphError> {
        let session = self.write_session().map_err(|err| err.into())?;
        let ti = time_from_input_session(&session, t)?;
        let properties: Vec<_> = props.collect_properties(|name, dtype| {
            Ok(session
                .resolve_graph_property(name, dtype, false)
                .map_err(into_graph_err)?
                .inner())
        })?;
        self.internal_add_properties(ti, &properties)
            .map_err(into_graph_err)?;
        Ok(())
    }

    fn add_constant_properties<PI: CollectProperties>(&self, props: PI) -> Result<(), GraphError> {
        let session = self.write_session().map_err(|err| err.into())?;
        let properties: Vec<_> = props.collect_properties(|name, dtype| {
            Ok(session
                .resolve_graph_property(name, dtype, true)
                .map_err(into_graph_err)?
                .inner())
        })?;
        self.internal_add_constant_properties(&properties)
            .map_err(into_graph_err)?;
        Ok(())
    }

    fn update_constant_properties<PI: CollectProperties>(
        &self,
        props: PI,
    ) -> Result<(), GraphError> {
        let session = self.write_session().map_err(|err| err.into())?;
        let properties: Vec<_> = props.collect_properties(|name, dtype| {
            Ok(session
                .resolve_graph_property(name, dtype, true)
                .map_err(into_graph_err)?
                .inner())
        })?;
        self.internal_update_constant_properties(&properties)
            .map_err(into_graph_err)?;
        Ok(())
    }
}
