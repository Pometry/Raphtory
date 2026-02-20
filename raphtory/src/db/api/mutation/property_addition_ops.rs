use super::CollectProperties;
use crate::{
    db::api::mutation::{time_from_input_session, TryIntoInputTime},
    errors::{into_graph_err, GraphError},
};
use raphtory_api::core::entities::properties::prop::Prop;
use raphtory_storage::{core_ops::CoreGraphOps, mutation::{
    addition_ops::{InternalAdditionOps, SessionAdditionOps},
    durability_ops::DurabilityOps, property_addition_ops::InternalPropertyAdditionOps
}};
use storage::wal::{GraphWalOps, WalOps};

pub trait PropertyAdditionOps:
    InternalPropertyAdditionOps<Error: Into<GraphError>> + InternalAdditionOps<Error: Into<GraphError>>
{
    fn add_properties<
        T: TryIntoInputTime,
        PN: AsRef<str>,
        PI: Into<Prop>,
        PII: IntoIterator<Item = (PN, PI)>,
    >(
        &self,
        t: T,
        props: PII,
    ) -> Result<(), GraphError>;

    fn add_metadata<PI: CollectProperties>(&self, props: PI) -> Result<(), GraphError>;

    fn update_metadata<PI: CollectProperties>(&self, props: PI) -> Result<(), GraphError>;
}

impl<
        G: InternalPropertyAdditionOps<Error: Into<GraphError>>
        + InternalAdditionOps<Error: Into<GraphError>>
        + CoreGraphOps
    > PropertyAdditionOps for G
{
    fn add_properties<
        T: TryIntoInputTime,
        PN: AsRef<str>,
        PI: Into<Prop>,
        PII: IntoIterator<Item = (PN, PI)>,
    >(
        &self,
        t: T,
        props: PII,
    ) -> Result<(), GraphError> {
        let transaction_manager = self.core_graph().transaction_manager()?;
        let wal = self.core_graph().wal()?;
        let transaction_id = transaction_manager.begin_transaction();
        let session = self.write_session().map_err(|err| err.into())?;

        let props_with_status = self.validate_props_with_status(
            false,
            self.graph_props_meta(),
            props.into_iter().map(|(k, v)| (k, v.into())),
        ).map_err(into_graph_err)?;

        let props_for_wal = props_with_status.iter().map(|maybe_new| {
            let (prop_name, prop_id, prop) = maybe_new.as_ref().inner();
            (prop_name.as_ref(), *prop_id, prop.clone())
        }).collect::<Vec<_>>();

        let props = props_with_status.iter().map(|maybe_new| {
            let (_, prop_id, prop) = maybe_new.as_ref().inner();
            (*prop_id, prop.clone())
        }).collect::<Vec<_>>();

        let t = time_from_input_session(&session, t)?;
        let mut writer = self.internal_add_properties(t, &props).map_err(into_graph_err)?;
        let lsn = wal.log_add_graph_props(transaction_id, t, props_for_wal).map_err(into_graph_err)?;

        writer.set_lsn(lsn);
        transaction_manager.end_transaction(transaction_id);
        drop(writer);

        if let Err(e) = wal.flush(lsn) {
            return Err(GraphError::FatalWriteError(e));
        }

        Ok(())
    }

    fn add_metadata<PI: CollectProperties>(&self, props: PI) -> Result<(), GraphError> {
        let session = self.write_session().map_err(|err| err.into())?;
        let properties: Vec<_> = props.collect_properties(|name, dtype| {
            Ok(session
                .resolve_graph_property(name, dtype, true)
                .map_err(into_graph_err)?
                .inner())
        })?;
        self.internal_add_metadata(&properties)
            .map_err(into_graph_err)?;
        Ok(())
    }

    fn update_metadata<PI: CollectProperties>(&self, props: PI) -> Result<(), GraphError> {
        let session = self.write_session().map_err(|err| err.into())?;
        let properties: Vec<_> = props.collect_properties(|name, dtype| {
            Ok(session
                .resolve_graph_property(name, dtype, true)
                .map_err(into_graph_err)?
                .inner())
        })?;
        self.internal_update_metadata(&properties)
            .map_err(into_graph_err)?;
        Ok(())
    }
}
