use crate::{
    db::api::mutation::{time_from_input_session, TryIntoInputTime},
    errors::{into_graph_err, GraphError},
};
use raphtory_api::core::entities::properties::prop::Prop;
use raphtory_storage::{
    core_ops::CoreGraphOps,
    mutation::{
        addition_ops::{InternalAdditionOps},
        durability_ops::DurabilityOps,
        property_addition_ops::InternalPropertyAdditionOps,
    },
};
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

    fn add_metadata<
        PN: AsRef<str>,
        P: Into<Prop>,
        PII: IntoIterator<Item = (PN, P)>,
    >(&self, props: PII) -> Result<(), GraphError>;

    fn update_metadata<
        PN: AsRef<str>,
        P: Into<Prop>,
        PII: IntoIterator<Item = (PN, P)>,
    >(&self, props: PII) -> Result<(), GraphError>;
}

impl<
        G: InternalPropertyAdditionOps<Error: Into<GraphError>>
            + InternalAdditionOps<Error: Into<GraphError>>
            + CoreGraphOps,
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
        let t = time_from_input_session(&session, t)?;

        let props_with_status = self
            .validate_props_with_status(
                false,
                self.graph_props_meta(),
                props.into_iter().map(|(k, v)| (k, v.into())),
            )
            .map_err(into_graph_err)?;

        let props = props_with_status
            .iter()
            .map(|maybe_new| {
                let (_, prop_id, prop) = maybe_new.as_ref().inner();
                (*prop_id, prop.clone())
            })
            .collect::<Vec<_>>();

        let mut writer = self
            .internal_add_properties(t, &props)
            .map_err(into_graph_err)?;

        let props_for_wal = props_with_status
            .iter()
            .map(|maybe_new| {
                let (prop_name, prop_id, prop) = maybe_new.as_ref().inner();
                (prop_name.as_ref(), *prop_id, prop.clone())
            })
            .collect::<Vec<_>>();

        let lsn = wal
            .log_add_graph_props(transaction_id, t, props_for_wal)
            .map_err(into_graph_err)?;

        writer.set_lsn(lsn);
        transaction_manager.end_transaction(transaction_id);
        drop(writer);

        if let Err(e) = wal.flush(lsn) {
            return Err(GraphError::FatalWriteError(e));
        }

        Ok(())
    }

    fn add_metadata<
        PN: AsRef<str>,
        P: Into<Prop>,
        PII: IntoIterator<Item = (PN, P)>,
    >(&self, props: PII) -> Result<(), GraphError> {
        let is_update = false;
        add_metadata_impl(self, props, is_update)
    }

    fn update_metadata<
        PN: AsRef<str>,
        P: Into<Prop>,
        PII: IntoIterator<Item = (PN, P)>,
    >(&self, props: PII) -> Result<(), GraphError> {
        let is_update = true;
        add_metadata_impl(self, props, is_update)
    }
}

fn add_metadata_impl<G, PN, P, PII>(
    graph: &G,
    props: PII,
    is_update: bool,
) -> Result<(), GraphError>
where
    G: InternalPropertyAdditionOps<Error: Into<GraphError>>
        + InternalAdditionOps<Error: Into<GraphError>>
        + CoreGraphOps,
    PN: AsRef<str>,
    P: Into<Prop>,
    PII: IntoIterator<Item = (PN, P)>,
{
    let transaction_manager = graph.core_graph().transaction_manager()?;
    let wal = graph.core_graph().wal()?;
    let transaction_id = transaction_manager.begin_transaction();

    let props_with_status = graph
        .validate_props_with_status(
            true,
            graph.graph_props_meta(),
            props.into_iter().map(|(k, v)| (k, v.into())),
        )
        .map_err(into_graph_err)?;

    let props = props_with_status
        .iter()
        .map(|maybe_new| {
            let (_, prop_id, prop) = maybe_new.as_ref().inner();
            (*prop_id, prop.clone())
        })
        .collect::<Vec<_>>();

    let mut writer = if is_update {
        graph.internal_update_metadata(&props).map_err(into_graph_err)?
    } else {
        graph.internal_add_metadata(&props).map_err(into_graph_err)?
    };

    let props_for_wal = props_with_status
        .iter()
        .map(|maybe_new| {
            let (prop_name, prop_id, prop) = maybe_new.as_ref().inner();
            (prop_name.as_ref(), *prop_id, prop.clone())
        })
        .collect::<Vec<_>>();

    let lsn = wal
        .log_add_graph_metadata(transaction_id, props_for_wal)
        .map_err(into_graph_err)?;

    writer.set_lsn(lsn);
    transaction_manager.end_transaction(transaction_id);
    drop(writer);

    if let Err(e) = wal.flush(lsn) {
        return Err(GraphError::FatalWriteError(e));
    }

    Ok(())
}
