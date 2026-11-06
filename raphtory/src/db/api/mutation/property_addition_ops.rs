use crate::{
    db::api::mutation::{time_from_input_session, TryIntoInputTime},
    errors::{into_graph_err, GraphError},
};
use raphtory_api::core::entities::properties::prop::Prop;
use raphtory_storage::{
    durability_ops::DurabilityOps,
    mutation::addition_ops::{
        GraphMutationOps, InternalAdditionOps, MetadataMutationOps, PropertyMutationOps,
    },
};
use storage::{
    segments::graph_prop::segment::MemGraphPropSegment,
    wal::{GraphWalOps, WalOps},
};

pub trait PropertyAdditionOps: InternalAdditionOps {
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

    fn add_metadata<PN: AsRef<str>, P: Into<Prop>, PII: IntoIterator<Item = (PN, P)>>(
        &self,
        props: PII,
    ) -> Result<(), GraphError>;

    fn update_metadata<PN: AsRef<str>, P: Into<Prop>, PII: IntoIterator<Item = (PN, P)>>(
        &self,
        props: PII,
    ) -> Result<(), GraphError>;
}

impl<G: InternalAdditionOps> PropertyAdditionOps for G {
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
        let mutable_graph = self.mutable()?;
        let transaction_manager = mutable_graph.transaction_manager()?;
        let wal = mutable_graph.wal()?;
        let transaction_id = transaction_manager.begin_transaction();
        let t = time_from_input_session(&mutable_graph, t)?;

        let validated_props = mutable_graph
            .validate_props(false, mutable_graph.graph_props_meta(), props)
            .map_err(into_graph_err)?;
        let mut writer = mutable_graph.atomic_update_graph_props();

        // We are modifying the graph from this point, no failures allowed!
        let (prop_names, props) = validated_props.into_props_and_names();
        mutable_graph
            .graph_props_meta()
            .temporal_prop_mapper()
            .mark_props_in_layer(
                MemGraphPropSegment::DEFAULT_LAYER,
                props.iter().map(|(id, _)| *id),
            );
        let props_for_wal = prop_names
            .iter()
            .zip(props.iter())
            .map(|(prop_name, (prop_id, prop))| (prop_name.as_ref(), *prop_id, prop.clone()))
            .collect::<Vec<_>>();

        let lsn = wal
            .log_add_graph_props(transaction_id, t, props_for_wal)
            .map_err(into_graph_err)?;

        writer.update_properties(t, MemGraphPropSegment::DEFAULT_LAYER, props);
        writer.set_lsn(lsn);
        transaction_manager.end_transaction(transaction_id);
        drop(writer);

        if let Err(e) = wal.flush(lsn) {
            return Err(GraphError::FatalWriteError(e));
        }

        Ok(())
    }

    fn add_metadata<PN: AsRef<str>, P: Into<Prop>, PII: IntoIterator<Item = (PN, P)>>(
        &self,
        props: PII,
    ) -> Result<(), GraphError> {
        let is_update = false;
        add_metadata_impl(self, props, is_update)
    }

    fn update_metadata<PN: AsRef<str>, P: Into<Prop>, PII: IntoIterator<Item = (PN, P)>>(
        &self,
        props: PII,
    ) -> Result<(), GraphError> {
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
    G: InternalAdditionOps,
    PN: AsRef<str>,
    P: Into<Prop>,
    PII: IntoIterator<Item = (PN, P)>,
{
    let mutable_graph = graph.mutable()?;
    let transaction_manager = mutable_graph.transaction_manager()?;
    let wal = mutable_graph.wal()?;
    let transaction_id = transaction_manager.begin_transaction();

    let validated_props =
        mutable_graph.validate_props(true, mutable_graph.graph_props_meta(), props)?;
    let mut writer = mutable_graph.atomic_update_graph_props();
    if !is_update {
        writer.check_metadata(
            MemGraphPropSegment::DEFAULT_LAYER,
            validated_props.resolved(),
        )?;
    }
    // we are modifying the graph at this point, no failure allowed!
    let (prop_names, props) = validated_props.into_props_and_names();
    mutable_graph
        .graph_props_meta()
        .metadata_mapper()
        .mark_props_in_layer(
            MemGraphPropSegment::DEFAULT_LAYER,
            props.iter().map(|(id, _)| *id),
        );
    let props_for_wal = prop_names
        .iter()
        .zip(props.iter())
        .map(|(prop_name, (prop_id, prop))| (prop_name.as_ref(), *prop_id, prop.clone()))
        .collect::<Vec<_>>();
    let lsn = wal
        .log_add_graph_metadata(transaction_id, props_for_wal)
        .map_err(into_graph_err)?;

    writer.update_metadata(MemGraphPropSegment::DEFAULT_LAYER, props);

    writer.set_lsn(lsn);
    transaction_manager.end_transaction(transaction_id);
    drop(writer);

    if let Err(e) = wal.flush(lsn) {
        return Err(GraphError::FatalWriteError(e));
    }

    Ok(())
}
