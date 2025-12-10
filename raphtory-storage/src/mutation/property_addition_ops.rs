use crate::{
    graph::graph::GraphStorage,
    mutation::{EdgeWriterT, MutationError, NodeWriterT},
};
use raphtory_api::{
    core::{
        entities::{properties::prop::Prop, EID, VID},
        storage::timeindex::TimeIndexEntry,
    },
    inherit::Base,
};
use storage::Extension;

pub trait InternalPropertyAdditionOps {
    type Error: From<MutationError>;

    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error>;

    fn internal_add_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error>;

    fn internal_update_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error>;

    fn internal_add_node_metadata(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<NodeWriterT<'_>, Self::Error>;

    fn internal_update_node_metadata(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<NodeWriterT<'_>, Self::Error>;

    fn internal_add_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<EdgeWriterT<'_>, Self::Error>;

    fn internal_update_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<EdgeWriterT<'_>, Self::Error>;
}

impl InternalPropertyAdditionOps for db4_graph::TemporalGraph<Extension> {
    type Error = MutationError;

    // FIXME: this can't fail
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        let mut writer = self.storage().graph_props().writer();
        writer.add_properties(t, props.iter().map(|(id, prop)| (*id, prop.clone())), 0);
        Ok(())
    }

    fn internal_add_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        let mut writer = self.storage().graph_props().writer();
        writer.check_metadata(props)?;
        writer.update_metadata(props.iter().map(|(id, prop)| (*id, prop.clone())), 0);
        Ok(())
    }

    // FIXME: this can't fail
    fn internal_update_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        let mut writer = self.storage().graph_props().writer();
        writer.update_metadata(props.iter().map(|(id, prop)| (*id, prop.clone())), 0);
        Ok(())
    }

    fn internal_add_node_metadata(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<NodeWriterT<'_>, Self::Error> {
        let (segment_id, node_pos) = self.storage().nodes().resolve_pos(vid);
        let mut writer = self.storage().nodes().writer(segment_id);
        writer.check_metadata(node_pos, 0, &props)?;
        writer.update_c_props(node_pos, 0, props);
        Ok(writer)
    }

    fn internal_update_node_metadata(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<NodeWriterT<'_>, Self::Error> {
        let (segment_id, node_pos) = self.storage().nodes().resolve_pos(vid);
        let mut writer = self.storage().nodes().writer(segment_id);
        writer.update_c_props(node_pos, 0, props);
        Ok(writer)
    }

    fn internal_add_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<EdgeWriterT<'_>, Self::Error> {
        let (_, edge_pos) = self.storage().edges().resolve_pos(eid);
        let mut writer = self.storage().edge_writer(eid);
        let (src, dst) = writer.get_edge(layer, edge_pos).unwrap_or_else(|| {
            panic!("Edge with EID {eid:?} not found in layer {layer}");
        });
        writer.check_metadata(edge_pos, layer, &props)?;
        writer.update_c_props(edge_pos, src, dst, layer, props);
        Ok(writer)
    }

    fn internal_update_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<EdgeWriterT<'_>, Self::Error> {
        let (_, edge_pos) = self.storage().edges().resolve_pos(eid);
        let mut writer = self.storage().edge_writer(eid);
        let (src, dst) = writer.get_edge(layer, edge_pos).unwrap_or_else(|| {
            panic!("Edge with EID {eid:?} not found in layer {layer}");
        });
        writer.update_c_props(edge_pos, src, dst, layer, props);
        Ok(writer)
    }
}

impl InternalPropertyAdditionOps for GraphStorage {
    type Error = MutationError;

    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        Ok(self.mutable()?.internal_add_properties(t, props)?)
    }

    fn internal_add_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        self.mutable()?.internal_add_metadata(props)
    }

    fn internal_update_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        self.mutable()?.internal_update_metadata(props)
    }

    fn internal_add_node_metadata(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<NodeWriterT<'_>, Self::Error> {
        self.mutable()?.internal_add_node_metadata(vid, props)
    }

    fn internal_update_node_metadata(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<NodeWriterT<'_>, Self::Error> {
        self.mutable()?.internal_update_node_metadata(vid, props)
    }

    fn internal_add_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<EdgeWriterT<'_>, Self::Error> {
        self.mutable()?
            .internal_add_edge_metadata(eid, layer, props)
    }

    fn internal_update_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<EdgeWriterT<'_>, Self::Error> {
        self.mutable()?
            .internal_update_edge_metadata(eid, layer, props)
    }
}

pub trait InheritPropertyAdditionOps: Base {}

impl<G: InheritPropertyAdditionOps> InternalPropertyAdditionOps for G
where
    G::Base: InternalPropertyAdditionOps,
{
    type Error = <G::Base as InternalPropertyAdditionOps>::Error;

    #[inline]
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.base().internal_add_properties(t, props)
    }

    #[inline]
    fn internal_add_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        self.base().internal_add_metadata(props)
    }

    #[inline]
    fn internal_update_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        self.base().internal_update_metadata(props)
    }

    #[inline]
    fn internal_add_node_metadata(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<NodeWriterT<'_>, Self::Error> {
        self.base().internal_add_node_metadata(vid, props)
    }

    #[inline]
    fn internal_update_node_metadata(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<NodeWriterT<'_>, Self::Error> {
        self.base().internal_update_node_metadata(vid, props)
    }

    #[inline]
    fn internal_add_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<EdgeWriterT<'_>, Self::Error> {
        self.base().internal_add_edge_metadata(eid, layer, props)
    }

    #[inline]
    fn internal_update_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<EdgeWriterT<'_>, Self::Error> {
        self.base().internal_update_edge_metadata(eid, layer, props)
    }
}
