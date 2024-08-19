use crate::{
    core::{Prop, PropType},
    db::api::view::serialise::ProtoGraph,
};
use parking_lot::Mutex;
use prost::Message;
use raphtory_api::core::{
    entities::{GidRef, EID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use std::{fs::File, io, io::Write, mem, ops::DerefMut};

pub struct GraphWriter {
    writer: Mutex<Box<dyn Write + Send>>,
    proto_delta: Mutex<ProtoGraph>,
}

impl GraphWriter {
    pub fn write(&self) -> Result<(), io::Error> {
        let proto = mem::take(self.proto_delta.lock().deref_mut());
        let bytes = proto.encode_to_vec();
        self.writer.lock().write_all(&bytes)?;
        Ok(())
    }

    #[inline]
    pub fn resolve_layer(&self, layer: Option<&str>, layer_id: MaybeNew<usize>) {
        layer_id.if_new(|id| {
            if let Some(layer) = layer {
                self.proto_delta.lock().new_layer(layer, id)
            }
        });
    }

    pub fn resolve_node(&self, vid: MaybeNew<VID>, gid: GidRef) {
        vid.if_new(|vid| self.proto_delta.lock().new_node(gid, vid, 0));
    }

    pub fn resolve_node_and_type(
        &self,
        node_and_type: MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>,
        node_type: &str,
        gid: GidRef,
    ) {
        if let MaybeNew::New((MaybeNew::Existing(node_id), type_id)) = node_and_type {
            // type assignment changed but node already exists
            self.proto_delta
                .lock()
                .update_node_type(node_id, type_id.inner());
        }
        if let (MaybeNew::New(node_id), type_id) = node_and_type.inner() {
            self.proto_delta
                .lock()
                .new_node(gid, node_id, type_id.inner());
        }
        if let (_, MaybeNew::New(type_id)) = node_and_type.inner() {
            self.proto_delta.lock().new_node_type(node_type, type_id);
        }
    }

    pub fn resolve_graph_property(
        &self,
        prop: &str,
        prop_id: MaybeNew<usize>,
        dtype: PropType,
        is_static: bool,
    ) {
        prop_id.if_new(|id| {
            if is_static {
                self.proto_delta.lock().new_graph_cprop(prop, id);
            } else {
                self.proto_delta.lock().new_graph_tprop(prop, id, &dtype);
            }
        });
    }

    pub fn resolve_node_property(
        &self,
        prop: &str,
        prop_id: MaybeNew<usize>,
        dtype: PropType,
        is_static: bool,
    ) {
        prop_id.if_new(|id| {
            if is_static {
                self.proto_delta.lock().new_node_cprop(prop, id, &dtype);
            } else {
                self.proto_delta.lock().new_node_tprop(prop, id, &dtype);
            }
        });
    }

    pub fn resolve_edge_property(
        &self,
        prop: &str,
        prop_id: MaybeNew<usize>,
        dtype: PropType,
        is_static: bool,
    ) {
        prop_id.if_new(|id| {
            if is_static {
                self.proto_delta.lock().new_edge_cprop(prop, id, &dtype);
            } else {
                self.proto_delta.lock().new_edge_tprop(prop, id, &dtype);
            }
        });
    }

    pub fn add_node_update(&self, t: TimeIndexEntry, v: VID, props: &[(usize, Prop)]) {
        self.proto_delta
            .lock()
            .update_node_tprops(v, t, props.iter().map(|(id, prop)| (*id, prop)))
    }

    pub fn resolve_edge(&self, eid: MaybeNew<EID>, src: VID, dst: VID) {
        eid.if_new(|eid| self.proto_delta.lock().new_edge(src, dst, eid));
    }

    pub fn add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) {
        self.proto_delta.lock().update_edge_tprops(
            edge,
            t,
            layer,
            props.iter().map(|(id, prop)| (*id, prop)),
        )
    }
    pub fn add_graph_tprops(&self, t: TimeIndexEntry, props: &[(usize, Prop)]) {
        self.proto_delta
            .lock()
            .update_graph_tprops(t, props.iter().map(|(id, prop)| (*id, prop)))
    }

    pub fn add_graph_cprops(&self, props: &[(usize, Prop)]) {
        self.proto_delta
            .lock()
            .update_graph_cprops(props.iter().map(|(id, prop)| (*id, prop)))
    }

    pub fn add_node_cprops(&self, node: VID, props: &[(usize, Prop)]) {
        self.proto_delta
            .lock()
            .update_node_cprops(node, props.iter().map(|(id, prop)| (*id, prop)))
    }

    pub fn add_edge_cprops(&self, edge: EID, layer: usize, props: &[(usize, Prop)]) {
        self.proto_delta.lock().update_edge_cprops(
            edge,
            layer,
            props.iter().map(|(id, prop)| (*id, prop)),
        )
    }

    pub fn delete_edge(&self, edge: EID, t: TimeIndexEntry, layer: usize) {
        self.proto_delta.lock().del_edge(edge, layer, t)
    }
}
