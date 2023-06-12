use serde::{Deserialize, Serialize};

use crate::core::{timeindex::TimeIndex, Direction, Prop};

use super::{adj::Adj, edge_layer::EdgeLayer, props::Props, VID};

#[derive(Serialize, Deserialize)]
pub(crate) struct NodeStore<const N: usize> {
    global_id: u64,
    // all the timestamps that have been seen by this vertex
    timestamps: TimeIndex,
    // each layer represents a separate view of the graph
    layers: Vec<Adj>,
    // props for vertex
    props: Props,
}

impl<const N: usize> NodeStore<N> {
    pub fn new(global_id: u64, t: i64) -> Self {
        Self {
            global_id,
            timestamps: TimeIndex::one(t),
            layers: vec![Adj::Solo],
            props: Props::new(),
        }
    }

    pub fn update_time(&mut self, t: i64) {
        self.timestamps.insert(t);
    }

    pub fn add_prop(&mut self, t: i64, prop_id: usize, prop: Prop) {
        self.props.add_prop(t, prop_id, prop);
    }

    pub(crate) fn find_edge(&self, dst: VID) -> Option<super::EID> {
        for layer in self.layers.iter() {
            if let Some(eid) = layer.get_edge(dst, Direction::OUT) {
                return Some(eid);
            }
        }
        None
    }

    pub(crate) fn add_edge(
        &self,
        t: i64,
        v_id: VID,
        dir: Direction,
        layer: &str,
        edge_id: super::EID,
    ) -> super::EID {
        todo!()
    }
}
