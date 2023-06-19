use serde::{Deserialize, Serialize};

use crate::core::{timeindex::TimeIndex, Prop};

use super::{props::Props, VID};

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub(crate) struct EdgeStore<const N: usize> {
    src: VID,
    dst: VID,
    timestamps: TimeIndex,
    props: Props,
}


impl <const N: usize> EdgeStore<N> {
    pub fn new(src: VID, dst: VID, t: i64) -> Self {
        Self {
            src,
            dst,
            timestamps: TimeIndex::one(t),
            props: Props::new(),
        }
    }

    pub fn update_time(&mut self, t: i64) {
        self.timestamps.insert(t);
    }

    pub fn add_prop(&mut self, t: i64, prop_id: usize, prop: Prop) {
        self.props.add_prop(t, prop_id, prop);
    }

    pub fn timestamps(&self) -> &TimeIndex {
        &self.timestamps
    }
}
