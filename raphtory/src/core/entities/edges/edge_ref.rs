use bitvec::prelude::BitArray;

use crate::core::entities::{vertices::vertex_ref::VertexRef, EID, VID};

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct EdgeRef{
    e_pid: EID,
    src_pid: VID,
    dst_pid: VID,
    e_type: Dir,
    time: Option<i64>,
    layer_id: Option<usize>,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Dir {
    Into,
    Out,
}

impl EdgeRef {

    pub fn new_outgoing(e_pid: EID, src_pid: VID, dst_pid: VID) -> Self {
        EdgeRef {
            e_pid,
            src_pid,
            dst_pid,
            e_type: Dir::Out,
            time: None,
            layer_id: None,
        }
    }

    pub fn new_incoming(e_pid: EID, src_pid: VID, dst_pid: VID) -> Self {
        EdgeRef {
            e_pid,
            src_pid,
            dst_pid,
            e_type: Dir::Into,
            time: None,
            layer_id: None,
        }
    }

    #[inline(always)]
    pub fn layer(&self) -> Option<&usize> {
        self.layer_id.as_ref()
    }

    #[inline(always)]
    pub fn time(&self) -> Option<i64> {
        self.time
    }

    pub fn dir(&self) -> Dir {
        self.e_type
    }

    pub fn src(&self) -> VertexRef {
        self.src_pid.into()
    }

    pub fn dst(&self) -> VertexRef {
        self.dst_pid.into()
    }

    pub fn remote(&self) -> VertexRef {
        match self.e_type {
            Dir::Into => self.src(),
            Dir::Out => self.dst(),
        }
    }

    pub fn local(&self) -> VertexRef {
        match self.e_type {
            Dir::Into => self.dst(),
            Dir::Out => self.src(),
        }
    }

    #[inline(always)]
    pub(crate) fn pid(&self) -> EID {
        self.e_pid
    }

    pub fn at(&self, time: i64) -> Self {
        let mut e_ref = *self;
        e_ref.time = Some(time);
        e_ref
    }

    pub fn at_layer(&self, layer: usize) -> Self {
        let mut e_ref = *self;
        e_ref.layer_id = Some(layer);
        e_ref
    }
}
