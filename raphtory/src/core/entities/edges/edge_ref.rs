use crate::core::{
    entities::{vertices::vertex_ref::VertexRef, EID, VID},
    storage::timeindex::{AsTime, TimeIndexEntry},
};

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct EdgeRef {
    e_pid: EID,
    src_pid: VID,
    dst_pid: VID,
    e_type: Dir,
    time: Option<TimeIndexEntry>,
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
    pub fn time(&self) -> Option<TimeIndexEntry> {
        self.time
    }

    #[inline(always)]
    pub fn time_t(&self) -> Option<i64> {
        self.time.map(|t| *t.t())
    }

    pub fn dir(&self) -> Dir {
        self.e_type
    }

    pub fn src(&self) -> VID {
        self.src_pid
    }

    pub fn dst(&self) -> VID {
        self.dst_pid
    }

    pub fn remote(&self) -> VID {
        match self.e_type {
            Dir::Into => self.src(),
            Dir::Out => self.dst(),
        }
    }

    pub fn local(&self) -> VID {
        match self.e_type {
            Dir::Into => self.dst(),
            Dir::Out => self.src(),
        }
    }

    #[inline(always)]
    pub(crate) fn pid(&self) -> EID {
        self.e_pid
    }

    pub fn at(&self, time: TimeIndexEntry) -> Self {
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
