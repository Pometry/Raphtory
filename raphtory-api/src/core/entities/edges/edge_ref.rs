use crate::core::{
    entities::{EID, VID},
    storage::timeindex::{AsTime, TimeIndexEntry},
};
use std::cmp::Ordering;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct EdgeRef {
    e_pid: EID,
    src_pid: VID,
    dst_pid: VID,
    e_type: Dir,
    time: Option<TimeIndexEntry>,
    layer_id: Option<usize>,
}

// This is used for merging iterators of EdgeRefs and only makes sense if the local node for both
// sides is the same
impl PartialOrd for EdgeRef {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.remote().partial_cmp(&other.remote())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Dir {
    Into,
    Out,
}

impl EdgeRef {
    #[inline]
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

    #[inline]
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

    #[inline]
    pub fn new(e_pid: EID, src_pid: VID, dst_pid: VID, dir: Dir) -> Self {
        Self {
            e_pid,
            src_pid,
            dst_pid,
            e_type: dir,
            time: None,
            layer_id: None,
        }
    }

    #[inline(always)]
    pub fn layer(&self) -> Option<usize> {
        self.layer_id
    }

    #[inline(always)]
    pub fn time(&self) -> Option<TimeIndexEntry> {
        self.time
    }

    #[inline(always)]
    pub fn time_t(&self) -> Option<i64> {
        self.time.map(|t| t.t())
    }

    #[inline]
    pub fn dir(&self) -> Dir {
        self.e_type
    }

    #[inline]
    pub fn src(&self) -> VID {
        self.src_pid
    }

    #[inline]
    pub fn dst(&self) -> VID {
        self.dst_pid
    }

    #[inline]
    pub fn remote(&self) -> VID {
        match self.e_type {
            Dir::Into => self.src(),
            Dir::Out => self.dst(),
        }
    }

    #[inline]
    pub fn local(&self) -> VID {
        match self.e_type {
            Dir::Into => self.dst(),
            Dir::Out => self.src(),
        }
    }

    #[inline(always)]
    pub fn pid(&self) -> EID {
        self.e_pid
    }

    #[inline]
    pub fn at(&self, time: TimeIndexEntry) -> Self {
        let mut e_ref = *self;
        e_ref.time = Some(time);
        e_ref
    }

    #[inline]
    pub fn at_layer(&self, layer: usize) -> Self {
        let mut e_ref = *self;
        e_ref.layer_id = Some(layer);
        e_ref
    }
}
