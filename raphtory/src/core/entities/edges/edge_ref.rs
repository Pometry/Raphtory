use crate::core::entities::{vertices::vertex_ref::VertexRef, EID, VID};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum EdgeRef {
    LocalInto {
        e_pid: EID,
        layer_id: usize,
        src_pid: VID,
        dst_pid: VID,
        time: Option<i64>,
    },
    LocalOut {
        e_pid: EID,
        layer_id: usize,
        src_pid: VID,
        dst_pid: VID,
        time: Option<i64>,
    },
}

impl EdgeRef {
    #[inline(always)]
    pub fn layer(&self) -> usize {
        match &self {
            EdgeRef::LocalInto { layer_id, .. } => *layer_id,
            EdgeRef::LocalOut { layer_id, .. } => *layer_id,
        }
    }

    #[inline(always)]
    pub fn time(&self) -> Option<i64> {
        match self {
            EdgeRef::LocalInto { time, .. } => *time,
            EdgeRef::LocalOut { time, .. } => *time,
        }
    }

    pub fn src(&self) -> VertexRef {
        match self {
            EdgeRef::LocalInto { src_pid, .. } => (*src_pid).into(),
            EdgeRef::LocalOut { src_pid, .. } => (*src_pid).into(),
        }
    }

    pub fn dst(&self) -> VertexRef {
        match self {
            EdgeRef::LocalInto { dst_pid, .. } => (*dst_pid).into(),
            EdgeRef::LocalOut { dst_pid, .. } => (*dst_pid).into(),
        }
    }

    pub fn remote(&self) -> VertexRef {
        match self {
            EdgeRef::LocalInto { .. } => self.src(),
            EdgeRef::LocalOut { .. } => self.dst(),
        }
    }

    pub fn local(&self) -> VertexRef {
        match self {
            EdgeRef::LocalInto { .. } => self.dst(),
            EdgeRef::LocalOut { .. } => self.src(),
        }
    }

    #[inline(always)]
    pub(crate) fn pid(&self) -> EID {
        match self {
            EdgeRef::LocalOut { e_pid, .. } => *e_pid,
            EdgeRef::LocalInto { e_pid, .. } => *e_pid,
        }
    }

    pub fn at_layer(mut self, layer: usize) -> Self {
        match self {
            e_ref @ EdgeRef::LocalInto {
                ref mut layer_id, ..
            } => {
                *layer_id = layer;
                e_ref
            }
            e_ref @ EdgeRef::LocalOut {
                ref mut layer_id, ..
            } => {
                *layer_id = layer;
                e_ref
            }
        }
    }

    pub fn at(&self, time: i64) -> Self {
        match *self {
            EdgeRef::LocalInto {
                e_pid,
                layer_id,
                src_pid,
                dst_pid,
                ..
            } => EdgeRef::LocalInto {
                time: Some(time),
                e_pid,
                layer_id,
                src_pid,
                dst_pid,
            },
            EdgeRef::LocalOut {
                e_pid,
                layer_id,
                src_pid,
                dst_pid,
                ..
            } => EdgeRef::LocalOut {
                time: Some(time),
                e_pid,
                layer_id,
                src_pid,
                dst_pid,
            },
        }
    }
}
