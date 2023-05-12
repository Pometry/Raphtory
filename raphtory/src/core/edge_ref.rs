use crate::core::vertex_ref::VertexRef;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum EdgeRef {
    RemoteInto {
        e_pid: usize,
        shard_id: usize,
        layer_id: usize,
        src: u64,
        dst_pid: usize,
        time: Option<i64>,
    },
    RemoteOut {
        e_pid: usize,
        shard_id: usize,
        layer_id: usize,
        src_pid: usize,
        dst: u64,
        time: Option<i64>,
    },
    LocalInto {
        e_pid: usize,
        shard_id: usize,
        layer_id: usize,
        src_pid: usize,
        dst_pid: usize,
        time: Option<i64>,
    },
    LocalOut {
        e_pid: usize,
        shard_id: usize,
        layer_id: usize,
        src_pid: usize,
        dst_pid: usize,
        time: Option<i64>,
    },
}

impl EdgeRef {
    #[inline(always)]
    pub fn shard(&self) -> usize {
        match &self {
            EdgeRef::RemoteInto { shard_id, .. } => *shard_id,
            EdgeRef::RemoteOut { shard_id, .. } => *shard_id,
            EdgeRef::LocalInto { shard_id, .. } => *shard_id,
            EdgeRef::LocalOut { shard_id, .. } => *shard_id,
        }
    }

    #[inline(always)]
    pub fn layer(&self) -> usize {
        match &self {
            EdgeRef::RemoteInto { layer_id, .. } => *layer_id,
            EdgeRef::RemoteOut { layer_id, .. } => *layer_id,
            EdgeRef::LocalInto { layer_id, .. } => *layer_id,
            EdgeRef::LocalOut { layer_id, .. } => *layer_id,
        }
    }

    #[inline(always)]
    pub fn time(&self) -> Option<i64> {
        match self {
            EdgeRef::RemoteInto { time, .. } => *time,
            EdgeRef::RemoteOut { time, .. } => *time,
            EdgeRef::LocalInto { time, .. } => *time,
            EdgeRef::LocalOut { time, .. } => *time,
        }
    }

    pub fn src(&self) -> VertexRef {
        match self {
            EdgeRef::RemoteInto { src, .. } => VertexRef::Remote(*src),
            EdgeRef::RemoteOut {
                src_pid, shard_id, ..
            } => VertexRef::new_local(*src_pid, *shard_id),
            EdgeRef::LocalInto {
                src_pid, shard_id, ..
            } => VertexRef::new_local(*src_pid, *shard_id),
            EdgeRef::LocalOut {
                src_pid, shard_id, ..
            } => VertexRef::new_local(*src_pid, *shard_id),
        }
    }

    pub fn dst(&self) -> VertexRef {
        match self {
            EdgeRef::RemoteInto {
                dst_pid, shard_id, ..
            } => VertexRef::new_local(*dst_pid, *shard_id),
            EdgeRef::RemoteOut { dst, .. } => VertexRef::Remote(*dst),
            EdgeRef::LocalInto {
                dst_pid, shard_id, ..
            } => VertexRef::new_local(*dst_pid, *shard_id),
            EdgeRef::LocalOut {
                dst_pid, shard_id, ..
            } => VertexRef::new_local(*dst_pid, *shard_id),
        }
    }

    pub fn remote(&self) -> VertexRef {
        match self {
            EdgeRef::RemoteInto { .. } => self.src(),
            EdgeRef::RemoteOut { .. } => self.dst(),
            EdgeRef::LocalInto { .. } => self.src(),
            EdgeRef::LocalOut { .. } => self.dst(),
        }
    }

    pub fn local(&self) -> VertexRef {
        match self {
            EdgeRef::RemoteInto { .. } => self.dst(),
            EdgeRef::RemoteOut { .. } => self.src(),
            EdgeRef::LocalInto { .. } => self.dst(),
            EdgeRef::LocalOut { .. } => self.src(),
        }
    }

    pub fn is_remote(&self) -> bool {
        match self {
            EdgeRef::RemoteInto { .. } => true,
            EdgeRef::RemoteOut { .. } => true,
            EdgeRef::LocalInto { .. } => false,
            EdgeRef::LocalOut { .. } => false,
        }
    }

    pub fn is_local(&self) -> bool {
        !self.is_remote()
    }

    #[inline(always)]
    pub(in crate::core) fn pid(&self) -> usize {
        match self {
            EdgeRef::RemoteInto { e_pid, .. } => *e_pid,
            EdgeRef::RemoteOut { e_pid, .. } => *e_pid,
            EdgeRef::LocalInto { e_pid, .. } => *e_pid,
            EdgeRef::LocalOut { e_pid, .. } => *e_pid,
        }
    }

    pub(in crate::core) fn merge_cmp(&self, other: &EdgeRef) -> bool {
        (self.local(), self.remote(), self.time(), self.layer())
            < (other.local(), other.remote(), other.time(), other.layer())
    }

    pub fn at(&self, time: i64) -> Self {
        match *self {
            EdgeRef::RemoteInto {
                e_pid,
                shard_id,
                layer_id,
                src,
                dst_pid,
                ..
            } => EdgeRef::RemoteInto {
                time: Some(time),
                e_pid,
                shard_id,
                layer_id,
                src,
                dst_pid,
            },
            EdgeRef::RemoteOut {
                e_pid,
                shard_id,
                layer_id,
                src_pid,
                dst,
                ..
            } => EdgeRef::RemoteOut {
                time: Some(time),
                e_pid,
                shard_id,
                layer_id,
                src_pid,
                dst,
            },
            EdgeRef::LocalInto {
                e_pid,
                shard_id,
                layer_id,
                src_pid,
                dst_pid,
                ..
            } => EdgeRef::LocalInto {
                time: Some(time),
                e_pid,
                shard_id,
                layer_id,
                src_pid,
                dst_pid,
            },
            EdgeRef::LocalOut {
                e_pid,
                shard_id,
                layer_id,
                src_pid,
                dst_pid,
                ..
            } => EdgeRef::LocalOut {
                time: Some(time),
                e_pid,
                shard_id,
                layer_id,
                src_pid,
                dst_pid,
            },
        }
    }
}
