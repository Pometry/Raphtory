use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        Prop,
    },
};
use enum_dispatch::enum_dispatch;
use raphtory_api::{
    core::{
        entities::{properties::props::Meta, AsNodeRef, NodeRef, EID, GID},
        storage::arc_str::ArcStr,
    },
    BoxedIter,
};
use raphtory_memstorage::{
    core::{
        entities::properties::{graph_meta::GraphMeta, tprop::TProp},
        storage::{
            locked_view::LockedView,
            timeindex::{TimeIndex, TimeIndexOps, TimeIndexWindow},
        },
    },
    db::api::{storage::graph::{
        edges::{edge_entry::EdgeStorageEntry, edges::EdgesStorage},
        nodes::{node_entry::NodeStorageEntry, nodes::NodesStorage},
        GraphStorage,
    }, view::internal::{core_ops::CoreGraphOps, inherit::Base}},
};
use std::{iter, ops::Range};

#[cfg(feature = "storage")]
use pometry_storage::timestamps::TimeStamps;
#[cfg(feature = "storage")]
use rayon::prelude::*;

/// Core functions that should (almost-)always be implemented by pointing at the underlying graph.


pub trait InheritCoreOps: Base {}

impl<G: InheritCoreOps> DelegateCoreOps for G
where
    G::Base: CoreGraphOps,
{
    type Internal = G::Base;

    #[inline]
    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateCoreOps {
    type Internal: CoreGraphOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateCoreOps + ?Sized> CoreGraphOps for G {
    #[inline]
    fn core_graph(&self) -> &GraphStorage {
        self.graph().core_graph()
    }
}

pub enum NodeAdditions<'a> {
    Mem(&'a TimeIndex<i64>),
    Locked(LockedView<'a, TimeIndex<i64>>),
    Range(TimeIndexWindow<'a, i64>),
    #[cfg(feature = "storage")]
    Col(Vec<TimeStamps<'a, i64>>),
}

impl<'b> TimeIndexOps for NodeAdditions<'b> {
    type IndexType = i64;
    type RangeType<'a>
        = NodeAdditions<'a>
    where
        Self: 'a;

    #[inline]
    fn active(&self, w: Range<i64>) -> bool {
        match self {
            NodeAdditions::Mem(index) => index.active_t(w),
            NodeAdditions::Locked(index) => index.active_t(w),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.par_iter().any(|index| index.active_t(w.clone())),
            NodeAdditions::Range(index) => index.active_t(w),
        }
    }

    fn range(&self, w: Range<i64>) -> Self::RangeType<'_> {
        match self {
            NodeAdditions::Mem(index) => NodeAdditions::Range(index.range(w)),
            NodeAdditions::Locked(index) => NodeAdditions::Range(index.range(w)),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => {
                let mut ranges = Vec::with_capacity(index.len());
                index
                    .par_iter()
                    .map(|index| index.range_t(w.clone()))
                    .collect_into_vec(&mut ranges);
                NodeAdditions::Col(ranges)
            }
            NodeAdditions::Range(index) => NodeAdditions::Range(index.range(w)),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            NodeAdditions::Mem(index) => index.first(),
            NodeAdditions::Locked(index) => index.first(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.par_iter().flat_map(|index| index.first()).min(),
            NodeAdditions::Range(index) => index.first(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            NodeAdditions::Mem(index) => index.last(),
            NodeAdditions::Locked(index) => index.last(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.par_iter().flat_map(|index| index.last()).max(),
            NodeAdditions::Range(index) => index.last(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = i64> + Send + '_> {
        match self {
            NodeAdditions::Mem(index) => index.iter(),
            NodeAdditions::Locked(index) => Box::new(index.iter()),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => Box::new(index.iter().flat_map(|index| index.iter())),
            NodeAdditions::Range(index) => index.iter(),
        }
    }

    fn len(&self) -> usize {
        match self {
            NodeAdditions::Mem(index) => index.len(),
            NodeAdditions::Locked(index) => index.len(),
            NodeAdditions::Range(range) => range.len(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(col) => col.len(),
        }
    }
}
