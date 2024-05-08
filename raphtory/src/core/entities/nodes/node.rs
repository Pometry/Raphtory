use crate::core::{
    entities::{
        edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
        graph::tgraph::TGraph,
        nodes::node_store::NodeStore,
        properties::{
            props::{DictMapper, Meta},
            tprop::TProp,
        },
        LayerIds, VID,
    },
    storage::{
        locked_view::LockedView,
        timeindex::{TimeIndex, TimeIndexEntry, TimeIndexOps},
        ArcEntry, Entry,
    },
    Direction, Prop,
};
use itertools::Itertools;
use std::{ops::Range, sync::Arc};
