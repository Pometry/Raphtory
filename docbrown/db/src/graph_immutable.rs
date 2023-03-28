use crate::graph::Graph;
use docbrown_core::tgraph::TemporalGraph;
use docbrown_core::tgraph_shard::ImmutableTGraphShard;
use docbrown_core::Direction;
use docbrown_core::{
    tgraph::{EdgeRef, VertexRef},
    utils,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImmutableGraph {
    pub(crate) nr_shards: usize,
    pub(crate) shards: Vec<ImmutableTGraphShard<TemporalGraph>>,
}

#[derive(Debug, PartialEq)]
pub struct UnfreezeFailure;

impl ImmutableGraph {
    pub fn unfreeze(self) -> Result<Graph, UnfreezeFailure> {
        let mut shards = Vec::with_capacity(self.shards.len());
        for shard in self.shards {
            match shard.unfreeze() {
                Ok(t) => shards.push(t),
                Err(_) => return Err(UnfreezeFailure),
            }
        }
        Ok(Graph {
            nr_shards: self.nr_shards,
            shards,
        })
    }

    pub fn shard_id(&self, g_id: u64) -> usize {
        utils::get_shard_id_from_global_vid(g_id, self.nr_shards)
    }

    pub fn get_shard_from_id(&self, g_id: u64) -> &ImmutableTGraphShard<TemporalGraph> {
        &self.shards[self.shard_id(g_id)]
    }

    pub fn get_shard_from_v(&self, v: VertexRef) -> &ImmutableTGraphShard<TemporalGraph> {
        &self.shards[self.shard_id(v.g_id)]
    }

    pub fn get_shard_from_e(&self, e: EdgeRef) -> &ImmutableTGraphShard<TemporalGraph> {
        &self.shards[self.shard_id(e.src_g_id)]
    }

    pub fn earliest_time(&self) -> Option<i64> {
        let min_from_shards = self.shards.iter().map(|shard| shard.earliest_time()).min();
        min_from_shards.filter(|&min| min != i64::MAX)
    }

    pub fn latest_time(&self) -> Option<i64> {
        let max_from_shards = self.shards.iter().map(|shard| shard.latest_time()).max();
        max_from_shards.filter(|&max| max != i64::MIN)
    }

    pub fn degree(&self, v: VertexRef, d: Direction) -> usize {
        self.get_shard_from_v(v).degree(v.g_id, d)
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = VertexRef> + Send + '_> {
        Box::new(self.shards.iter().flat_map(|s| s.vertices()))
    }

    pub fn edges(&self) -> Box<dyn Iterator<Item = (usize, EdgeRef)> + Send + '_> {
        Box::new(
            self.vertices()
                .flat_map(|v| self.get_shard_from_v(v).edges(v.g_id, Direction::OUT)),
        )
    }

    pub fn num_edges(&self) -> usize {
        self.shards.iter().map(|shard| shard.out_edges_len()).sum()
    }
}
