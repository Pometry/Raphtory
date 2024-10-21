use raphtory_api::core::utils::errors::GraphError;

use crate::{
    prelude::{GraphViewOps, StableDecode, StableEncode},
    search::IndexedGraph,
    serialise::{
        incremental::{GraphWriter, InternalCache},
        GraphFolder, ProtoGraph,
    },
};

impl<G: StableEncode> StableEncode for IndexedGraph<G> {
    fn encode_to_proto(&self) -> ProtoGraph {
        self.graph.encode_to_proto()
    }
}

impl<'graph, G: StableDecode + GraphViewOps<'graph>> StableDecode for IndexedGraph<G> {
    fn decode_from_proto(graph: &ProtoGraph) -> Result<Self, GraphError> {
        let inner = G::decode_from_proto(graph)?;
        let indexed = Self::from_graph(&inner)?;
        Ok(indexed)
    }
}

impl<G: InternalCache> InternalCache for IndexedGraph<G> {
    fn init_cache(&self, path: &GraphFolder) -> Result<(), GraphError> {
        self.graph.init_cache(path)
    }

    fn get_cache(&self) -> Option<&GraphWriter> {
        self.graph.get_cache()
    }
}
