use crate::core::tgraph_shard::errors::GraphError;
use crate::db::graph::{Graph, InternalGraph};
use crate::db::graph_deletions::GraphWithDeletions;
use crate::db::mutation_api::*;
use crate::db::view_api::internal::{CoreGraphOps, GraphOps, GraphPropertiesOps};
use crate::db::view_api::*;

pub enum MaterializedGraph {
    EventGraph(Graph),
    PersistentGraph(GraphWithDeletions),
}

pub trait InternalMaterialize: GraphViewOps {
    fn new_base_graph(graph: InternalGraph) -> MaterializedGraph;

    fn include_deletions(&self) -> bool;

    fn materialize(&self) -> Result<MaterializedGraph, GraphError> {
        let g = InternalGraph::new(self.num_shards());
        for v in self.vertices().iter() {
            for h in v.history() {
                g.add_vertex(h, v.id(), [])?;
            }
            for (name, props) in v.property_histories() {
                for (t, prop) in props {
                    g.add_vertex(t, v.id(), [(name.clone(), prop)])?;
                }
            }
            g.add_vertex_properties(v.id(), v.static_properties())?;
        }

        for e in self.edges() {
            let layer_name = &e.layer_name().to_string();
            let mut layer: Option<&str> = None;
            if layer_name != "default layer" {
                layer = Some(layer_name)
            }
            for ee in e.explode() {
                g.add_edge(
                    ee.time().unwrap(),
                    ee.src().id(),
                    ee.dst().id(),
                    ee.properties(false),
                    layer,
                )?;
            }
            if self.include_deletions() {
                for t in self.edge_deletion_history(e.edge) {
                    g.delete_edge(t, e.src().id(), e.dst().id(), layer)?;
                }
            }

            g.add_edge_properties(e.src().id(), e.dst().id(), e.static_properties(), layer)?;
        }

        g.add_static_properties(self.static_properties())?;

        Ok(Self::new_base_graph(g))
    }
}
