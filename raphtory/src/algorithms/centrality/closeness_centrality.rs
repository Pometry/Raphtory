use crate::{core::entities::vertices::input_vertex::InputVertex, prelude::GraphViewOps};

pub fn closeness_centrality<G: GraphViewOps, T: InputVertex>(graph: &G, weight: String) {}
