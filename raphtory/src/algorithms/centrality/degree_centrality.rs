use crate::{
    db::api::{
        state::{GenericNodeState, TypedNodeState},
        view::StaticGraphViewOps,
    },
    prelude::*,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct CentralityScore {
    #[serde(rename = "degree_centrality")]
    score: f64,
}

/// Computes the degree centrality of all nodes in the graph. The values are normalized
/// by dividing each result with the maximum possible degree. Graphs with self-loops can have
/// values of centrality greater than 1.
///
/// # Arguments
///
/// - `g`: A reference to the graph.
///
/// # Returns
///
/// An [AlgorithmResult] containing the degree centrality of each node.
pub fn degree_centrality<G: StaticGraphViewOps>(
    g: &G,
) -> TypedNodeState<'static, HashMap<String, Option<Prop>>, G> {
    // NodeState<'static, f64, G> {
    let max_degree = match g.nodes().degree().max() {
        None => return GenericNodeState::new_empty(g.clone()).transform(),
        Some(v) => v,
    };

    let values: Vec<CentralityScore> = g
        .nodes()
        .degree()
        .into_par_iter_values()
        .map(|v| CentralityScore {
            score: (v as f64) / max_degree as f64,
        })
        .collect();

    // NodeState::new_from_values(g.clone(), values)
    GenericNodeState::new_from_eval(g.clone(), values).transform()
}

#[cfg(test)]
mod degree_centrality_test {
    use arrow_array::RecordBatch;
    use serde::Deserialize;
    use serde_arrow::Deserializer;
    use serde_json::Value;

    use crate::{
        algorithms::centrality::degree_centrality::{degree_centrality, CentralityScore},
        db::{
            api::{mutation::AdditionOps, state::NodeStateValue},
            graph::graph::Graph,
        },
        prelude::{NodeStateOps, NO_PROPS},
        test_storage,
    };
    use raphtory_api::core::entities::properties::prop::Prop;
    use std::collections::HashMap;

    fn des<T: NodeStateValue>(recordbatch: &RecordBatch) -> T {
        let deserializer = Deserializer::from_record_batch(recordbatch).unwrap();
        T::deserialize(deserializer.get(0).unwrap()).unwrap()
    }

    #[test]
    fn test_degree_centrality() {
        let graph = Graph::new();
        let vs = vec![(1, 2), (1, 3), (1, 4), (2, 3), (2, 4)];
        for (src, dst) in &vs {
            graph.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let mut expected: HashMap<String, f64> = HashMap::new();
            expected.insert("1".to_string(), 1.0);
            expected.insert("2".to_string(), 1.0);
            expected.insert("3".to_string(), 2.0 / 3.0);
            expected.insert("4".to_string(), 2.0 / 3.0);

            let res = degree_centrality(graph);
            println!("{:?}", des::<HashMap<String, Prop>>(res.state.values()))

            // let v: Vec<_> = res.iter_values().collect();
            // println!("{:?}", v);
            //assert_eq!(res, expected);
        });
    }
}
