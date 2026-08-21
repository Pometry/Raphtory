//! Highest-scoring paths to a destination node, with per-layer and per-node-type scoring.
//!
//! Unlike [`dijkstra`](super::dijkstra), this maximises an additive score rather than minimising a
//! distance, and scores may be negative. Maximising an additive score over arbitrary-length paths
//! is unbounded (a positive cycle can be traversed forever) and NP-hard over simple paths, so the
//! search is bounded by a hop cutoff and paths are constrained to be simple.

use crate::{
    core::entities::nodes::node_ref::{AsNodeRef, NodeRef},
    db::api::view::StaticGraphViewOps,
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::{
    entities::{properties::prop::PropUnwrap, VID},
    storage::arc_str::{ArcStr, OptionAsStr},
    Direction,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
};

/// Hop cutoff used when the caller does not supply one.
pub const DEFAULT_MAX_HOPS: usize = 4;

fn unit_scale() -> f64 {
    1.0
}

/// How a single property contributes to the score of an edge or node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyScore {
    /// Name of the property to read. Temporal properties are checked first, then metadata.
    pub name: String,
    /// Multiplier applied to a numeric property value. Ignored when `categories` is set.
    #[serde(default = "unit_scale")]
    pub scale: f64,
    /// Maps a property value to a score. When set, the property is scored by lookup rather than by
    /// its numeric value; values are matched on their string form.
    #[serde(default)]
    pub categories: Option<HashMap<String, f64>>,
    /// Score contributed when the property is absent, is not numeric, or its value is not in
    /// `categories`.
    #[serde(default)]
    pub default: f64,
}

impl Default for PropertyScore {
    fn default() -> Self {
        Self {
            name: String::new(),
            scale: 1.0,
            categories: None,
            default: 0.0,
        }
    }
}

/// Scoring rules for one edge layer or one node type.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EntityScore {
    /// Base score contributed by every edge in this layer, or every node of this type.
    pub weight: f64,
    /// Property contributions, summed on top of `weight`.
    pub properties: Vec<PropertyScore>,
}

/// The scoring rules for a whole graph, keyed by edge layer and node type.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScoringMap {
    /// Scoring rules per edge layer.
    pub layers: HashMap<String, EntityScore>,
    /// Rules applied to layers with no entry in `layers`. Unlisted layers score 0 when unset.
    pub default_layer: Option<EntityScore>,
    /// Scoring rules per node type.
    pub node_types: HashMap<String, EntityScore>,
    /// Rules applied to node types with no entry in `node_types`. Unlisted types score 0 when unset.
    pub default_node_type: Option<EntityScore>,
    /// Only traverse layers with an explicit entry in `layers`.
    pub skip_unscored_layers: bool,
    /// Only route through nodes whose type has an explicit entry in `node_types`. The destination
    /// and the start nodes are subject to this too.
    pub skip_unscored_node_types: bool,
}

/// A path to the destination, together with its total score.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScoredPath {
    /// Sum of every node score and edge score along the path.
    pub score: f64,
    /// Nodes in traversal order, starting at the start node and ending at the destination.
    pub nodes: Vec<VID>,
    /// Layer traversed at each hop: `layers[i]` connects `nodes[i]` to `nodes[i + 1]`.
    pub layers: Vec<ArcStr>,
}

/// A path under construction, held in reverse: `nodes[0]` is the destination and `head` is the
/// node the search has reached working backwards from it.
#[derive(Clone, Debug)]
struct Partial {
    head: VID,
    score: f64,
    nodes: Vec<VID>,
    layers: Vec<ArcStr>,
}

impl Partial {
    fn finish(&self) -> ScoredPath {
        let mut nodes = self.nodes.clone();
        nodes.reverse();
        let mut layers = self.layers.clone();
        layers.reverse();
        ScoredPath {
            score: self.score,
            nodes,
            layers,
        }
    }
}

fn score_entity<F>(spec: &EntityScore, get: F) -> f64
where
    F: Fn(&str) -> Option<Prop>,
{
    spec.properties.iter().fold(spec.weight, |acc, rule| {
        let contribution = match get(&rule.name) {
            None => rule.default,
            Some(value) => match &rule.categories {
                Some(categories) => categories
                    .get(&value.to_string())
                    .copied()
                    .unwrap_or(rule.default),
                None => value.as_f64().map_or(rule.default, |v| v * rule.scale),
            },
        };
        acc + contribution
    })
}

/// Scores one edge or node against the rules for its layer or type. Returns `None` when the entity
/// has no explicit rules and `skip_unscored` says to only traverse the ones that do.
fn score_with<F>(
    explicit: Option<&EntityScore>,
    fallback: Option<&EntityScore>,
    skip_unscored: bool,
    get: F,
) -> Option<f64>
where
    F: Fn(&str) -> Option<Prop>,
{
    match explicit {
        Some(spec) => Some(score_entity(spec, get)),
        None if skip_unscored => None,
        None => Some(fallback.map_or(0.0, |spec| score_entity(spec, get))),
    }
}

fn node_score<G: StaticGraphViewOps>(
    graph: &G,
    node: VID,
    scoring: &ScoringMap,
    memo: &mut HashMap<VID, Option<f64>>,
) -> Option<f64> {
    if let Some(cached) = memo.get(&node) {
        return *cached;
    }
    let view = graph.node(node).expect("node from traversal exists");
    let node_type = view.node_type();
    let score = score_with(
        node_type.as_str().and_then(|t| scoring.node_types.get(t)),
        scoring.default_node_type.as_ref(),
        scoring.skip_unscored_node_types,
        |name| {
            view.properties()
                .get(name)
                .or_else(|| view.metadata().get(name))
        },
    );
    memo.insert(node, score);
    score
}

fn missing_node(graph: &impl StaticGraphViewOps, node: NodeRef<'_>) -> GraphError {
    let gid = match node {
        NodeRef::Internal(vid) => graph.node_id(vid),
        NodeRef::External(gid) => gid.to_owned(),
    };
    GraphError::NodeMissingError(gid)
}

/// Finds the highest-scoring paths that reach `destination`.
///
/// Every node and every edge on a path contributes a score, looked up from `scoring` by node type
/// and by edge layer respectively. Scores are summed, and may be negative — a negative layer weight
/// makes the search route around that kind of relationship rather than forbid it, so a longer path
/// of positive edges can beat a direct negative one.
///
/// The search runs backwards from `destination` and paths are simple: no node appears twice.
///
/// # Arguments
///
/// * `graph`: The graph to search in.
/// * `destination`: The node every returned path ends at.
/// * `sources`: The nodes paths may start from. When `None`, every node is a candidate start.
/// * `scoring`: Per-layer and per-node-type scoring rules.
/// * `max_hops`: Longest path to consider, in edges. Defaults to [`DEFAULT_MAX_HOPS`].
/// * `top_k`: Return only this many paths, highest score first. When `None`, all are returned.
/// * `beam_width`: Keep at most this many partial paths per node at each hop. When `None` the
///   search is exhaustive and the result is the exact top-scoring set; setting it bounds the work
///   on graphs with high-degree nodes at the cost of possibly missing the true best path.
/// * `direction`: Direction the returned paths follow. [`Direction::OUT`] means each hop follows an
///   out-edge from the start towards the destination; [`Direction::BOTH`] ignores edge direction.
///
/// # Returns
///
/// Paths ordered by descending score. A path from `destination` to itself scores the destination's
/// own node score and is included when `destination` is a candidate start.
pub fn top_scoring_paths<G: StaticGraphViewOps, T: AsNodeRef>(
    graph: &G,
    destination: T,
    sources: Option<Vec<T>>,
    scoring: &ScoringMap,
    max_hops: Option<usize>,
    top_k: Option<usize>,
    beam_width: Option<NonZeroUsize>,
    direction: Direction,
) -> Result<Vec<ScoredPath>, GraphError> {
    let destination_ref = destination.as_node_ref();
    let destination = graph
        .node(destination_ref)
        .ok_or_else(|| missing_node(graph, destination_ref))?
        .node;

    let source_set = match &sources {
        None => None,
        Some(sources) => {
            let mut set = HashSet::with_capacity(sources.len());
            for source in sources {
                let source_ref = source.as_node_ref();
                let node = graph
                    .node(source_ref)
                    .ok_or_else(|| missing_node(graph, source_ref))?;
                set.insert(node.node);
            }
            Some(set)
        }
    };
    let is_source = |node: VID| {
        source_set
            .as_ref()
            .is_none_or(|sources| sources.contains(&node))
    };

    let max_hops = max_hops.unwrap_or(DEFAULT_MAX_HOPS);
    let beam_width = beam_width.map(NonZeroUsize::get);
    let mut memo = HashMap::new();
    let Some(destination_score) = node_score(graph, destination, scoring, &mut memo) else {
        return Ok(vec![]);
    };

    let mut results: Vec<ScoredPath> = Vec::new();
    let mut frontier = vec![Partial {
        head: destination,
        score: destination_score,
        nodes: vec![destination],
        layers: vec![],
    }];
    if is_source(destination) {
        results.push(frontier[0].finish());
    }

    for _ in 0..max_hops {
        if frontier.is_empty() {
            break;
        }
        let mut next: HashMap<VID, Vec<Partial>> = HashMap::new();
        for partial in &frontier {
            let head = graph
                .node(partial.head)
                .expect("node from traversal exists");
            // The search walks backwards, so it follows edges opposite to the requested direction.
            let edges = match direction {
                Direction::OUT => head.in_edges(),
                Direction::IN => head.out_edges(),
                Direction::BOTH => head.edges(),
            };
            for edge in edges {
                let neighbour = edge.nbr().node;
                if partial.nodes.contains(&neighbour) {
                    continue;
                }
                let Some(neighbour_score) = node_score(graph, neighbour, scoring, &mut memo) else {
                    continue;
                };
                // One candidate transition per layer connecting the pair, so a strongly scored
                // relationship is picked over a weak one between the same two nodes.
                for layered in edge.explode_layers() {
                    let layer = layered.layer_name()?;
                    let Some(edge_score) = score_with(
                        scoring.layers.get(&*layer),
                        scoring.default_layer.as_ref(),
                        scoring.skip_unscored_layers,
                        |name| {
                            layered
                                .properties()
                                .get(name)
                                .or_else(|| layered.metadata().get(name))
                        },
                    ) else {
                        continue;
                    };

                    let mut nodes = partial.nodes.clone();
                    nodes.push(neighbour);
                    let mut layers = partial.layers.clone();
                    layers.push(layer);
                    let candidate = Partial {
                        head: neighbour,
                        score: partial.score + edge_score + neighbour_score,
                        nodes,
                        layers,
                    };

                    let bucket = next.entry(neighbour).or_default();
                    bucket.push(candidate);
                    if let Some(beam_width) = beam_width {
                        if bucket.len() > beam_width {
                            bucket.sort_unstable_by(|a, b| b.score.total_cmp(&a.score));
                            bucket.truncate(beam_width);
                        }
                    }
                }
            }
        }
        frontier = next.into_values().flatten().collect();
        results.extend(
            frontier
                .iter()
                .filter(|partial| is_source(partial.head))
                .map(Partial::finish),
        );
        // Keep the accumulated results from growing without bound when the caller only wants the
        // best few and every node is a candidate start.
        if let Some(top_k) = top_k {
            if results.len() > top_k.saturating_mul(4) {
                results.sort_by(|a, b| b.score.total_cmp(&a.score));
                results.truncate(top_k);
            }
        }
    }

    results.sort_by(|a, b| b.score.total_cmp(&a.score));
    if let Some(top_k) = top_k {
        results.truncate(top_k);
    }
    Ok(results)
}
