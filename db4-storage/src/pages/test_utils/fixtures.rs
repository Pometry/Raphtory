use std::collections::HashMap;
use proptest::{collection, prelude::*};
use raphtory_core::entities::VID;
use raphtory_api::core::entities::properties::prop::Prop;

use super::props::{make_props, prop_type};

pub type AddEdge = (
    VID,
    VID,
    i64,
    Vec<(String, Prop)>,
    Vec<(String, Prop)>,
    Option<&'static str>,
);

#[derive(Debug)]
pub struct NodeFixture {
    pub temp_props: Vec<(VID, i64, Vec<(String, Prop)>)>,
    pub const_props: Vec<(VID, Vec<(String, Prop)>)>,
}

#[derive(Debug)]
pub struct Fixture {
    pub edges: Vec<AddEdge>,
    pub const_props: HashMap<(VID, VID), Vec<(String, Prop)>>,
}

impl From<Vec<AddEdge>> for Fixture {
    fn from(edges: Vec<AddEdge>) -> Self {
        let mut const_props = HashMap::new();
        for (src, dst, _, _, c_props, _) in &edges {
            for (k, v) in c_props {
                const_props
                    .entry((*src, *dst))
                    .or_insert_with(|| vec![])
                    .push((k.clone(), v.clone()));
            }
        }
        const_props.iter_mut().for_each(|(_, v)| {
            v.sort_by(|a, b| a.0.cmp(&b.0));
            v.dedup_by(|a, b| a.0 == b.0);
        });
        Self { edges, const_props }
    }
}

pub fn make_edges(num_edges: usize, num_nodes: usize) -> impl Strategy<Value = Fixture> {
    assert!(num_edges > 0);
    assert!(num_nodes > 0);
    (1..=num_edges, 1..=num_nodes)
        .prop_flat_map(|(len, num_nodes)| build_raw_edges(len, num_nodes))
        .prop_map(|edges| edges.into())
}

pub fn make_nodes(num_nodes: usize) -> impl Strategy<Value = NodeFixture> {
    assert!(num_nodes > 0);
    let schema = proptest::collection::hash_map(
        (0i32..1000).prop_map(|i| i.to_string()),
        prop_type(),
        0..30,
    );

    schema.prop_flat_map(move |schema| {
        let (t_props, c_props) = make_props(&schema);
        let temp_props = proptest::collection::vec(
            ((0..num_nodes).prop_map(VID), 0i64..1000, t_props),
            1..=num_nodes,
        );

        let const_props =
            proptest::collection::vec(((0..num_nodes).prop_map(VID), c_props), 1..=num_nodes);

        (temp_props, const_props).prop_map(|(temp_props, const_props)| NodeFixture {
            temp_props,
            const_props,
        })
    })
}

pub fn edges_strat(size: usize) -> impl Strategy<Value = Vec<(VID, VID)>> {
    (1..=size).prop_flat_map(|num_nodes| {
        let num_edges = 0..(num_nodes * num_nodes);
        let srcs = (0usize..num_nodes).prop_map(VID);
        let dsts = (0usize..num_nodes).prop_map(VID);
        num_edges.prop_flat_map(move |num_edges| {
            collection::vec((srcs.clone(), dsts.clone()), num_edges as usize)
        })
    })
}

pub fn edges_strat_with_layers(size: usize) -> impl Strategy<Value = Vec<(VID, VID, usize)>> {
    const MAX_LAYERS: usize = 16;

    (1..=size).prop_flat_map(|num_nodes| {
        let num_edges = 0..(num_nodes * num_nodes);
        let srcs = (0usize..num_nodes).prop_map(VID);
        let dsts = (0usize..num_nodes).prop_map(VID);
        let layer_ids = (0usize..MAX_LAYERS).prop_map(|i| i as usize);

        num_edges.prop_flat_map(move |num_edges| {
            collection::vec((srcs.clone(), dsts.clone(), layer_ids.clone()), num_edges as usize)
        })
    })
}

pub fn build_raw_edges(
    len: usize,
    num_nodes: usize,
) -> impl Strategy<
    Value = Vec<(
        VID,
        VID,
        i64,
        Vec<(String, Prop)>,
        Vec<(String, Prop)>,
        Option<&'static str>,
    )>,
> {
    proptest::collection::hash_map((0i32..1000).prop_map(|i| i.to_string()), prop_type(), 0..20)
        .prop_flat_map(move |schema| {
            let (t_props, c_props) = make_props(&schema);

            proptest::collection::vec(
                (
                    (0..num_nodes).prop_map(VID),
                    (0..num_nodes).prop_map(VID),
                    0i64..(num_nodes as i64 * 5),
                    t_props,
                    c_props,
                    proptest::sample::select(vec![Some("a"), Some("b"), None]),
                ),
                1..=len,
            )
        })
}
