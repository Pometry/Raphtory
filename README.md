# docbrown

Docbrow is an in memory graph database

dockbrown temporal graph tracks vertices, edges and properties over time, all changes are visible at any time window.

## Add vertices and edges at different times and iterate over various windows

```rust
use docbrown::graph::TemporalGraph;
use docbrown::Prop;


let mut g = TemporalGraph::default();

g.add_vertex(11, 1);
g.add_vertex(22, 2);
g.add_vertex(33, 3);
g.add_vertex(44, 4);

g.add_edge_props(
    11,
    22,
    2,
    vec![
        ("amount".into(), Prop::F64(12.34)),
        ("label".into(), Prop::Str("blerg".into())),
    ],
);

g.add_edge_props(
    22,
    33,
    3,
    vec![
        ("weight".into(), Prop::U32(12)),
        ("label".into(), Prop::Str("blerg".into())),
    ],
);

g.add_edge_props(33, 44, 4, vec![("label".into(), Prop::Str("blerg".into()))]);

g.add_edge_props(
    44,
    11,
    5,
    vec![
        ("weight".into(), Prop::U32(12)),
        ("amount".into(), Prop::F64(12.34)),
    ],
);

// betwen t:2 and t:4 (excluded) only 11, 22 and 33 are visible, 11 is visible because it has an edge at time 2
let vs = g
    .iter_vs_window(2..4)
    .map(|v| v.global_id())
    .collect::<Vec<_>>();
assert_eq!(vs, vec![11, 22, 33]);


// between t: 3 and t:6 (excluded) show the visible outbound edges
let vs = g
    .iter_vs_window(3..6)
    .flat_map(|v| {
        v.outbound().map(|e| e.global_dst()).collect::<Vec<_>>() // FIXME: we can't just return v.outbound().map(|e| e.global_dst()) here we might need to do so check lifetimes
    }).collect::<Vec<_>>();

assert_eq!(vs, vec![33, 44, 11]);

let edge_weights = g
    .outbound(11)
    .flat_map(|e| {
        let mut weight = e.props("weight").collect::<Vec<_>>();

        let mut amount = e.props("amount").collect::<Vec<_>>();

        let mut label = e.props("label").collect::<Vec<_>>();

        weight.append(&mut amount);
        weight.append(&mut label);
        weight
    })
    .collect::<Vec<_>>();

assert_eq!(
    edge_weights,
    vec![
        (&2, Prop::F64(12.34)),
        (&2, Prop::Str("blerg".into()))
    ]
)
```
