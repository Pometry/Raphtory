use crate::{
    core::state::{
        accumulator_id::accumulators::sum,
        compute_state::{ComputeState, ComputeStateVec},
    },
    db::{
        api::view::{GraphViewOps, VertexViewOps},
        graph::{edge::EdgeView, views::layer_graph::LayeredGraph},
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
            vertex::eval_vertex::EvalVertexView,
        },
    },
    prelude::{EdgeListOps, EdgeViewOps, LayerOps, Prop, PropUnwrap, TimeOps},
};

fn get_one_hop_counts<G: GraphViewOps, CS: ComputeState>(
    evv: &EvalVertexView<G, CS, ()>,
    no_time: bool,
) -> usize {
    let vid = evv.id();
    evv.graph
        .vertex(vid)
        .unwrap()
        .layer("Netflow")
        .unwrap()
        .in_edges()
        .explode()
        .map(|nf_e_edge_expl| one_path_algorithm(evv, nf_e_edge_expl, no_time))
        .sum::<usize>()
}

fn one_path_algorithm<G: GraphViewOps, CS: ComputeState>(
    evv: &EvalVertexView<G, CS, ()>,
    nf_e_edge_expl: EdgeView<LayeredGraph<G>>,
    no_time: bool,
) -> usize {
    if nf_e_edge_expl.src() == nf_e_edge_expl.dst() {
        return 0usize;
    }
    let dstBytesVal = nf_e_edge_expl
        .properties()
        .get("dstBytes")
        .unwrap_or(Prop::I64(0));

    if dstBytesVal <= Prop::I64(100000000) {
        return 0usize;
    }

    let nf1_time = nf_e_edge_expl.time().unwrap_or_default();
    let mut time_bound = nf1_time.saturating_sub(30);
    if no_time {
        time_bound = 0;
    }

    let b_edges = match evv.graph.layer("Events1v4688").and_then(|layer| {
        layer
            .window(time_bound, nf1_time)
            .edge(nf_e_edge_expl.src(), nf_e_edge_expl.src())
    }) {
        Some(edges) => edges,
        None => return 0,
    };

    b_edges
        .explode()
        .map(|prog1_b_edge| {
            let prog1_time = prog1_b_edge.time().unwrap_or_default();
            let b_layer = match evv
                .graph
                .vertex(prog1_b_edge.src())
                .and_then(|vertex| vertex.layer("Events2v4624"))
            {
                Some(layer) => layer,
                None => return 0,
            };

            b_layer
                .window(time_bound, prog1_time)
                .in_edges()
                .explode()
                .filter(|edge| edge.src() != nf_e_edge_expl.src())
                .count()
        })
        .sum()
}

pub fn netflow_one_path_vertex<G: GraphViewOps>(
    g: &G,
    no_time: bool,
    threads: Option<usize>,
) -> usize {
    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let total_value = sum::<usize>(0);
    ctx.global_agg(total_value);

    let step1 = ATask::new(move |evv| {
        let one_hop_counts = get_one_hop_counts(evv, no_time);
        evv.global_update(&total_value, one_hop_counts);
        Step::Done
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);
    runner.run(
        vec![],
        vec![Job::new(step1)],
        None,
        |egs, _, _, _| egs.finalize(&total_value),
        threads,
        1,
        None,
        None,
    )
}

#[cfg(test)]
mod one_path_test {
    use crate::{
        algorithms::netflow_one_path_vertex::netflow_one_path_vertex,
        core::Prop,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
    };
    use pretty_assertions::assert_eq;

    #[test]
    fn test_one_path() {
        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, NO_PROPS, Some("Events2v4624"))
            .expect("Panic");
        graph
            .add_edge(1, 2, 2, NO_PROPS, Some("Events1v4688"))
            .expect("Panic");
        graph
            .add_edge(
                2,
                2,
                3,
                [("dstBytes", Prop::I64(100_000_005))],
                Some("Netflow"),
            )
            .expect("Panic");
        // graph
        //     .add_edge(
        //         2,
        //         4,
        //         5,
        //         [("dstBytes", Prop::U64(100000005))],
        //         Some("Netflow"),
        //     )
        //     .expect("Panic");
        let actual = netflow_one_path_vertex(&graph, true, None);
        assert_eq!(actual, 1);
    }
}
