use raphtory::{
    core::state::{
        accumulator_id::accumulators::sum,
        compute_state::{ComputeState, ComputeStateVec},
    },
    db::{
        api::view::{GraphViewOps, NodeViewOps, StaticGraphViewOps},
        task::{
            context::Context,
            edge::eval_edge::EvalEdgeView,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
    prelude::{EdgeViewOps, LayerOps, PropUnwrap, TimeOps},
};

fn get_one_hop_counts<'graph, G: GraphViewOps<'graph>>(
    evv: &EvalNodeView<'graph, '_, G, ()>,
    no_time: bool,
) -> usize {
    evv.layers("Netflow")
        .unwrap()
        .in_edges()
        .explode()
        .iter()
        .map(|nf_e_edge_expl| one_path_algorithm(nf_e_edge_expl, no_time))
        .sum::<usize>()
}

fn one_path_algorithm<
    'graph,
    G: GraphViewOps<'graph>,
    GH: GraphViewOps<'graph>,
    CS: ComputeState,
>(
    nf_e_edge_expl: EvalEdgeView<'graph, '_, G, GH, CS, ()>,
    no_time: bool,
) -> usize {
    //     MATCH
    //   (E)<-[nf1:Netflow]-(B)<-[login1:Events2v]-(A), (B)<-[prog1:Events1v]-(B)
    // WHERE A <> B AND B <> E AND A <> E
    //   AND login1.eventID = 4624
    //   AND prog1.eventID = 4688
    //   AND nf1.dstBytes > 100000000
    //   // time constraints within each path
    //   AND login1.epochtime < prog1.epochtime
    //   AND prog1.epochtime < nf1.epochtime
    //   AND nf1.epochtime - login1.epochtime <= 30
    // RETURN count(*)

    let a_id = nf_e_edge_expl.src().id();
    let b_id = nf_e_edge_expl.dst().id();
    // First we remove any A->A edges, no cyclces allowed
    if a_id == b_id {
        return 0usize;
    }
    // for the netflow B we now look for all E edges that have the dstBytes Prop
    let dst_bytes_val = nf_e_edge_expl
        .properties()
        .get("dstBytes")
        .into_i64()
        .unwrap_or(0);
    // For the nf1 we filter any edges from B that do not have the byte size (<=1e8)
    // we only watch B->E edges that are >1e8
    if dst_bytes_val <= 100000000 {
        return 0usize;
    }

    // Now we save the time of nf1
    let nf1_time = nf_e_edge_expl.time().unwrap_or_default();
    let mut time_bound = nf1_time.saturating_sub(30);
    if no_time {
        time_bound = 0;
    }

    // Find all the login events satisfying the time constraint and count the program starts that fall in the window
    let event_count = nf_e_edge_expl
        .src()
        .window(time_bound, nf1_time)
        .layers("Events2v4624")
        .into_iter()
        .flat_map(|v| {
            v.in_edges()
                .iter()
                .filter(|e| e.src().id() != a_id && e.src().id() != b_id)
                .flat_map(|e| e.explode())
        })
        .flat_map(|login_exp| {
            login_exp
                .dst()
                .window(login_exp.time().unwrap().saturating_add(1), nf1_time)
                .layers("Events1v4688")
        })
        .flat_map(|v| {
            v.out_edges()
                .iter()
                .filter(|e| e.src().id() == e.dst().id())
                .flat_map(|e| e.explode())
        })
        .count();
    event_count
}

pub fn netflow_one_path_node<G: StaticGraphViewOps>(
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
    use super::*;
    use raphtory::{
        core::Prop,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
    };

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
        let actual = netflow_one_path_node(&graph, true, None);
        assert_eq!(actual, 1);
    }
}
