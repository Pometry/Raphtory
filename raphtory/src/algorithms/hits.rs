use crate::algorithms::*;
use crate::core::agg::*;
use crate::core::state::accumulator_id::accumulators::max;
use crate::core::state::accumulator_id::accumulators::sum;
use crate::core::state::accumulator_id::accumulators::val;
use crate::core::state::accumulator_id::AccId;
use crate::core::state::*;
use crate::db::graph::Graph;
use crate::db::program::*;
use crate::db::view_api::GraphViewOps;
use rustc_hash::FxHashMap;

struct HitsS0 {
    hub_score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
    auth_score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
}

impl HitsS0 {
    fn new() -> Self {
        Self {
            hub_score: val(0),
            auth_score: val(1),
        }
    }
}

impl Program for HitsS0 {
    type Out = ();

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let hub_score = c.agg(self.hub_score);
        let auth_score = c.agg(self.auth_score);
        c.step(|s| {
            s.update(&hub_score, MulF32::zero());
            s.update(&auth_score, MulF32::zero())
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        c.step(|_| true)
    }

    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
    }
}

struct HitsS1 {
    hub_score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
    auth_score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
    recv_hub_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    recv_auth_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
}

impl HitsS1 {
    fn new() -> Self {
        Self {
            hub_score: val(0),
            auth_score: val(1),
            recv_hub_score: sum(2),
            recv_auth_score: sum(3),
        }
    }
}

impl Program for HitsS1 {
    type Out = ();

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let hub_score = c.agg(self.hub_score);
        let auth_score = c.agg(self.auth_score);
        let recv_hub_score = c.agg(self.recv_hub_score);
        let recv_auth_score = c.agg(self.recv_auth_score);

        c.step(|s| {
            let hub_score = s.read(&hub_score).0;
            let auth_score = s.read(&auth_score).0;
            for t in s.neighbours_out() {
                t.update(&recv_hub_score, SumF32(hub_score))
            }
            for t in s.neighbours_in() {
                t.update(&recv_auth_score, SumF32(auth_score))
            }
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.agg(self.recv_hub_score);
        let _ = c.agg(self.recv_auth_score);
        c.step(|_| true)
    }

    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
    }
}

struct HitsS2 {
    recv_hub_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    recv_auth_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    total_hub_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    total_auth_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
}

impl HitsS2 {
    fn new() -> Self {
        Self {
            recv_hub_score: sum(2),
            recv_auth_score: sum(3),
            total_hub_score: sum(4),
            total_auth_score: sum(5),
        }
    }
}

impl Program for HitsS2 {
    type Out = ();

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let recv_hub_score = c.agg(self.recv_hub_score);
        let recv_auth_score = c.agg(self.recv_auth_score);
        let total_hub_score = c.global_agg(self.total_hub_score);
        let total_auth_score = c.global_agg(self.total_auth_score);

        c.step(|s| {
            let recv_hub_score = s.read(&recv_hub_score).0;
            let recv_auth_score = s.read(&recv_auth_score).0;

            s.global_update(&total_hub_score, SumF32(recv_hub_score));
            s.global_update(&total_auth_score, SumF32(recv_auth_score));
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.global_agg(self.total_hub_score);
        let _ = c.global_agg(self.total_auth_score);
        c.step(|_| true)
    }

    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
    }
}

struct HitsS3 {
    hub_score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
    auth_score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
    recv_hub_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    recv_auth_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    total_hub_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    total_auth_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    max_diff_hub_score: AccId<f32, f32, f32, MaxDef<f32>>,
    max_diff_auth_score: AccId<f32, f32, f32, MaxDef<f32>>,
}

impl HitsS3 {
    fn new() -> Self {
        Self {
            hub_score: val(0),
            auth_score: val(1),
            recv_hub_score: sum(2),
            recv_auth_score: sum(3),
            total_hub_score: sum(4),
            total_auth_score: sum(5),
            max_diff_hub_score: max(6),
            max_diff_auth_score: max(7),
        }
    }
}

impl Program for HitsS3 {
    type Out = ();

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let hub_score = c.agg(self.hub_score);
        let auth_score = c.agg(self.auth_score);
        let recv_hub_score = c.agg(self.recv_hub_score);
        let recv_auth_score = c.agg(self.recv_auth_score);
        let total_hub_score = c.global_agg(self.total_hub_score);
        let total_auth_score = c.global_agg(self.total_auth_score);
        let max_diff_hub_score = c.global_agg(self.max_diff_hub_score);
        let max_diff_auth_score = c.global_agg(self.max_diff_auth_score);

        c.step(|s| {
            let recv_hub_score = s.read(&recv_hub_score).0;
            let recv_auth_score = s.read(&recv_auth_score).0;
            println!("s = {}, recv_hub_score = {}", s.global_id(), recv_hub_score);
            println!(
                "s = {}, recv_auth_score = {}",
                s.global_id(),
                recv_auth_score
            );

            s.update(
                &auth_score,
                MulF32(recv_hub_score / s.read_global(&total_hub_score).0),
            );
            s.update(
                &hub_score,
                MulF32(recv_auth_score / s.read_global(&total_auth_score).0),
            );

            let prev_hub_score = s.read_prev(&hub_score);
            let curr_hub_score = s.read(&hub_score);
            let md_hub_score = abs((prev_hub_score - curr_hub_score).0);
            s.global_update(&max_diff_hub_score, md_hub_score);

            let prev_auth_score = s.read_prev(&auth_score);
            let curr_auth_score = s.read(&auth_score);
            let md_auth_score = abs((prev_auth_score - curr_auth_score).0);
            s.global_update(&max_diff_auth_score, md_auth_score);
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.agg_reset(self.recv_hub_score);
        let _ = c.agg_reset(self.recv_auth_score);
        let _ = c.global_agg_reset(self.total_hub_score);
        let _ = c.global_agg_reset(self.total_auth_score);
        let _ = c.global_agg_reset(self.max_diff_hub_score);
        let _ = c.global_agg_reset(self.max_diff_auth_score);
        c.step(|_| true)
    }

    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
    }
}

// HITS (Hubs and Authority) Algorithm:
// AuthScore of a vertex (A) = Sum of HubScore of all vertices pointing at vertex (A) from previous iteration /
//     Sum of HubScore of all vertices in the current iteration
//
// HubScore of a vertex (A) = Sum of AuthScore of all vertices pointing away from vertex (A) from previous iteration /
//     Sum of AuthScore of all vertices in the current iteration

#[allow(unused_variables)]
pub fn hits(g: &Graph, window: Range<i64>, iter_count: usize) -> FxHashMap<u64, (f32, f32)> {
    let mut c = GlobalEvalState::new(g.clone(), true);
    let hits_s0 = HitsS0::new();
    let hits_s1 = HitsS1::new();
    let hits_s2 = HitsS2::new();
    let hits_s3 = HitsS3::new();

    hits_s0.run_step(g, &mut c);

    let max_diff_hub_score = 0.01f32;
    let max_diff_auth_score = max_diff_hub_score;
    let mut i = 0;

    loop {
        hits_s1.run_step(g, &mut c);
        println!("step1, hub: {:?}", c.read_vec_partitions(&val::<MulF32>(0)));
        println!(
            "step1, auth: {:?}",
            c.read_vec_partitions(&val::<MulF32>(1))
        );

        hits_s2.run_step(g, &mut c);
        hits_s3.run_step(g, &mut c);
        println!(
            "total_hub = {:?}",
            c.read_global_state(&sum::<SumF32>(4)).unwrap()
        );
        println!(
            "total_auth = {:?}",
            c.read_global_state(&sum::<SumF32>(5)).unwrap()
        );

        let r1 = c.read_global_state(&max::<f32>(6)).unwrap();
        println!("max_diff_hub = {:?}", r1);
        let r2 = c.read_global_state(&max::<f32>(7)).unwrap();
        println!("max_diff_auth = {:?}", r2);

        if (r1 <= max_diff_hub_score && r2 <= max_diff_auth_score) || i > iter_count {
            break;
        }

        if c.keep_past_state {
            c.ss += 1;
        }
        i += 1;
    }

    println!("i = {}", i);

    let mut results: FxHashMap<u64, (f32, f32)> = FxHashMap::default();

    (0..g.nr_shards)
        .into_iter()
        .fold(&mut results, |res, part_id| {
            let r = c.fold_state(&val::<MulF32>(0), part_id, res, |res, v_id, sc| {
                res.insert(*v_id, (sc.0, 0.0));
                res
            });
            c.fold_state(&val::<MulF32>(1), part_id, r, |res, v_id, sc| {
                let (a, _) = res.get(v_id).unwrap();
                res.insert(*v_id, (*a, sc.0));
                res
            })
        });

    results
}

#[cfg(test)]
mod hits_tests {
    use super::*;

    fn load_graph(n_shards: usize, edges: Vec<(u64, u64)>) -> Graph {
        let graph = Graph::new(n_shards);

        for (src, dst) in edges {
            graph.add_edge(0, src, dst, &vec![], None).unwrap();
        }
        graph
    }

    fn test_hits(n_shards: usize) {
        let graph = load_graph(
            n_shards,
            vec![
                (1, 4),
                (2, 3),
                (2, 5),
                (3, 1),
                (4, 2),
                (4, 3),
                (5, 2),
                (5, 3),
                (5, 4),
                (5, 6),
                (6, 3),
                (6, 8),
                (7, 1),
                (7, 3),
                (8, 1),
            ],
        );

        let window = 0..10;

        let results: FxHashMap<u64, (f32, f32)> = hits(&graph, window, 20).into_iter().collect();

        // NetworkX results
        // >>> G = nx.DiGraph()
        // >>> G.add_edge(1, 4)
        // >>> G.add_edge(2, 3)
        // >>> G.add_edge(2, 5)
        // >>> G.add_edge(3, 1)
        // >>> G.add_edge(4, 2)
        // >>> G.add_edge(4, 3)
        // >>> G.add_edge(5, 2)
        // >>> G.add_edge(5, 3)
        // >>> G.add_edge(5, 4)
        // >>> G.add_edge(5, 6)
        // >>> G.add_edge(6,3)
        // >>> G.add_edge(6,8)
        // >>> G.add_edge(7,1)
        // >>> G.add_edge(7,3)
        // >>> G.add_edge(8,1)
        // >>> nx.hits(G)
        // (
        //     (1, (0.04305010876408988, 0.08751958702900825)),
        //     (2, (0.14444089276992705, 0.18704574169397797)),
        //     (3, (0.02950848945012511, 0.3690360954887363)),
        //     (4, (0.1874910015340169, 0.12768284011810235)),
        //     (5, (0.26762580040598083, 0.05936290157587567)),
        //     (6, (0.144440892769927, 0.10998993251842377)),
        //     (7, (0.15393432485580819, 5.645162243895331e-17))
        //     (8, (0.02950848945012511, 0.05936290157587556)),
        // )

        assert_eq!(
            results,
            vec![
                (7, (0.15471625, 0.0)),
                (4, (0.18654142, 0.12442485)),
                (1, (0.043136504, 0.096625775)),
                (8, (0.030866565, 0.05943252)),
                (5, (0.26667947, 0.05943252)),
                (2, (0.14359663, 0.18366565)),
                (6, (0.14359663, 0.10755368)),
                (3, (0.030866565, 0.36886504))
            ]
            .into_iter()
            .collect::<FxHashMap<u64, (f32, f32)>>()
        );
    }

    #[test]
    fn test_hits_1() {
        test_hits(1);
    }
}
