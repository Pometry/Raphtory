pub mod accumulator_id;
pub mod agg;
pub mod compute_state;
pub mod container;
pub mod morcel_state;
pub mod shuffle_state;

pub trait StateType: PartialEq + Clone + std::fmt::Debug + Send + Sync + 'static {}

impl<T: PartialEq + Clone + std::fmt::Debug + Send + Sync + 'static> StateType for T {}

#[cfg(test)]
mod state_test {
    use itertools::Itertools;
    use quickcheck_macros::quickcheck;
    use rand::Rng;

    use crate::{
        core::state::{
            accumulator_id::accumulators, compute_state::ComputeStateVec, container::merge_2_vecs,
            morcel_state::MorcelComputeState, shuffle_state::ShuffleComputeState,
        },
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
    };

    #[quickcheck]
    fn check_merge_2_vecs(mut a: Vec<usize>, b: Vec<usize>) {
        let len_a = a.len();
        let len_b = b.len();

        merge_2_vecs(&mut a, &b, |ia, ib| *ia = usize::max(*ia, *ib));

        assert_eq!(a.len(), usize::max(len_a, len_b));

        for (i, expected) in a.iter().enumerate() {
            match (a.get(i), b.get(i)) {
                (Some(va), Some(vb)) => assert_eq!(*expected, usize::max(*va, *vb)),
                (Some(va), None) => assert_eq!(*expected, *va),
                (None, Some(vb)) => assert_eq!(*expected, *vb),
                (None, None) => assert!(false, "value should exist in either a or b"),
            }
        }
    }

    fn tiny_graph() -> Graph {
        let g = Graph::new();

        g.add_node(1, 1, NO_PROPS).unwrap();
        g.add_node(1, 2, NO_PROPS).unwrap();
        g.add_node(1, 3, NO_PROPS).unwrap();
        g
    }

    #[test]
    fn min_aggregates_for_3_keys() {
        let g = tiny_graph();

        let min = accumulators::min(0);

        let mut state_map: MorcelComputeState<ComputeStateVec> = MorcelComputeState::new(3);

        // create random vec of numbers
        let mut rng = rand::thread_rng();
        let mut vec = vec![];
        let mut actual_min = i32::MAX;
        for _ in 0..100 {
            let i = rng.gen_range(0..100);
            actual_min = actual_min.min(i);
            vec.push(i);
        }

        for a in vec {
            state_map.accumulate_into(0, 0, a, &min);
            state_map.accumulate_into(0, 1, a, &min);
            state_map.accumulate_into(0, 2, a, &min);
        }

        let mut actual = state_map.finalize(0, &min, &g).into_iter().collect_vec();
        actual.sort();
        assert_eq!(
            actual,
            vec![(0, actual_min), (1, actual_min), (2, actual_min),]
        );
    }

    #[test]
    fn avg_aggregates_for_3_keys() {
        let g = tiny_graph();

        let avg = accumulators::avg(0);

        let mut state_map: MorcelComputeState<ComputeStateVec> = MorcelComputeState::new(3);

        // create random vec of numbers
        let mut rng = rand::thread_rng();
        let mut vec = vec![];
        let mut sum = 0;
        for _ in 0..100 {
            let i = rng.gen_range(0..100);
            sum += i;
            vec.push(i);
        }

        for a in vec {
            state_map.accumulate_into(0, 0, a, &avg);
            state_map.accumulate_into(0, 1, a, &avg);
            state_map.accumulate_into(0, 2, a, &avg);
        }

        let actual_avg = sum / 100;
        let mut actual = state_map.finalize(0, &avg, &g).into_iter().collect_vec();
        actual.sort();
        assert_eq!(
            actual,
            vec![(0, actual_avg), (1, actual_avg), (2, actual_avg),]
        );
    }

    #[test]
    fn top3_aggregates_for_3_keys() {
        let g = tiny_graph();

        let top3 = accumulators::topk::<i32, 3>(0);

        let mut state_map: MorcelComputeState<ComputeStateVec> = MorcelComputeState::new(3);

        for a in 0..100 {
            state_map.accumulate_into(0, 0, a, &top3);
            state_map.accumulate_into(0, 1, a, &top3);
            state_map.accumulate_into(0, 2, a, &top3);
        }
        let expected = vec![99, 98, 97];

        let mut actual = state_map.finalize(0, &top3, &g).into_iter().collect_vec();

        actual.sort();

        assert_eq!(
            actual,
            vec![
                (0, expected.clone()),
                (1, expected.clone()),
                (2, expected.clone()),
            ]
        );
    }

    #[test]
    fn sum_aggregates_for_3_keys() {
        let g = tiny_graph();

        let sum = accumulators::sum(0);

        let mut state: MorcelComputeState<ComputeStateVec> = MorcelComputeState::new(3);

        // create random vec of numbers
        let mut rng = rand::thread_rng();
        let mut vec = vec![];
        let mut actual_sum = 0;
        for _ in 0..100 {
            let i = rng.gen_range(0..100);
            actual_sum += i;
            vec.push(i);
        }

        for a in vec {
            state.accumulate_into(0, 0, a, &sum);
            state.accumulate_into(0, 1, a, &sum);
            state.accumulate_into(0, 2, a, &sum);
        }

        let mut actual = state.finalize(0, &sum, &g).into_iter().collect_vec();
        actual.sort();
        assert_eq!(
            actual,
            vec![(0, actual_sum), (1, actual_sum), (2, actual_sum),]
        );
    }

    #[test]
    fn sum_aggregates_for_3_keys_2_parts() {
        let sum = accumulators::sum(0);

        let mut part1_state: ShuffleComputeState<ComputeStateVec> =
            ShuffleComputeState::new(3, 2, 2);
        let mut part2_state: ShuffleComputeState<ComputeStateVec> =
            ShuffleComputeState::new(3, 2, 2);

        // create random vec of numbers
        let mut rng = rand::thread_rng();
        let mut vec1 = vec![];
        let mut vec2 = vec![];
        let mut actual_sum_1 = 0;
        let mut actual_sum_2 = 0;
        for _ in 0..3 {
            // data for first partition
            let i = rng.gen_range(0..100);
            actual_sum_1 += i;
            vec1.push(i);

            // data for second partition
            let i = rng.gen_range(0..100);
            actual_sum_2 += i;
            vec2.push(i);
        }

        // 1 gets all the numbers
        // 2 gets the numbers from part1
        // 3 gets the numbers from part2
        for a in vec1 {
            part1_state.accumulate_into(0, 0, a, &sum);
            part1_state.accumulate_into(0, 1, a, &sum);
        }

        for a in vec2 {
            part2_state.accumulate_into(0, 0, a, &sum);
            part2_state.accumulate_into(0, 2, a, &sum);
        }

        let actual = part1_state.iter_out(0, sum).collect_vec();

        assert_eq!(actual, vec![(0, actual_sum_1), (1, actual_sum_1), (2, 0)]);

        let actual = part2_state.iter_out(0, sum).collect_vec();

        assert_eq!(actual, vec![(0, actual_sum_2), (1, 0), (2, actual_sum_2)]);

        ShuffleComputeState::merge_mut(&mut part1_state, &part2_state, sum, 0);

        let actual = part1_state.iter_out(0, sum).collect_vec();

        assert_eq!(
            actual,
            vec![
                (0, (actual_sum_1 + actual_sum_2)),
                (1, actual_sum_1),
                (2, actual_sum_2),
            ]
        );
    }

    #[test]
    fn min_sum_aggregates_for_3_keys_2_parts() {
        let g = tiny_graph();

        let sum = accumulators::sum(0);
        let min = accumulators::min(1);

        let mut part1_state: ShuffleComputeState<ComputeStateVec> =
            ShuffleComputeState::new(3, 2, 2);
        let mut part2_state: ShuffleComputeState<ComputeStateVec> =
            ShuffleComputeState::new(3, 2, 2);

        // create random vec of numbers
        let mut rng = rand::thread_rng();
        let mut vec1 = vec![];
        let mut vec2 = vec![];
        let mut actual_sum_1 = 0;
        let mut actual_sum_2 = 0;
        let mut actual_min_1 = 100;
        let mut actual_min_2 = 100;
        for _ in 0..100 {
            // data for first partition
            let i = rng.gen_range(0..100);
            actual_sum_1 += i;
            actual_min_1 = actual_min_1.min(i);
            vec1.push(i);

            // data for second partition
            let i = rng.gen_range(0..100);
            actual_sum_2 += i;
            actual_min_2 = actual_min_2.min(i);
            vec2.push(i);
        }

        // 1 gets all the numbers
        // 2 gets the numbers from part1
        // 3 gets the numbers from part2
        for a in vec1 {
            part1_state.accumulate_into(0, 0, a, &sum);
            part1_state.accumulate_into(0, 1, a, &sum);
            part1_state.accumulate_into(0, 0, a, &min);
            part1_state.accumulate_into(0, 1, a, &min);
        }

        for a in vec2 {
            part2_state.accumulate_into(0, 0, a, &sum);
            part2_state.accumulate_into(0, 2, a, &sum);
            part2_state.accumulate_into(0, 0, a, &min);
            part2_state.accumulate_into(0, 2, a, &min);
        }

        let mut actual = part1_state
            .clone()
            .finalize(&sum, 0, &g, |c| c)
            .into_iter()
            .collect_vec();

        actual.sort();

        assert_eq!(actual, vec![(0, actual_sum_1), (1, actual_sum_1), (2, 0),]);

        let mut actual = part1_state
            .clone()
            .finalize(&min, 0, &g, |c| c)
            .into_iter()
            .collect_vec();

        actual.sort();

        assert_eq!(
            actual,
            vec![(0, actual_min_1), (1, actual_min_1), (2, i32::MAX),]
        );

        let mut actual = part2_state
            .clone()
            .finalize(&sum, 0, &g, |c| c)
            .into_iter()
            .collect_vec();

        actual.sort();

        assert_eq!(actual, vec![(0, actual_sum_2), (1, 0), (2, actual_sum_2),]);

        let mut actual = part2_state
            .clone()
            .finalize(&min, 0, &g, |c| c)
            .into_iter()
            .collect_vec();

        actual.sort();

        assert_eq!(
            actual,
            vec![(0, actual_min_2), (1, i32::MAX), (2, actual_min_2),]
        );

        ShuffleComputeState::merge_mut(&mut part1_state, &part2_state, sum, 0);
        let mut actual = part1_state
            .clone()
            .finalize(&sum, 0, &g, |c| c)
            .into_iter()
            .collect_vec();

        actual.sort();

        assert_eq!(
            actual,
            vec![
                (0, (actual_sum_1 + actual_sum_2)),
                (1, actual_sum_1),
                (2, actual_sum_2),
            ]
        );

        ShuffleComputeState::merge_mut(&mut part1_state, &part2_state, min, 0);
        let mut actual = part1_state
            .clone()
            .finalize(&min, 0, &g, |c| c)
            .into_iter()
            .collect_vec();

        actual.sort();

        assert_eq!(
            actual,
            vec![
                (0, actual_min_1.min(actual_min_2)),
                (1, actual_min_1),
                (2, actual_min_2),
            ]
        );
    }
}
