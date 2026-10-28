#[cfg(test)]
mod test {
    use raphtory::{
        db::api::state::{AsOrderedNodeStateOps, NodeState, OrderedNodeStateOps},
        prelude::*,
    };

    #[test]
    fn float_state() {
        let g = Graph::new();
        g.add_node(0, 0, NO_PROPS, None, None).unwrap();
        let float_state = NodeState::new_from_values(g.clone(), [0.0f64]);
        let int_state = NodeState::new_from_values(g.clone(), [1i64]);
        let min_float = float_state.min_item().unwrap().1;
        let min_int = int_state.min_item().unwrap().1;
        assert_eq!(min_float, &0.0);
        assert_eq!(min_int, &1);
    }
}

#[cfg(test)]
mod index_subset_test {
    use proptest::prelude::*;
    use raphtory::{core::entities::VID, db::api::state::*};
    use std::{collections::BTreeSet, sync::Arc};
    use storage::state::StateIndex;

    fn sorted(keys: &[usize]) -> Index<VID> {
        let mut keys: Vec<VID> = keys.iter().map(|k| VID(*k)).collect();
        keys.sort_by_key(|k| k.0);
        keys.dedup();
        Index::Sorted {
            keys: keys.into(),
            exact: true,
        }
    }

    fn partial(keys: &[usize]) -> Index<VID> {
        keys.iter().map(|k| VID(*k)).collect()
    }

    proptest! {
        /// Every representation pairing must agree with `BTreeSet::is_subset`.
        #[test]
        fn agrees_with_a_set_reference(
            a in proptest::collection::vec(0usize..12, 0..8),
            b in proptest::collection::vec(0usize..12, 0..8),
        ) {
            let want = BTreeSet::from_iter(a.iter().copied())
                .is_subset(&BTreeSet::from_iter(b.iter().copied()));

            prop_assert_eq!(sorted(&a).is_subset(&sorted(&b)), want, "sorted/sorted");
            prop_assert_eq!(sorted(&a).is_subset(&partial(&b)), want, "sorted/partial");
            prop_assert_eq!(partial(&a).is_subset(&sorted(&b)), want, "partial/sorted");
            prop_assert_eq!(partial(&a).is_subset(&partial(&b)), want, "partial/partial");
        }
    }

    fn full(len: usize) -> Index<VID> {
        Index::Full(Arc::new(StateIndex::new([len], len as u32)))
    }

    #[test]
    fn full_is_only_contained_by_full() {
        assert!(full(4).is_subset(&full(4)));
        // conservatively false even though these do hold every key of a
        // 3-node graph: `Full` makes no claim about the other side's contents
        assert!(!full(3).is_subset(&sorted(&[0, 1, 2])));
        assert!(!full(3).is_subset(&partial(&[0, 1, 2])));
        // and `Full` contains everything
        assert!(sorted(&[0, 1]).is_subset(&full(4)));
        assert!(partial(&[0, 1]).is_subset(&full(4)));
    }
}

#[cfg(test)]
mod index_par_iter_test {
    use proptest::prelude::*;
    use raphtory::db::api::state::Index;
    use rayon::prelude::*;
    use std::sync::Arc;
    use storage::state::StateIndex;

    fn indexes(chunk_sizes: &[usize], max_page_len: u32) -> Vec<Index<usize>> {
        let full = Index::Full(Arc::new(StateIndex::new(
            chunk_sizes.iter().copied(),
            max_page_len,
        )));
        let keys: Vec<usize> = full.iter().collect();
        vec![
            full,
            Index::from_iter(keys.iter().rev().copied()),
            Index::from_sorted(keys, false),
        ]
    }

    proptest! {
        #[test]
        fn matches_sequential_iter(
            chunk_sizes in prop::collection::vec(0usize..8, 0..10),
            max_len in 1usize..4,
        ) {
            for index in indexes(&chunk_sizes, 8) {
                let expected: Vec<usize> = index.iter().collect();

                let par: Vec<usize> = index.clone().into_par_iter().collect();
                prop_assert_eq!(&par, &expected);

                let rev: Vec<usize> = index.clone().into_par_iter().rev().collect();
                prop_assert_eq!(rev, expected.iter().rev().copied().collect::<Vec<_>>());

                let enumerated: Vec<(usize, usize)> = index.par_iter().collect();
                prop_assert_eq!(enumerated, expected.iter().copied().enumerate().collect::<Vec<_>>());

                // force the producer to split into small pieces, drained from either end
                let split: Vec<usize> = index.clone().into_par_iter().with_max_len(max_len).collect();
                prop_assert_eq!(&split, &expected);
                let split_rev: Vec<usize> =
                    index.clone().into_par_iter().rev().with_max_len(max_len).collect();
                prop_assert_eq!(split_rev, expected.iter().rev().copied().collect::<Vec<_>>());
            }
        }
    }
}
