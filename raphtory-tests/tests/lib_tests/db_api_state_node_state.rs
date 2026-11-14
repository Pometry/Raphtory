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
    use rayon::prelude::*;
    use roaring::RoaringTreemap;
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

    fn roaring(keys: &[usize]) -> Index<VID> {
        Index::from_bitmap(
            Arc::new(keys.iter().map(|k| *k as u64).collect::<RoaringTreemap>()),
            true,
        )
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
            prop_assert_eq!(roaring(&a).is_subset(&roaring(&b)), want, "roaring/roaring");
            prop_assert_eq!(roaring(&a).is_subset(&sorted(&b)), want, "roaring/sorted");
            prop_assert_eq!(sorted(&a).is_subset(&roaring(&b)), want, "sorted/roaring");
            prop_assert_eq!(roaring(&a).is_subset(&partial(&b)), want, "roaring/partial");
            prop_assert_eq!(partial(&a).is_subset(&roaring(&b)), want, "partial/roaring");
        }

        #[test]
        fn roaring_owned_iteration_agrees_with_a_list(
            a in proptest::collection::vec(0usize..300_000, 0..60),
            steps in proptest::collection::vec((any::<bool>(), 0usize..8), 0..30),
        ) {
            let IndexIntoIter::Roaring(mut iter) = roaring(&a).into_iter() else {
                unreachable!("a bitmap index walks as a bitmap")
            };
            let mut reference = sorted(&a).iter().collect::<Vec<_>>().into_iter();
            for (from_back, n) in steps {
                let (got, want) = if from_back {
                    (iter.nth_back(n), reference.nth_back(n))
                } else {
                    (iter.nth(n), reference.nth(n))
                };
                prop_assert_eq!(got, want);
                prop_assert_eq!(iter.len(), reference.len());
                prop_assert_eq!(iter.clone().collect::<Vec<_>>(), reference.clone().collect::<Vec<_>>());
                prop_assert_eq!(iter.clone().rev().collect::<Vec<_>>(), reference.clone().rev().collect::<Vec<_>>());
            }
            prop_assert_eq!(iter.collect::<Vec<_>>(), reference.collect::<Vec<_>>());
        }

        /// A bitmap index must behave exactly like a sorted one
        #[test]
        fn roaring_agrees_with_sorted(
            a in proptest::collection::vec(0usize..40, 0..20),
            b in proptest::collection::vec(0usize..40, 0..20),
            probe in 0usize..45,
        ) {
            let (r, s) = (roaring(&a), sorted(&a));
            let keys = |index: &Index<VID>| index.iter().collect::<Vec<_>>();
            let key_set = |index: &Index<VID>| BTreeSet::from_iter(index.iter());

            prop_assert_eq!(r.len(), s.len());
            prop_assert_eq!(keys(&r), keys(&s));
            prop_assert_eq!(r.clone().into_iter().collect::<Vec<_>>(), keys(&s));
            prop_assert_eq!(r.clone().into_iter().nth(probe), s.clone().into_iter().nth(probe));
            prop_assert_eq!(r.par_iter().collect::<Vec<_>>(), s.par_iter().collect::<Vec<_>>());
            prop_assert_eq!(r.clone().into_par_iter().collect::<Vec<_>>(), keys(&s));
            prop_assert_eq!(r.index(&VID(probe)), s.index(&VID(probe)));
            prop_assert_eq!(r.value(probe), s.value(probe));
            prop_assert_eq!(r.contains(&VID(probe)), s.contains(&VID(probe)));
            prop_assert!(r.dynamically_exact());
            prop_assert!(!r.clone().into_inexact().dynamically_exact());

            for (other, name) in [(roaring(&b), "roaring"), (sorted(&b), "sorted"), (partial(&b), "partial"), (full(40), "full")] {
                prop_assert_eq!(key_set(&r.intersection(&other)), key_set(&s.intersection(&other)), "{} ∩ {}", "roaring", name);
                prop_assert_eq!(key_set(&other.intersection(&r)), key_set(&other.intersection(&s)), "{} ∩ {}", name, "roaring");
                prop_assert_eq!(key_set(&r.union(&other)), key_set(&s.union(&other)), "{} ∪ {}", "roaring", name);
                prop_assert_eq!(key_set(&other.union(&r)), key_set(&other.union(&s)), "{} ∪ {}", name, "roaring");
                prop_assert_eq!(r.intersection(&other).dynamically_exact(), s.intersection(&other).dynamically_exact(), "exactness of roaring ∩ {}", name);
                prop_assert_eq!(r.union(&other).dynamically_exact(), s.union(&other).dynamically_exact(), "exactness of roaring ∪ {}", name);
            }
        }
    }

    fn full(len: usize) -> Index<VID> {
        Index::Full(Arc::new(StateIndex::new([len], len as u32)))
    }

    /// Spans several bitmap containers, with gaps so positions and keys differ.
    #[test]
    fn roaring_parallel_iteration_keeps_order_across_splits() {
        let keys: Vec<usize> = (0..300_000).filter(|k| k % 3 != 1).collect();
        let expected: Vec<VID> = keys.iter().map(|k| VID(*k)).collect();
        let index = roaring(&keys);

        assert_eq!(index.clone().into_par_iter().collect::<Vec<_>>(), expected);
        assert_eq!(
            index.par_iter().collect::<Vec<_>>(),
            expected.iter().copied().enumerate().collect::<Vec<_>>()
        );
        // tiny pieces, so nearly every key sits next to a split
        assert_eq!(
            index
                .clone()
                .into_par_iter()
                .with_max_len(7)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            index
                .clone()
                .into_par_iter()
                .rev()
                .with_max_len(7)
                .collect::<Vec<_>>(),
            expected.iter().rev().copied().collect::<Vec<_>>()
        );
        // indexed operations, which the parquet encoder relies on
        assert_eq!(
            index.into_par_iter().chunks(1_000).collect::<Vec<_>>(),
            expected
                .chunks(1_000)
                .map(<[VID]>::to_vec)
                .collect::<Vec<_>>()
        );
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
    use roaring::RoaringTreemap;
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
            Index::from_bitmap(
                Arc::new(keys.iter().map(|k| *k as u64).collect::<RoaringTreemap>()),
                false,
            ),
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
