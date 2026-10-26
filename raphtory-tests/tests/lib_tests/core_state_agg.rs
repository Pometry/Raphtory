#[cfg(test)]
mod agg_test {

    #[test]
    fn avg_def() {
        use raphtory::core::state::agg::{
            topk::{TopK, TopKHeap},
            Accumulator, AvgDef, MaxDef, MinDef, SumDef,
        };

        let mut avg = AvgDef::<i32>::zero();
        let mut sum = SumDef::<i32>::zero();
        let mut min = MinDef::<i32>::zero();
        let mut max = MaxDef::<i32>::zero();
        let mut top3 = TopK::<i32, 3>::zero();
        let mut top5 = TopK::<i32, 5>::zero();
        // let mut arr = ArrConst::<u32, 5>::zero();

        for i in 0..100 {
            <AvgDef<i32> as Accumulator<(i32, usize), i32, i32>>::add0(&mut avg, i);
            <SumDef<i32> as Accumulator<i32, i32, i32>>::add0(&mut sum, i);
            <MinDef<i32> as Accumulator<i32, i32, i32>>::add0(&mut min, i);
            <MaxDef<i32> as Accumulator<i32, i32, i32>>::add0(&mut max, i);
            <TopK<i32, 3> as Accumulator<TopKHeap<i32>, i32, Vec<i32>>>::add0(&mut top3, i);
            <TopK<i32, 5> as Accumulator<TopKHeap<i32>, i32, Vec<i32>>>::add0(&mut top5, i);
        }

        assert_eq!(
            <AvgDef<i32> as Accumulator<(i32, usize), i32, i32>>::finish(&avg),
            49
        );
        assert_eq!(
            <SumDef<i32> as Accumulator<i32, i32, i32>>::finish(&sum),
            4950
        );
        assert_eq!(<MinDef<i32> as Accumulator<i32, i32, i32>>::finish(&min), 0);
        assert_eq!(
            <MaxDef<i32> as Accumulator<i32, i32, i32>>::finish(&max),
            99
        );
        assert_eq!(
            <TopK<i32, 3> as Accumulator<TopKHeap<i32>, i32, Vec<i32>>>::finish(&top3),
            vec![99, 98, 97]
        );
        assert_eq!(
            <TopK<i32, 5> as Accumulator<TopKHeap<i32>, i32, Vec<i32>>>::finish(&top5),
            vec![99, 98, 97, 96, 95]
        );
    }
}
