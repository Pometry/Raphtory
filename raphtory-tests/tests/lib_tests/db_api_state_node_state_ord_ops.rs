#[cfg(test)]
mod test {
    use raphtory::db::api::state::{par_top_k, top_k};

    use rand::{
        distr::{Distribution, Uniform},
        Rng,
    };
    use tokio::time::Instant;

    fn gen_x_ints(
        count: u32,
        distribution: impl Distribution<u32>,
        rng: &mut (impl Rng + ?Sized),
    ) -> Vec<u32> {
        let mut results = Vec::with_capacity(count as usize);
        let iter = distribution.sample_iter(rng);
        for (_, sample) in (0..count).zip(iter) {
            results.push(sample);
        }
        results
    }

    #[test]
    fn test_top_k() {
        let values = gen_x_ints(
            100_000_000,
            Uniform::new(0, 10000000).unwrap(),
            &mut rand::rng(),
        ); // [4i32, 2, 3, 100, 4, 2];
        let timer = Instant::now();
        let res1 = top_k(values.clone(), |a, b| a.cmp(b), 100);
        println!("Top K in: {:?}", timer.elapsed());
        let timer = Instant::now();
        let res2 = par_top_k(values, |a, b| a.cmp(b), 100);
        println!("Par Top K in: {:?}", timer.elapsed());
        assert_eq!(res1, res2);
        //assert_eq!(res, par_top_k(values, |a, b| a.cmp(b), 100))  //[100, 4, 4])
    }
}
