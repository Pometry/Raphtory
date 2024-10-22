use rayon::prelude::*;
/// Compute cumulative sum in parallel over `num_chunks` chunks
pub fn par_cum_sum(values: &mut [usize]) {
    let num_chunks = rayon::current_num_threads();
    let chunk_size = values.len().div_ceil(num_chunks);
    let mut chunk_sums = Vec::with_capacity(num_chunks);
    values
        .par_chunks_mut(chunk_size)
        .map(|chunk| {
            let mut cum_sum = 0;
            for v in chunk {
                *v += cum_sum;
                cum_sum = *v;
            }
            cum_sum
        })
        .collect_into_vec(&mut chunk_sums);

    let mut cum_sum = 0;
    for (partial_sum, next_chunk) in chunk_sums
        .into_iter()
        .zip(values.chunks_mut(chunk_size).skip(1))
    {
        cum_sum += partial_sum;
        next_chunk.par_iter_mut().for_each(|v| *v += cum_sum);
    }
}