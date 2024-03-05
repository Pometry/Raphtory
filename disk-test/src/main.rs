use disk_faiss::merge_ondisk;
use faiss::{index_factory, read_index, write_index, Idx, Index, MetricType};
use std::fs;
use std::io::Error as IoError;
use std::io::ErrorKind::InvalidData;

struct FVecsContent {
    dimensions: u32,
    vectors: Vec<f32>,
}

fn read_fvecs(file_name: &str) -> Result<FVecsContent, std::io::Error> {
    let data = fs::read(file_name)?;
    let (dim_data, vector_data) = data.split_at(4);
    let dim = dim_data
        .try_into()
        .map_err(|e| IoError::new(InvalidData, e))?;
    let dimensions = u32::from_le_bytes(dim);
    let vectors: Vec<_> = vector_data
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
        .collect();

    Ok(FVecsContent {
        dimensions,
        vectors,
    })
}

fn tmpfile<S: AsRef<str>>(filename: S) -> String {
    return "/tmp/faiss-disk-test/".to_owned() + filename.as_ref();
}

fn main() {
    println!("Training index");
    let FVecsContent {
        dimensions,
        vectors,
    } = read_fvecs("resources/sift/sift_learn.fvecs").unwrap();
    // println!("dimensions -> {dimensions}");
    // let sample: Vec<_> = flattened_vectors.iter().take(8).collect();
    // println!("sample -> {sample:?}");
    let mut index = index_factory(dimensions, "IVF4096,Flat", MetricType::InnerProduct).unwrap();
    index.train(vectors.as_slice()).unwrap();

    let index = index.into_ivf_flat().unwrap();
    dbg!(&index.nlist());
    dbg!(&index.ntotal());
    write_index(&index, tmpfile("trained.index")).unwrap();

    println!("Splitting vectors into files");
    let vectors = read_fvecs("resources/sift/sift_base.fvecs")
        .unwrap()
        .vectors;

    let num_chunks = 4;
    let num_vectors = vectors.len() / dimensions as usize;
    let vectors_per_chunk = num_vectors / num_chunks + 1;
    let chunk_files: Vec<_> = (0..num_chunks)
        .map(|chunk_number| tmpfile(format!("block_{chunk_number}.index")))
        .collect();
    let chunk_files: Vec<_> = chunk_files.iter().map(|f| f.as_str()).collect();

    for ((chunk_number, chunk), filename) in vectors
        .chunks(vectors_per_chunk * dimensions as usize)
        .enumerate()
        .zip(chunk_files.iter())
    {
        let first_id = vectors_per_chunk * chunk_number;
        let ids_range = first_id..(first_id + chunk.len());
        let ids: Vec<_> = ids_range.map(|id| Idx::from(id as i64)).collect();
        let mut index = read_index(tmpfile("trained.index")).unwrap();
        index.add_with_ids(chunk, ids.as_slice()).unwrap();
        write_index(&index, filename).unwrap();
    }

    println!("merging indexes on disk");
    merge_ondisk(
        &tmpfile("trained.index"),
        chunk_files,
        &tmpfile("merged_index.ivfdata"),
        &tmpfile("populated.index"),
    );

    println!("using the ondisk index");
    let mut index = read_index(&tmpfile("populated.index")).unwrap();
    let queries = read_fvecs("resources/sift/sift_query.fvecs").unwrap();

    let result = index.search(&queries.vectors, 5).unwrap();
    println!("result: {result:?}");
}
