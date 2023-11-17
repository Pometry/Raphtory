use std::{
    io::BufReader,
    path::{Path, PathBuf},
};

use arrow2::{
    array::{Array, MutableUtf8Array, PrimitiveArray, Utf8Array},
    chunk::Chunk,
    compute::{
        merge_sort,
        sort::{self, SortColumn, SortOptions},
    },
    datatypes::{Field, Schema},
    io::parquet::read,
};
use itertools::Itertools;
use rayon::prelude::*;

use crate::arrow::{
    array_as_id_iter,
    mmap::{mmap_batch, write_batches},
};

use super::{global_order::GlobalOrder, Error, GraphChunk};

#[derive(Debug)]
pub struct ExternalEdgeList<'a, P: AsRef<Path>> {
    pub layer: &'a str,
    pub path: P,
    pub src_col: &'a str,
    pub src_hash_col: &'a str,
    pub dst_col: &'a str,
    pub dst_hash_col: &'a str,
    pub time_col: &'a str,
    pub parquet_files: Vec<PathBuf>,
}

impl<'a, P: AsRef<Path>> ExternalEdgeList<'a, P> {
    pub fn new(
        layer: &'a str,
        path: P,
        src_col: &'a str,
        src_hash_col: &'a str,
        dst_col: &'a str,
        dst_hash_col: &'a str,
        time_col: &'a str,
    ) -> Result<Self, Error> {
        let parquet_files = if std::fs::read_dir(&path).is_ok() {
            std::fs::read_dir(&path)?
                .map(|res| res.unwrap())
                .filter(|e| {
                    e.path()
                        .extension()
                        .filter(|ext| ext == &"parquet")
                        .is_some()
                })
                .sorted_by(|f1, f2| f1.path().cmp(&f2.path()))
                .map(|e| e.path())
                .collect_vec()
        } else {
            vec![path.as_ref().to_path_buf()]
        };

        Ok(Self {
            layer,
            path,
            src_col,
            src_hash_col,
            dst_col,
            dst_hash_col,
            time_col,
            parquet_files,
        })
    }

    pub(crate) fn layer(&self) -> &'a str {
        self.layer
    }

    pub(crate) fn par_sorted_vertices(
        &self,
    ) -> impl ParallelIterator<Item = (Box<dyn Array>, Box<dyn Array>)> + '_ {
        self.parquet_files
            .par_iter()
            .map(|path| {
                sort_within_chunks(
                    path,
                    self.src_col,
                    self.src_hash_col,
                    self.dst_col,
                    self.dst_hash_col,
                )
            })
            .flatten()
    }

    pub(crate) fn load_chunks(&self) -> impl Iterator<Item = GraphChunk> + '_ {
        self.parquet_files.iter().flat_map(|path| {
            read_file_chunks(path, self.src_col, self.dst_col)
                .expect("failed to load chunks from path")
        })
    }

    pub(crate) fn files(&self) -> &[PathBuf] {
        &self.parquet_files
    }
}

pub(crate) fn sort_within_chunks<P: AsRef<Path>>(
    parquet_file: P,
    src_col: &str,
    src_hash_col: &str,
    dst_col: &str,
    dst_hash_col: &str,
) -> Result<(Box<dyn Array>, Box<dyn Array>), Error> {
    println!("pre sort reading file: {:?}", parquet_file.as_ref());
    let file = std::fs::File::open(&parquet_file)?;
    let mut reader = BufReader::new(file);
    let metadata = read::read_metadata(&mut reader)?;
    let schema = read::infer_schema(&metadata)?;

    let schema = schema.filter(|_, field| {
        field.name == src_col
            || field.name == dst_col
            || field.name == src_hash_col
            || field.name == dst_hash_col
    });

    let src_idx = schema
        .fields
        .iter()
        .position(|f| f.name == src_col)
        .unwrap();
    let dst_idx = schema
        .fields
        .iter()
        .position(|f| f.name == dst_col)
        .unwrap();
    let src_hash_idx = schema
        .fields
        .iter()
        .position(|f| f.name == src_hash_col)
        .unwrap();
    let dst_hash_idx = schema
        .fields
        .iter()
        .position(|f| f.name == dst_hash_col)
        .unwrap();

    let reader = read::FileReader::new(reader, metadata.row_groups, schema, None, None, None);
    let sorted_vertices = reader
        .flatten()
        .par_bridge()
        .map(|chunk| {
            sort_dedup_2(
                (&chunk[src_hash_idx], &chunk[src_idx]),
                (&chunk[dst_hash_idx], &chunk[dst_idx]),
            )
        })
        .reduce_with(|l, r| {
            l.and_then(|(l_hash, l)| {
                r.and_then(|(r_hash, r)| sort_dedup_2((&r_hash, &r), (&l_hash, &l)))
            })
        })
        .unwrap();

    if sorted_vertices.is_err() {
        println!("error reading file: {:?}", sorted_vertices);
    }

    sorted_vertices
}

// sort by 2 columns
pub(crate) fn sort_dedup_2(
    lhs: (&Box<dyn Array>, &Box<dyn Array>),
    rhs: (&Box<dyn Array>, &Box<dyn Array>),
) -> Result<(Box<dyn Array>, Box<dyn Array>), Error> {
    let (hash_rhs, rhs) = rhs;
    let (hash_lhs, lhs) = lhs;

    let options = SortOptions::default();

    let rhs_sorted = sort::lexsort::<i32>(
        &vec![
            SortColumn {
                values: hash_rhs.as_ref(),
                options: None,
            },
            SortColumn {
                values: rhs.as_ref(),
                options: None,
            },
        ],
        None,
    )?;

    let lhs_sorted = sort::lexsort::<i32>(
        &vec![
            SortColumn {
                values: hash_lhs.as_ref(),
                options: None,
            },
            SortColumn {
                values: lhs.as_ref(),
                options: None,
            },
        ],
        None,
    )?;

    let rhs_hash = rhs_sorted[0].as_ref();
    let lhs_hash = lhs_sorted[0].as_ref();

    let rhs_vertices = rhs_sorted[1].as_ref();
    let lhs_vertices = lhs_sorted[1].as_ref();

    let hashes = vec![rhs_hash.as_ref(), lhs_hash.as_ref()];
    let vertices = vec![rhs_vertices.as_ref(), lhs_vertices.as_ref()];
    let pairs = vec![(hashes.as_ref(), &options), (vertices.as_ref(), &options)];

    let slices = merge_sort::slices(pairs.as_ref())?;

    let new_hashes = merge_sort::take_arrays(&[rhs_hash, lhs_hash], slices.iter().copied(), None);
    let merged =
        merge_sort::take_arrays(&[rhs_vertices, lhs_vertices], slices.iter().copied(), None);

    if let Some(hash_arr) = new_hashes.as_any().downcast_ref::<PrimitiveArray<i64>>() {
        let mut deduped_hash: Vec<i64> = Vec::with_capacity(hash_arr.len());

        if let Some(arr) = merged.as_any().downcast_ref::<PrimitiveArray<u64>>() {
            let mut deduped: Vec<u64> = Vec::with_capacity(arr.len());
            for (h, v) in hash_arr
                .into_iter()
                .flatten()
                .zip(arr.into_iter().flatten())
                .dedup()
            {
                deduped_hash.push(*h);
                deduped.push(*v);
            }
            let arr = PrimitiveArray::from_vec(deduped);
            let next_hash = PrimitiveArray::from_vec(deduped_hash);
            Ok((next_hash.boxed(), arr.to_boxed()))
        } else if let Some(arr) = merged.as_any().downcast_ref::<Utf8Array<i64>>() {
            let mut deduped = MutableUtf8Array::<i64>::new();
            for (h, v) in hash_arr
                .into_iter()
                .flatten()
                .zip(arr.into_iter().flatten())
                .dedup()
            {
                deduped_hash.push(*h);
                deduped.push(Some(v));
            }
            let next_hash = PrimitiveArray::from_vec(deduped_hash);
            let arr: Utf8Array<i64> = deduped.into();
            Ok((next_hash.boxed(), arr.to_boxed()))
        } else if let Some(arr) = merged.as_any().downcast_ref::<Utf8Array<i32>>() {
            let mut deduped = MutableUtf8Array::<i32>::new();
            for (h, v) in hash_arr
                .into_iter()
                .flatten()
                .zip(arr.into_iter().flatten())
                .dedup()
            {
                deduped_hash.push(*h);
                deduped.push(Some(v));
            }
            let next_hash = PrimitiveArray::from_vec(deduped_hash);
            let arr: Utf8Array<i32> = deduped.into();
            Ok((next_hash.boxed(), arr.to_boxed()))
        } else {
            Err(Error::InvalidTypeColumn(format!(
                "src or dst column need to be u64 or string, found: {:?}",
                merged.data_type()
            )))
        }
    } else {
        Err(Error::InvalidTypeColumn(format!(
            "hash column need to be i64 found: {:?}",
            new_hashes.data_type()
        )))
    }
}

pub(crate) fn read_file_chunks<P: AsRef<Path>>(
    parquet_file: P,
    src_col: &str,
    dst_col: &str,
) -> Result<impl Iterator<Item = GraphChunk>, Error> {
    println!("reading file: {:?}", parquet_file.as_ref());

    let file = std::fs::File::open(&parquet_file)?;
    let mut reader = BufReader::new(file);
    let metadata = read::read_metadata(&mut reader)?;
    let schema = read::infer_schema(&metadata)?;
    let src_col_field = schema
        .fields
        .iter()
        .find(|field| &field.name == src_col)
        .ok_or_else(|| Error::ColumnNotFound(src_col.to_owned()))?;
    let dst_col_field = schema
        .fields
        .iter()
        .find(|field| &field.name == dst_col)
        .ok_or_else(|| Error::ColumnNotFound(dst_col.to_owned()))?;

    let schema = Schema::from(vec![src_col_field.clone(), dst_col_field.clone()])
        .with_metadata(schema.metadata);

    let reader = read::FileReader::new(
        reader,
        metadata.row_groups,
        schema.clone(),
        None,
        None,
        None,
    );
    Ok(reader
        .flatten()
        .map(|chunk| GraphChunk::from_chunk(chunk, 0, 1)))
}

pub(crate) fn make_global_ordering<'a, GO: GlobalOrder, P: AsRef<Path> + Sync + Send>(
    sorted_gids_path: impl AsRef<Path>,
    edge_lists: &[ExternalEdgeList<'a, P>],
    go: &mut GO,
) -> Result<(), Error> {
    let now = std::time::Instant::now();

    let sorted_vertices = if sorted_gids_path.as_ref().exists() {
        let chunk = unsafe { mmap_batch(sorted_gids_path.as_ref(), 0)? };
        chunk.into_arrays().into_iter().next().unwrap()
    } else {
        let (_, sorted_vertices) = edge_lists
            .par_iter()
            .flat_map(|e_list| e_list.par_sorted_vertices())
            .reduce_with(|(l_hash, l), (r_hash, r)| {
                sort_dedup_2((&l_hash, &l), (&r_hash, &r)).unwrap()
            })
            .ok_or_else(|| Error::NoEdgeLists)?;

        let schema = Schema::from(vec![Field::new(
            "sorted_global_ids",
            sorted_vertices.data_type().clone(),
            false,
        )]);

        let chunk = [Chunk::try_new(vec![sorted_vertices.clone()])?];
        write_batches(sorted_gids_path.as_ref(), schema, &chunk)?;
        sorted_vertices
    };

    println!(
        "DONE sorting time: {:?}, len: {}",
        now.elapsed(),
        sorted_vertices.len()
    );

    for (i, gid) in array_as_id_iter(&sorted_vertices)?.enumerate() {
        go.insert(gid, i);
    }

    go.finalize();

    println!(
        "DONE global order time: {:?}, len: {}, vec len: {}",
        now.elapsed(),
        go.len(),
        sorted_vertices.len()
    );

    Ok(())
}
