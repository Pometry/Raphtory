use crate::{EdgeSegmentOps, NodeSegmentOps, pages::GraphStore};
use arrow::buffer::ScalarBuffer;
use arrow_array::{
    Array, PrimitiveArray, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, types::Int64Type,
};
use arrow_csv::reader::Format;
use arrow_schema::{ArrowError, DataType, Schema, TimeUnit};
use bytemuck::checked::cast_slice_mut;
use db4_common::error::DBV4Error;
use either::Either;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use raphtory::{
    atomic_extra::atomic_usize_from_mut_slice,
    core::entities::{EID, VID, graph::logical_to_physical::Mapping},
    errors::LoadError,
    io::arrow::{
        node_col::NodeCol,
        prop_handler::{PropCols, combine_properties_arrow},
    },
};
use raphtory_api::core::{
    entities::properties::prop::PropType,
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use rayon::prelude::*;
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{self, AtomicBool, AtomicUsize},
    },
};

pub struct Loader<'a> {
    path: PathBuf,
    src_col: Either<&'a str, usize>,
    dst_col: Either<&'a str, usize>,
    time_col: Either<&'a str, usize>,
    format: FileFormat,
}

pub enum FileFormat {
    CSV {
        delimiter: u8,
        has_header: bool,
        sample_records: usize,
    },
    Parquet,
}

pub struct Rows {
    rb: RecordBatch,
    src: usize,
    dst: usize,
    t_properties: Vec<String>,
    t_indices: Vec<usize>,
    time_col: ScalarBuffer<i64>,
}

impl Rows {
    pub fn srcs(&self) -> Result<NodeCol, DBV4Error> {
        let arr = self.rb.column(self.src);
        let arr = arr.as_ref();
        let srcs = NodeCol::try_from(arr)?;
        Ok(srcs)
    }

    pub fn dsts(&self) -> Result<NodeCol, DBV4Error> {
        let arr = self.rb.column(self.dst);
        let arr = arr.as_ref();
        let dsts = NodeCol::try_from(arr)?;
        Ok(dsts)
    }

    pub fn time(&self) -> &[i64] {
        &self.time_col
    }

    pub fn properties(
        &self,
        prop_id_resolver: impl Fn(&str, PropType) -> Result<MaybeNew<usize>, DBV4Error>,
    ) -> Result<PropCols, DBV4Error> {
        combine_properties_arrow(
            &self.t_properties,
            &self.t_indices,
            self.rb.columns(),
            prop_id_resolver,
        )
    }

    fn new(rb: RecordBatch, src: usize, dst: usize, time: usize) -> Result<Self, DBV4Error> {
        let (t_indices, t_properties): (Vec<_>, Vec<_>) = rb
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(id, f)| {
                if id == src || id == dst || id == time {
                    None
                } else {
                    Some((id, f.name().to_owned()))
                }
            })
            .unzip();

        let time_arr = rb.column(time);
        let values = if let Some(arr) = time_arr
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
        {
            arr.values().clone()
        } else if let Some(arr) = time_arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
            let arr_to_millis =
                arrow::compute::cast(&arr, &DataType::Timestamp(TimeUnit::Millisecond, None))?;
            let arr = arr_to_millis
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            arr.values().clone()
        } else if let Some(arr) = time_arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
        {
            let arr_to_millis =
                arrow::compute::cast(&arr, &DataType::Timestamp(TimeUnit::Millisecond, None))?;
            let arr = arr_to_millis
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            arr.values().clone()
        } else if let Some(arr) = time_arr
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
        {
            arr.values().clone()
        } else {
            return Err(DBV4Error::ArrowRS(ArrowError::CastError(format!(
                "failed to cast time column {} to i64",
                time_arr.data_type()
            ))));
        };

        Ok(Self {
            rb,
            src,
            dst,
            t_indices,
            t_properties,
            time_col: values,
        })
    }

    fn num_rows(&self) -> usize {
        self.rb.num_rows()
    }
}

impl<'a> Loader<'a> {
    pub fn new(
        path: &Path,
        src_col: Either<&'a str, usize>,
        dst_col: Either<&'a str, usize>,
        time_col: Either<&'a str, usize>,
        format: FileFormat,
    ) -> Result<Self, DBV4Error> {
        Ok(Self {
            path: path.to_owned(),
            src_col,
            dst_col,
            time_col,
            format,
        })
    }

    pub fn iter_file(
        &self,
        path: &Path,
        rows_per_batch: usize,
    ) -> Result<Box<dyn Iterator<Item = Result<Rows, DBV4Error>> + Send>, DBV4Error> {
        match &self.format {
            FileFormat::CSV {
                delimiter,
                has_header,
                sample_records,
            } => {
                let file = File::open(path).unwrap();
                let (schema, _) = Format::default()
                    .with_header(*has_header)
                    .with_delimiter(*delimiter)
                    .infer_schema(file, Some(*sample_records))?;
                let schema = Arc::new(schema);

                let (src, dst, time) = self.src_dst_time_cols(&schema)?;

                let file = File::open(path)?;

                let reader = arrow_csv::reader::ReaderBuilder::new(schema.clone())
                    .with_header(*has_header)
                    .with_delimiter(*delimiter)
                    .with_batch_size(rows_per_batch)
                    .build(file)?;
                Ok(Box::new(reader.map(move |rb| {
                    rb.map_err(DBV4Error::from)
                        .and_then(|rb| Rows::new(rb, src, dst, time))
                })))
            }
            FileFormat::Parquet => {
                let file = File::open(path)?;
                let builder =
                    ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(rows_per_batch);

                let (src, dst, time) = self.src_dst_time_cols(&builder.schema())?;
                let reader = builder.build()?;
                Ok(Box::new(reader.map(move |rb| {
                    rb.map_err(DBV4Error::from)
                        .and_then(|rb| Rows::new(rb, src, dst, time))
                })))
            }
        }
    }

    pub fn iter(
        &self,
        rows_per_batch: usize,
    ) -> Result<Box<dyn Iterator<Item = Result<Rows, DBV4Error>> + Send>, DBV4Error> {
        if self.path.is_dir() {
            let mut files = vec![];
            for entry in std::fs::read_dir(&self.path)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    files.push(entry.path());
                }
            }
            let iterators: Vec<_> = files
                .into_iter()
                .map(|path| self.iter_file(&path, rows_per_batch))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Box::new(iterators.into_iter().flatten()))
        } else {
            Ok(self.iter_file(&self.path, rows_per_batch)?)
        }
    }

    fn src_dst_time_cols(&self, schema: &Schema) -> Result<(usize, usize, usize), DBV4Error> {
        let src_field = match self.src_col {
            Either::Left(name) => schema.index_of(name)?,
            Either::Right(idx) => idx,
        };
        let dst_field = match self.dst_col {
            Either::Left(name) => schema.index_of(name)?,
            Either::Right(idx) => idx,
        };

        let time_field = match self.time_col {
            Either::Left(name) => schema.index_of(name)?,
            Either::Right(idx) => idx,
        };

        Ok((src_field, dst_field, time_field))
    }

    pub fn load_into<
        NS: NodeSegmentOps<Extension = EXT>,
        ES: EdgeSegmentOps<Extension = EXT>,
        EXT: Clone + Default + Send + Sync,
    >(
        &self,
        graph: &GraphStore<NS, ES, EXT>,
        rows_per_batch: usize,
    ) -> Result<Mapping, DBV4Error> {
        let mut src_col_resolved: Vec<VID> = vec![];
        let mut dst_col_resolved: Vec<VID> = vec![];
        let mut eid_col_resolved: Vec<EID> = vec![];
        let mut eids_exist: Vec<AtomicBool> = vec![]; // exists or needs to be created

        let max_edge_id = AtomicUsize::new(graph.edges().num_edges().saturating_sub(1));

        let resolver = Mapping::new();

        let next_id = AtomicUsize::new(0);
        let mut offset = 0;

        let now = std::time::Instant::now();
        for chunk in self.iter(rows_per_batch)? {
            let now_chunk = std::time::Instant::now();
            let rb = chunk?;

            let props = rb.properties(|name, p_type| {
                graph
                    .edge_meta()
                    .resolve_prop_id(name, p_type, false)
                    .map_err(DBV4Error::from)
            })?;

            let srcs = rb.srcs()?;
            let dsts = rb.dsts()?;

            src_col_resolved.resize_with(rb.num_rows(), Default::default);
            srcs.par_iter()
                .zip(src_col_resolved.par_iter_mut())
                .try_for_each(|(gid, resolved)| {
                    let gid = gid.ok_or_else(|| LoadError::MissingSrcError)?;
                    let id = resolver
                        .get_or_init(gid, || VID(next_id.fetch_add(1, atomic::Ordering::Relaxed)))
                        .unwrap()
                        .inner();
                    *resolved = id;
                    Ok::<(), DBV4Error>(())
                })?;

            dst_col_resolved.resize_with(rb.num_rows(), Default::default);
            dsts.par_iter()
                .zip(dst_col_resolved.par_iter_mut())
                .try_for_each(|(gid, resolved)| {
                    let gid = gid.ok_or_else(|| LoadError::MissingDstError)?;
                    let id = resolver
                        .get_or_init(gid, || VID(next_id.fetch_add(1, atomic::Ordering::Relaxed)))
                        .unwrap()
                        .inner();
                    *resolved = id;
                    Ok::<(), DBV4Error>(())
                })?;

            eid_col_resolved.resize_with(rb.num_rows(), Default::default);
            eids_exist.resize_with(rb.num_rows(), Default::default);
            let eid_col_shared = atomic_usize_from_mut_slice(cast_slice_mut(&mut eid_col_resolved));

            let num_pages =
                next_id.load(atomic::Ordering::Relaxed) / graph.nodes().max_page_len() + 1;
            graph.nodes().grow(num_pages);

            let mut node_writers = graph.nodes().locked();

            node_writers.par_iter_mut().try_for_each(|locked_page| {
                for (row, (&src, &dst)) in src_col_resolved
                    .iter()
                    .zip(dst_col_resolved.iter())
                    .enumerate()
                {
                    if let Some(src_pos) = locked_page.resolve_pos(src) {
                        let mut writer = locked_page.writer();
                        if let Some(edge_id) = writer.get_out_edge(src_pos, dst) {
                            eid_col_shared[row].store(edge_id.0, atomic::Ordering::Relaxed);
                            eids_exist[row].store(true, atomic::Ordering::Relaxed);
                        } else {
                            let edge_id = EID(max_edge_id.fetch_add(1, atomic::Ordering::Relaxed));
                            writer.add_outbound_edge(0, src_pos, dst, edge_id.with_layer(0), 0); // FIXME: when we update this to work with layers use the correct layer
                            eid_col_shared[row].store(edge_id.0, atomic::Ordering::Relaxed);
                            eids_exist[row].store(false, atomic::Ordering::Relaxed);
                        }
                    }
                }

                Ok::<_, DBV4Error>(())
            })?;

            node_writers.par_iter_mut().try_for_each(|locked_page| {
                for (&edge_id, (&src, &dst)) in eid_col_resolved
                    .iter()
                    .zip(src_col_resolved.iter().zip(&dst_col_resolved))
                {
                    if let Some(dst_pos) = locked_page.resolve_pos(dst) {
                        let mut writer = locked_page.writer();
                        if !writer.get_inb_edge(dst_pos, src).is_some() {
                            let edge_id = EID(edge_id.0);
                            writer.add_inbound_edge(0, dst_pos, src, edge_id.with_layer(0), 0); // FIXME: when we update this to work with layers use the correct layer
                        }
                    }
                }

                Ok::<_, DBV4Error>(())
            })?;

            // now edges

            let num_pages =
                max_edge_id.load(atomic::Ordering::Relaxed) / graph.edges().max_page_len() + 1;

            graph.edges().grow(num_pages);

            let mut edge_writers = graph.edges().locked();

            let time_col = rb.time();

            edge_writers.iter_mut().try_for_each(|edge_writer| {
                for (row_idx, ((((&src, &dst), &eid), edge_exists), time)) in src_col_resolved
                    .iter()
                    .zip(&dst_col_resolved)
                    .zip(&eid_col_resolved)
                    .zip(
                        eids_exist
                            .iter()
                            .map(|exists| exists.load(atomic::Ordering::Relaxed)),
                    )
                    .zip(time_col)
                    .enumerate()
                {
                    if let Some(local_pos) = edge_writer.resolve_pos(eid) {
                        let mut writer = edge_writer.writer();
                        let time = TimeIndexEntry::new(*time, offset + row_idx);
                        writer.add_edge(
                            time,
                            Some(local_pos),
                            src,
                            dst,
                            props.iter_row(row_idx),
                            0,
                            Some(edge_exists),
                        )?;
                    }
                }
                Ok::<_, DBV4Error>(())
            })?;

            src_col_resolved.clear();
            dst_col_resolved.clear();
            eid_col_resolved.clear();
            eids_exist.clear();
            offset += rb.num_rows();

            // println!(
            //     "Loaded {} events in {:?}. Average {} events/s. Batch time: {:?}",
            //     offset,
            //     now.elapsed(),
            //     offset as f64 / now.elapsed().as_secs_f64(),
            //     now_chunk.elapsed(),
            // );
        }

        Ok(resolver)
    }
}

#[cfg(test)]
mod test {
    use crate::{Layer, pages::test_utils::check_load_support};
    use proptest::{collection::vec, prelude::*};

    fn check_load(edges: &[(i64, u64, u64)], max_page_len: usize) {
        check_load_support(edges, false, |path| {
            Layer::<()>::new(path, max_page_len, max_page_len)
        });
    }

    #[test]
    fn test_one_edge() {
        check_load(&[(0, 0, 1)], 32);
    }

    #[test]
    fn test_load_graph_from_csv() {
        let edge_strat = (1u64..100).prop_flat_map(|num_nodes| {
            (1usize..100).prop_flat_map(move |num_edges| {
                vec(((0i64..100), (0..num_nodes), (0..num_nodes)), num_edges)
            })
        });

        proptest!(|(edges in edge_strat, max_page_len in 1usize .. 100)| {
            check_load(&edges, max_page_len);
        });
    }

    #[test]
    fn teas_load_graph_from_csv_5() {
        let edges = [
            (42, 16, 24),
            (96, 41, 8),
            (37, 9, 9),
            (62, 37, 57),
            (12, 49, 23),
            (8, 60, 44),
            (56, 35, 0),
            (9, 48, 58),
            (59, 20, 37),
            (36, 17, 46),
        ];
        let max_page_len = 7;
        check_load(&edges, max_page_len);
    }

    #[test]
    fn test_load_graph_from_csv_4() {
        let edges = [
            (27, 20, 85),
            (2, 29, 77),
            (55, 59, 22),
            (72, 47, 73),
            (26, 66, 36),
            (22, 39, 37),
            (5, 49, 88),
            (2, 48, 13),
            (97, 23, 57),
        ];
        let max_page_len = 8;
        check_load(&edges, max_page_len);
    }

    #[test]
    fn test_load_graph_from_csv_1() {
        let edges = [(0, 33, 31), (1, 12, 20), (2, 22, 32)];

        check_load(&edges, 32);
    }

    #[test]
    fn test_load_graph_from_csv_2() {
        let edges = [
            (0, 23, 61),
            (1, 52, 14),
            (2, 62, 62),
            (3, 13, 9),
            (4, 29, 6),
            (5, 13, 7),
        ];

        check_load(&edges, 5);
    }

    #[test]
    fn test_load_graph_from_csv_3() {
        let edges = [(0, 0, 32)];

        check_load(&edges, 51);
    }

    #[test]
    fn test_edges_1() {
        let edges = [(0, 1, 0), (0, 0, 0), (0, 0, 0)];

        check_load(&edges, 32);
    }
}
