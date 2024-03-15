use std::{iter, sync::Arc};

use arrow2::array::PrimitiveArray;
use raphtory::{arrow::{chunked_array::{chunked_array::{ChunkedArray, NonNull}, list_array::ChunkedListArray, ChunkedArraySlice}, graph_impl::ArrowGraph, prelude::BaseArrayOps}, core::Direction};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use super::{expr::Expr, Context, DataBlock, Source, Column};

pub trait Operator {
    fn execute(&self, input: DataBlock, ctx: Context) -> impl Iterator<Item = DataBlock>;
}

#[derive(Debug, Clone)]
pub enum PhysicalOperator {
    Expand(Expand),
    Filter(Filter),
}

#[derive(Debug, Clone)]
pub struct Expand {
    dir: Direction,
    from_col: usize,
    filter: Expr,
}

#[derive(Debug, Clone)]
pub struct Filter {
    expr: Expr,
}

pub struct EdgeScan {
    layer: String,
    columns: Arc<[usize]>,
}

impl EdgeScan {
    pub fn new(layer: impl AsRef<str>, columns: Arc<[usize]>) -> Self {
        Self { layer: layer.as_ref().to_string(), columns }
    }
}

impl Source for EdgeScan {
    fn produce<'a, 'g>(&'a self, graph: &'g ArrowGraph, producer: Arc<dyn Fn(DataBlock) + Send + Sync + 'a>) -> Result<(), super::ExecError> {
        let layer_id = graph.find_layer_id(&self.layer).ok_or(super::ExecError::LayerNotFound(self.layer.clone()))?;
        let layer = graph.layer(layer_id);

        let edges = layer.edges_storage();

        // chunk edges into windows of 1000 as parallel iterators
        let chunk_size = 1000;
        let num_morcels = edges.len() / chunk_size;
        (0..num_morcels).into_par_iter().map(|i| {
            let start = i * chunk_size;
            let end = (i + 1) * chunk_size;
            (start, end)
        }).for_each(|(start, end)| {
            let mut cols = vec![];
            let srcs = edges.srcs().sliced(start .. end);
            let dsts = edges.dsts().sliced(start .. end);
            let time: ChunkedArraySlice<'_, ChunkedListArray<'_, ChunkedArray<PrimitiveArray<i64>, NonNull>>> = edges.time().sliced(start .. end);
            
            cols.push(Column::Ids(srcs));
            cols.push(Column::Ids(dsts));

            for col in self.columns.iter() {
            }

            let block = DataBlock {
                cols,
            };
            producer(block);
        });

        Ok(())
    }
}


pub struct NodeScan {
    columns: Arc<[usize]>, // name could be one column
}

impl Operator for Expand {
    fn execute(&self, input: DataBlock, ctx: Context) -> impl Iterator<Item = DataBlock> {
        iter::empty()
    }
}

impl Operator for Filter {
    fn execute(&self, input: DataBlock, ctx: Context) -> impl Iterator<Item = DataBlock> {
        iter::empty()
    }
}
