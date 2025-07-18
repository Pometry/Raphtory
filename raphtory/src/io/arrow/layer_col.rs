use crate::{
    errors::{into_graph_err, GraphError, LoadError},
    io::arrow::dataframe::DFChunk,
    prelude::AdditionOps,
};
use iter_enum::{
    DoubleEndedIterator, ExactSizeIterator, IndexedParallelIterator, Iterator, ParallelIterator,
};
use polars_arrow::array::{StaticArray, Utf8Array, Utf8ViewArray};
use rayon::prelude::*;

#[derive(Copy, Clone)]
pub(crate) enum LayerCol<'a> {
    Name { name: Option<&'a str>, len: usize },
    Utf8 { col: &'a Utf8Array<i32> },
    LargeUtf8 { col: &'a Utf8Array<i64> },
    Utf8View { col: &'a Utf8ViewArray },
}

#[derive(
    Iterator, DoubleEndedIterator, ExactSizeIterator, ParallelIterator, IndexedParallelIterator,
)]
pub enum LayerColVariants<Name, Utf8, LargeUtf8, Utf8View> {
    Name(Name),
    Utf8(Utf8),
    LargeUtf8(LargeUtf8),
    Utf8View(Utf8View),
}

        }
    }

impl<'a> LayerCol<'a> {
    pub fn par_iter(self) -> impl IndexedParallelIterator<Item = Option<&'a str>> {
        match self {
            LayerCol::Name { name, len } => {
                LayerColVariants::Name((0..len).into_par_iter().map(move |_| name))
            }
            LayerCol::Utf8 { col } => {
                LayerColVariants::Utf8((0..col.len()).into_par_iter().map(|i| col.get(i)))
            }
            LayerCol::LargeUtf8 { col } => {
                LayerColVariants::LargeUtf8((0..col.len()).into_par_iter().map(|i| col.get(i)))
            }

            LayerCol::Utf8View { col } => {
                LayerColVariants::Utf8View((0..col.len()).into_par_iter().map(|i| col.get(i)))
            }
        }
    }

    pub fn resolve(
        self,
        graph: &(impl AdditionOps + Send + Sync),
    ) -> Result<Vec<usize>, GraphError> {
        match self {
            LayerCol::Name { name, len } => {
                let layer = graph.resolve_layer(name).map_err(into_graph_err)?.inner();
                Ok(vec![layer; len])
            }
            col => {
                let iter = col.par_iter();
                let mut res = vec![0usize; iter.len()];
                iter.zip(res.par_iter_mut())
                    .try_for_each(|(layer, entry)| {
                        let layer = graph.resolve_layer(layer).map_err(into_graph_err)?.inner();
                        *entry = layer;
                        Ok::<(), GraphError>(())
                    })?;
                Ok(res)
            }
        }
    }
}

pub(crate) fn lift_layer_col<'a>(
    layer_name: Option<&'a str>,
    layer_index: Option<usize>,
    df: &'a DFChunk,
) -> Result<LayerCol<'a>, GraphError> {
    match (layer_name, layer_index) {
        (name, None) => Ok(LayerCol::Name {
            name,
            len: df.len(),
        }),
        (None, Some(layer_index)) => {
            let col = &df.chunk[layer_index];
            if let Some(col) = col.as_any().downcast_ref::<Utf8Array<i32>>() {
                Ok(LayerCol::Utf8 { col })
            } else if let Some(col) = col.as_any().downcast_ref::<Utf8Array<i64>>() {
                Ok(LayerCol::LargeUtf8 { col })
            } else if let Some(col) = col.as_any().downcast_ref::<Utf8ViewArray>() {
                Ok(LayerCol::Utf8View { col })
            } else {
                Err(LoadError::InvalidLayerType(col.data_type().clone()).into())
            }
        }
        _ => Err(GraphError::WrongNumOfArgs(
            "layer_name".to_string(),
            "layer_col".to_string(),
        )),
    }
}

pub(crate) fn lift_node_type_col<'a>(
    node_type_name: Option<&'a str>,
    node_type_index: Option<usize>,
    df: &'a DFChunk,
) -> Result<LayerCol<'a>, GraphError> {
    match (node_type_name, node_type_index) {
        (name, None) => Ok(LayerCol::Name {
            name,
            len: df.len(),
        }),
        (None, Some(layer_index)) => {
            let col = &df.chunk[layer_index];
            if let Some(col) = col.as_any().downcast_ref::<Utf8Array<i32>>() {
                Ok(LayerCol::Utf8 { col })
            } else if let Some(col) = col.as_any().downcast_ref::<Utf8Array<i64>>() {
                Ok(LayerCol::LargeUtf8 { col })
            } else if let Some(col) = col.as_any().downcast_ref::<Utf8ViewArray>() {
                Ok(LayerCol::Utf8View { col })
            } else {
                Err(LoadError::InvalidNodeType(col.data_type().clone()).into())
            }
        }
        _ => Err(GraphError::WrongNumOfArgs(
            "node_type_name".to_string(),
            "node_type_col".to_string(),
        )),
    }
}
