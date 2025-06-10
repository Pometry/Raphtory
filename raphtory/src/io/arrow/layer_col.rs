use crate::{
    errors::{into_graph_err, GraphError, LoadError},
    io::arrow::dataframe::DFChunk,
    prelude::AdditionOps,
};
use polars_arrow::array::{StaticArray, Utf8Array, Utf8ViewArray};
use raphtory_storage::mutation::addition_ops::SessionAdditionOps;
use rayon::{
    iter::{
        plumbing::{Consumer, ProducerCallback, UnindexedConsumer},
        IndexedParallelIterator, IntoParallelIterator, ParallelIterator,
    },
    prelude::*,
};

#[derive(Copy, Clone)]
pub(crate) enum LayerCol<'a> {
    Name { name: Option<&'a str>, len: usize },
    Utf8 { col: &'a Utf8Array<i32> },
    LargeUtf8 { col: &'a Utf8Array<i64> },
    Utf8View { col: &'a Utf8ViewArray },
}

pub enum LayerColVariants<Name, Utf8, LargeUtf8, Utf8View> {
    Name(Name),
    Utf8(Utf8),
    LargeUtf8(LargeUtf8),
    Utf8View(Utf8View),
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            LayerColVariants::Name($pattern) => $result,
            LayerColVariants::Utf8($pattern) => $result,
            LayerColVariants::LargeUtf8($pattern) => $result,
            LayerColVariants::Utf8View($pattern) => $result,
        }
    };
}

impl<
        V: Send,
        Name: ParallelIterator<Item = V>,
        Utf8: ParallelIterator<Item = V>,
        LargeUtf8: ParallelIterator<Item = V>,
        Utf8View: ParallelIterator<Item = V>,
    > ParallelIterator for LayerColVariants<Name, Utf8, LargeUtf8, Utf8View>
{
    type Item = V;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        for_all!(self, iter => iter.drive_unindexed(consumer))
    }

    fn opt_len(&self) -> Option<usize> {
        for_all!(self, iter => iter.opt_len())
    }
}

impl<
        V: Send,
        Name: IndexedParallelIterator<Item = V>,
        Utf8: IndexedParallelIterator<Item = V>,
        LargeUtf8: IndexedParallelIterator<Item = V>,
        Utf8View: IndexedParallelIterator<Item = V>,
    > IndexedParallelIterator for LayerColVariants<Name, Utf8, LargeUtf8, Utf8View>
{
    fn len(&self) -> usize {
        for_all!(self, iter => iter.len())
    }

    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        for_all!(self, iter => iter.drive(consumer))
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        for_all!(self, iter => iter.with_producer(callback))
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
        let session = graph.write_session().map_err(|err| err.into())?;
        match self {
            LayerCol::Name { name, len } => {
                let layer = session.resolve_layer(name).map_err(into_graph_err)?.inner();
                Ok(vec![layer; len])
            }
            col => {
                let iter = col.par_iter();
                let mut res = vec![0usize; iter.len()];
                iter.zip(res.par_iter_mut())
                    .try_for_each(|(layer, entry)| {
                        let layer = session
                            .resolve_layer(layer)
                            .map_err(into_graph_err)?
                            .inner();
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
