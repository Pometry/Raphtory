use crate::{errors::GraphError, prelude::GraphViewOps};

pub trait CreateFilter: Sized {
    type EntityFiltered<'graph, G>: GraphViewOps<'graph>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>;
}
