use crate::core::{tgraph2::{VID, vertex::Vertex, edge::EdgeView}, Direction};

pub trait VertexListOps {
    type Value<T>;
    type Edge;
    type Neighbour;

    fn ids(self) -> Self::Value<VID>;
    fn edges(self) -> Self::Value<Self::Edge>;
    fn neighbours(self) -> Self::Value<Self::Neighbour>;
}

impl<
        'a,
        const N: usize,
        I: IntoIterator<Item = Vertex<'a, N>> + 'a,
    > VertexListOps for I
{
    type Value<T> = Box<dyn Iterator<Item = T> + 'a>;
    type Edge = EdgeView<'a, N>;
    type Neighbour = Vertex<'a, N>;
    fn ids(self) -> Self::Value<VID> {
        let iter = self.into_iter().map(|v| v.id());
        Box::new(iter)
    }

    fn edges(self) -> Self::Value<Self::Edge> {
        let iter = self
            .into_iter()
            .flat_map(|v| v.edges("follows", Direction::OUT));
        Box::new(iter)
    }

    fn neighbours(self) -> Self::Value<Self::Neighbour> {
        let iter = self
            .into_iter()
            .flat_map(|v| v.edges("follows", Direction::OUT).map(|e| e.dst()));
        Box::new(iter)
    }
}

