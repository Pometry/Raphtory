pub mod edge;
pub mod graph;
pub mod path;
pub mod vertex;
pub mod vertices;
pub mod views;

#[cfg(test)]
mod test {
    use crate::{core::AsProp, prelude::*};

    #[test]
    fn easy_ways_to_add_vertex() {
        let g = Graph::new();
        let v = g.add_vertex2(1, "Gandalf", [("age", 2019)]);
        let v = g.add_vertex2(
            1,
            "Gandalf",
            [("age", 2019.as_prop()), ("type", "Character".as_prop())],
        );
        let v = g.add_vertex2(1, "Gandalf", EMPTY);
    }
}
