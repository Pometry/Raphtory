use std::{borrow::Borrow, ops::BitAnd};


#[derive(Debug, Clone)]
pub enum Layer<B:Borrow<str>> {
    Name(B),
    And(Box<Layer<B>>, Box<Layer<B>>),
    Or(Box<Layer<B>>, Box<Layer<B>>),
}

impl<B:Borrow<str>> Layer<B> {
    pub fn new(name: B) -> Self {
        Layer::Name(name)
    }

    pub fn names(&self) -> Vec<&str> {
        match self {
            Layer::Name(name) => vec![name.borrow()],
            Layer::And(l1, l2) => {
                let mut names = l1.names();
                names.extend(l2.names());
                names
            }
            Layer::Or(l1, l2) => {
                let mut names = l1.names();
                names.extend(l2.names());
                names
            },
        }
    }
}

impl <B:Borrow<str>> BitAnd for Layer<B> {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        Layer::And(Box::new(self), Box::new(rhs))
    }
}

#[cfg(test)]
mod test{
    use super::*;
    #[test]
    fn test_layer() {
        let layer = Layer::new("test");
        assert_eq!(layer.names(), vec!["test"]);
    }

    // test owned
    #[test]
    fn test_layer_owned() {
        let layer = Layer::new("test".to_string());
        assert_eq!(layer.names(), vec!["test"]);
    }

    #[test]
    fn test_layer_and() {
        let test = Layer::new("test");
        let test2 = Layer::new("test2");
        let layer = test & test2;
        assert_eq!(layer.names(), vec!["test", "test2"]);
    }
}
