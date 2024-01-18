//! A data structure for representing input nodes in a temporal graph.
//!
//! Input nodes are used when first creating or accessing verticies by the user.
//! This trait allows you to use a variety of types as input nodes, including
//! `u64`, `&str`, and `String`.

use crate::core::utils::hashing;
use regex::Regex;

pub trait InputNode: Clone {
    fn id(&self) -> u64;
    fn id_str(&self) -> Option<&str>;
}

impl InputNode for u64 {
    fn id(&self) -> u64 {
        *self
    }

    fn id_str(&self) -> Option<&str> {
        None
    }
}

impl<'a> InputNode for &'a str {
    fn id(&self) -> u64 {
        let positive_strict_num = Regex::new(r"^[1-9][0-9]*$").unwrap();
        if *self == "0" || positive_strict_num.is_match(self) {
            self.parse().unwrap()
        } else {
            hashing::calculate_hash(self)
        }
    }

    fn id_str(&self) -> Option<&str> {
        Some(self)
    }
}

impl InputNode for String {
    fn id(&self) -> u64 {
        let s: &str = self;
        s.id()
    }

    fn id_str(&self) -> Option<&str> {
        Some(self)
    }
}

#[cfg(test)]
mod test {
    use crate::core::entities::nodes::input_node::InputNode;
    use regex::Regex;

    #[test]
    fn test_weird_num_edge_cases() {
        assert_ne!("+3".id(), "3".id());
        assert_eq!(3.id(), "3".id());
        assert_ne!("00".id(), "0".id());
        assert_eq!("0".id(), 0.id());
    }

    #[test]
    fn test_num_regex() {
        let re = Regex::new(r"^[1-9][0-9]*$").unwrap();
        assert!(re.is_match("10"));
        assert!(!re.is_match("00"));
        assert!(!re.is_match("+3"));
    }
}
