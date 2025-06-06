//! A data structure for representing input nodes in a temporal graph.
//!
//! Input nodes are used when first creating or accessing verticies by the user.
//! This trait allows you to use a variety of types as input nodes, including
//! `u64`, `&str`, and `String`.

use crate::core::utils::hashing;

const MAX_U64_BYTES: [u8; 20] = [
    49, 56, 52, 52, 54, 55, 52, 52, 48, 55, 51, 55, 48, 57, 53, 53, 49, 54, 49, 53,
];

pub fn parse_u64_strict(input: &str) -> Option<u64> {
    if input.len() > 20 {
        // a u64 string has at most 20 bytes
        return None;
    }
    let byte_0 = b'0';
    let byte_1 = b'1';
    let byte_9 = b'9';
    let mut input_iter = input.bytes();
    let first = input_iter.next()?;
    if first == byte_0 {
        return input_iter.next().is_none().then_some(0);
    }

    let mut check_max = input.len() == 20;
    if check_max {
        if !(byte_1..=MAX_U64_BYTES[0]).contains(&first) {
            return None;
        }
    } else if !(byte_1..=byte_9).contains(&first) {
        return None;
    }

    let mut result = (first - byte_0) as u64;
    for (next_byte, max_byte) in input_iter.zip(MAX_U64_BYTES[1..].iter().copied()) {
        if check_max {
            if !(byte_0..=max_byte).contains(&next_byte) {
                return None;
            }
            check_max = next_byte == max_byte;
        } else if !(byte_0..=byte_9).contains(&next_byte) {
            return None;
        }
        result = result * 10 + (next_byte - byte_0) as u64;
    }
    Some(result)
}

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

impl InputNode for &str {
    fn id(&self) -> u64 {
        parse_u64_strict(self).unwrap_or_else(|| hashing::calculate_hash(self))
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
    use crate::core::input::input_node::{parse_u64_strict, InputNode};
    use proptest::prelude::*;

    #[test]
    fn test_weird_num_edge_cases() {
        assert_ne!("+3".id(), "3".id());
        assert_eq!(3.id(), "3".id());
        assert_ne!("00".id(), "0".id());
        assert_eq!("0".id(), 0.id());
    }

    #[test]
    fn test_u64_string_works() {
        proptest!(|(n in any::<u64>())| {
            assert_eq!(n.to_string().id(), n);
        });
    }

    #[test]
    fn test_if_str_parses_it_is_a_u64() {
        proptest!(|(s in any::<String>())| {
            let res = parse_u64_strict(&s);
            if let Some(n) = res {
                assert_eq!(n.to_string(), s)
            } else {
                assert_ne!(s.id().to_string(), s)
            }
        });
    }
}
