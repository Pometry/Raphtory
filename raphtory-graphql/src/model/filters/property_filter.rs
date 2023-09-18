use crate::model::{
    filters::primitive_filter::NumberFilter,
    graph::{edge::Edge, node::Node},
};
use dynamic_graphql::InputObject;
use raphtory::{core::Prop, db::api::view::VertexViewOps, prelude::EdgeViewOps};

#[derive(InputObject, Clone)]
pub(crate) struct PropertyHasFilter {
    key: Option<String>,
    value_str: Option<String>,
    value_num: Option<NumberFilter>,
}

impl PropertyHasFilter {
    pub(crate) fn matches_node_properties(&self, node: &Node) -> bool {
        let valid_prop = |prop| valid_prop(prop, &self.value_str, &self.value_num);

        return match &self.key {
            Some(key) => {
                if let Some(prop) = node.vv.properties().get(key) {
                    valid_prop(prop)
                } else {
                    false
                }
            }
            None => node.vv.properties().values().any(valid_prop),
        };
    }

    pub(crate) fn matches_edge_properties(&self, edge: &Edge) -> bool {
        let valid_prop = |prop| valid_prop(prop, &self.value_str, &self.value_num);

        return match &self.key {
            Some(key) => {
                if let Some(prop) = EdgeViewOps::properties(&edge.ee).get(key) {
                    valid_prop(prop)
                } else {
                    false
                }
            }
            None => EdgeViewOps::properties(&edge.ee).values().any(valid_prop),
        };
    }
}

fn valid_prop(prop: Prop, value_str: &Option<String>, num_filter: &Option<NumberFilter>) -> bool {
    if let Some(value_str) = value_str {
        if value_neq_str_prop(value_str, &prop) {
            return false;
        }
    }

    if let Some(num_filter) = num_filter {
        if value_neq_num_prop(num_filter, &prop) {
            return false;
        }
    }

    true
}

fn value_neq_str_prop(value: &str, prop: &Prop) -> bool {
    if let Prop::Str(prop_str) = prop {
        return prop_str != value;
    }

    false
}

fn value_neq_num_prop(num_filter: &NumberFilter, prop: &Prop) -> bool {
    match prop {
        Prop::I32(i32_prop) => match_signed_num(num_filter, i64::from(*i32_prop)),
        Prop::I64(i64_prop) => match_signed_num(num_filter, *i64_prop),
        Prop::U8(u8_prop) => match_unsigned_num(num_filter, u64::from(*u8_prop)),
        Prop::U16(u16_prop) => match_unsigned_num(num_filter, u64::from(*u16_prop)),
        Prop::U32(u32_prop) => match_unsigned_num(num_filter, u64::from(*u32_prop)),
        Prop::U64(u64_prop) => match_unsigned_num(num_filter, *u64_prop),
        Prop::F32(f32_prop) => match_float(num_filter, f64::from(*f32_prop)),
        Prop::F64(f64_prop) => match_float(num_filter, *f64_prop),
        _ => false,
    }
}

fn match_signed_num(num_filter: &NumberFilter, signed_num: i64) -> bool {
    if signed_num < 0 {
        return false;
    }

    let as_usize = signed_num as usize;

    if !num_filter.matches(as_usize) {
        return true;
    }
    false
}

fn match_unsigned_num(num_filter: &NumberFilter, unsigned_num: u64) -> bool {
    let as_usize = unsigned_num as usize;
    if !num_filter.matches(as_usize) {
        return true;
    }
    false
}

fn match_float(num_filter: &NumberFilter, float_num: f64) -> bool {
    if float_num < 0.0 {
        return false;
    }
    let rounded = float_num.round();
    let as_usize = rounded as usize;
    if !num_filter.matches(as_usize) {
        return true;
    }
    false
}
