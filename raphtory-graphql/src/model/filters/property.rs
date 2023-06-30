use crate::model::filters::primitives::NumberFilter;
use crate::model::graph::node::Node;
use dynamic_graphql::InputObject;
use raphtory::core::Prop;
use raphtory::db::api::view::VertexViewOps;

#[derive(InputObject)]
pub(crate) struct PropertyHasFilter {
    key: Option<String>,
    value_str: Option<String>,
    value_num: Option<NumberFilter>,
}

impl PropertyHasFilter {
    pub(crate) fn matches(&self, node: &Node) -> bool {
        if let Some(key) = &self.key {
            if node.vv.property(key.to_string(), true).is_none() {
                return false;
            }
        }

        if let Some(value_str) = &self.value_str {
            let properties = node.vv.properties(true);
            if properties
                .iter()
                .all(|(_, prop)| value_neq_str_prop(value_str, prop))
            {
                return false;
            }
        }

        if let Some(value_num) = &self.value_num {
            let properties = node.vv.properties(true);
            if properties
                .iter()
                .all(|(_, prop)| value_neq_num_prop(value_num, prop))
            {
                return false;
            }
        }

        true
    }
}

fn value_neq_str_prop(value: &str, prop: &Prop) -> bool {
    if let Prop::Str(prop_str) = prop {
        return value != prop_str;
    }

    false
}

fn value_neq_num_prop(num_filter: &NumberFilter, prop: &Prop) -> bool {
    match prop {
        Prop::I32(i32_prop) => match_signed_num(num_filter, i64::from(*i32_prop)),
        Prop::I64(i64_prop) => match_signed_num(num_filter, *i64_prop),
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
