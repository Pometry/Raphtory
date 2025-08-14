use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use proptest::prelude::*;
use raphtory_api::core::entities::properties::prop::{DECIMAL_MAX, Prop, PropType};
use std::collections::HashMap;

pub fn prop_type() -> impl Strategy<Value = PropType> {
    let leaf = proptest::sample::select(&[
        PropType::Str,
        PropType::I64,
        PropType::F64,
        PropType::F32,
        PropType::I32,
        PropType::U8,
        PropType::Bool,
        PropType::DTime,
        PropType::NDTime,
        PropType::Decimal { scale: 7 }, // decimal breaks the tests because of polars-parquet
    ]);

    // leaf.prop_recursive(3, 10, 10, |inner| {
    //     let dict = proptest::collection::hash_map(r"\w{1,10}", inner.clone(), 1..10)
    //         .prop_map(|map| PropType::map(map));
    //     let list = inner
    //         .clone()
    //         .prop_map(|p_type| PropType::List(Box::new(p_type)));
    //     prop_oneof![inner, list, dict]
    // })
    leaf
}

pub fn make_props(
    schema: &HashMap<String, PropType>,
) -> (
    BoxedStrategy<Vec<(String, Prop)>>,
    BoxedStrategy<Vec<(String, Prop)>>,
) {
    let mut iter = schema.iter();

    // split in half, one temporal one constant
    let t_prop_s = (&mut iter)
        .take(schema.len() / 2)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();
    let c_prop_s = iter
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();

    let num_tprops = t_prop_s.len();
    let num_cprops = c_prop_s.len();

    let t_props = proptest::sample::subsequence(t_prop_s, 0..=num_tprops).prop_flat_map(|schema| {
        schema
            .into_iter()
            .map(|(k, v)| prop(&v).prop_map(move |prop| (k.clone(), prop)))
            .collect::<Vec<_>>()
    });
    let c_props = proptest::sample::subsequence(c_prop_s, 0..=num_cprops).prop_flat_map(|schema| {
        schema
            .into_iter()
            .map(|(k, v)| prop(&v).prop_map(move |prop| (k.clone(), prop)))
            .collect::<Vec<_>>()
    });
    (t_props.boxed(), c_props.boxed())
}

pub(crate) fn prop(p_type: &PropType) -> impl Strategy<Value = Prop> + use<> {
    match p_type {
        PropType::Str => (0i32..1000).prop_map(|s| Prop::str(s.to_string())).boxed(),
        PropType::I64 => any::<i64>().prop_map(Prop::I64).boxed(),
        PropType::I32 => any::<i32>().prop_map(Prop::I32).boxed(),
        PropType::F64 => any::<f64>().prop_map(Prop::F64).boxed(),
        PropType::F32 => any::<f32>().prop_map(Prop::F32).boxed(),
        PropType::U8 => any::<u8>().prop_map(Prop::U8).boxed(),
        PropType::Bool => any::<bool>().prop_map(Prop::Bool).boxed(),
        PropType::DTime => (1900..2024, 1..=12, 1..28, 0..24, 0..60, 0..60)
            .prop_map(|(year, month, day, h, m, s)| {
                Prop::DTime(
                    format!(
                        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                        year, month, day, h, m, s
                    )
                    .parse::<DateTime<Utc>>()
                    .unwrap(),
                )
            })
            .boxed(),
        PropType::NDTime => (1970..2024, 1..=12, 1..28, 0..24, 0..60, 0..60)
            .prop_map(|(year, month, day, h, m, s)| {
                // 2015-09-18T23:56:04
                Prop::NDTime(
                    format!(
                        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
                        year, month, day, h, m, s
                    )
                    .parse::<NaiveDateTime>()
                    .unwrap(),
                )
            })
            .boxed(),
        PropType::List(p_type) => proptest::collection::vec(prop(p_type), 0..10)
            .prop_map(|props| Prop::List(props.into()))
            .boxed(),
        PropType::Map(p_types) => {
            let prop_types: Vec<BoxedStrategy<(String, Prop)>> = p_types
                .iter()
                .map(|(a, b)| (a.clone(), b.clone()))
                .collect::<Vec<_>>()
                .into_iter()
                .map(|(name, p_type)| {
                    let pt_strat = prop(&p_type)
                        .prop_map(move |prop| (name.clone(), prop.clone()))
                        .boxed();
                    pt_strat
                })
                .collect_vec();

            let props = proptest::sample::select(prop_types).prop_flat_map(|prop| prop);

            proptest::collection::vec(props, 1..10)
                .prop_map(|props| Prop::map(props))
                .boxed()
        }
        PropType::Decimal { scale } => {
            let scale = *scale;
            let dec_max = DECIMAL_MAX;
            ((scale as i128)..dec_max)
                .prop_map(move |int| Prop::Decimal(BigDecimal::new(int.into(), scale)))
                .boxed()
        }
        pt => {
            panic!("Unsupported prop type: {:?}", pt);
        }
    }
}
