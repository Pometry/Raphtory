use bigdecimal::BigDecimal;
use proptest::{prelude::*, proptest};
use raphtory_api::core::entities::properties::prop::{CastBound, Prop, PropType};
use raphtory_tests::utils::{numeric_prop_type, prop};

fn check_upper_bound_cast(input_value: Prop, target_type: PropType) {
    let bound = input_value.clone().cast_bound(&target_type, true).unwrap(); // numeric casting should return a result
    let dtype = match bound {
        CastBound::Exact(v) => {
            assert!(v.compare(&input_value).unwrap().is_eq()); // should be exact
            v.dtype()
        }
        CastBound::ClampedMax(v) => {
            let expected = target_type.max_value().unwrap();
            assert!(v.compare(&expected).unwrap().is_eq());
            v.dtype()
        }
        CastBound::ClampedMin(v) => {
            let expected = target_type.min_value().unwrap();
            assert!(v.compare(&expected).unwrap().is_eq());
            v.dtype()
        }
        CastBound::Rounded(v) => {
            assert!(input_value.compare(&v).unwrap().is_lt());
            v.dtype()
        }
    };
}

#[test]
fn cast_upper_bounds_proptest() {
    proptest!(ProptestConfig::with_cases(1000000), |(input_value in numeric_prop_type().prop_flat_map(|dtype| prop(&dtype)), target_type in numeric_prop_type())| {
        let bound = input_value.clone().cast_bound(&target_type, true).unwrap(); // numeric casting should return a result
        let dtype = match bound {
            CastBound::Exact(v) => {
                prop_assert!(v.compare(&input_value).unwrap().is_eq()); // should be exact
                v.dtype()
            }
            CastBound::ClampedMax(v) => {
                let expected = target_type.max_value().unwrap();
                prop_assert!(v.compare(&expected).unwrap().is_eq());
                v.dtype()
            }
            CastBound::ClampedMin(v) => {
                let expected = target_type.min_value().unwrap();
                prop_assert!(v.compare(&expected).unwrap().is_eq());
                v.dtype()
            }
            CastBound::Rounded(v) => {
                prop_assert!(input_value.compare(&v).unwrap().is_lt());
                v.dtype()
            }
        };
        // prop_assert_eq!(target_type, dtype);
    })
}

#[test]
fn cast_lower_bounds_proptest() {
    proptest!(ProptestConfig::with_cases(1000000), |(input_value in numeric_prop_type().prop_flat_map(|dtype| prop(&dtype)), target_type in numeric_prop_type())| {
        let bound = input_value.clone().cast_bound(&target_type, false).unwrap(); // numeric casting should return a result
        let dtype = match bound {
            CastBound::Exact(v) => {
                prop_assert!(v.compare(&input_value).unwrap().is_eq()); // should be exact
                v.dtype()
            }
            CastBound::ClampedMax(v) => {
                let expected = target_type.max_value().unwrap();
                prop_assert!(v.compare(&expected).unwrap().is_eq());
                v.dtype()
            }
            CastBound::ClampedMin(v) => {
                let expected = target_type.min_value().unwrap();
                prop_assert!(v.compare(&expected).unwrap().is_eq());
                v.dtype()
            }
            CastBound::Rounded(v) => {
                prop_assert!(input_value.compare(&v).unwrap().is_gt());
                v.dtype()
            }
        };
        // prop_assert_eq!(target_type, dtype);
    })
}

#[test]
fn cast_upper_bounds_test_large_float() {
    check_upper_bound_cast(Prop::U64(15041406696521671), PropType::F64)
}

#[test]
fn cast_upper_bound_decimal_to_u64() {
    check_upper_bound_cast(
        Prop::Decimal(BigDecimal::from_bigint(254963213025981751u64.into(), 7)),
        PropType::U64,
    )
}

#[test]
fn cast_upper_bound_float() {
    check_upper_bound_cast(Prop::F32(0.0), PropType::U64)
}
