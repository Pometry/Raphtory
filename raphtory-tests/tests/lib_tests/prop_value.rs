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
    if matches!(target_type, PropType::Decimal { .. }) {
        assert!(matches!(dtype, PropType::Decimal { .. })) // scale doesn't have to match
    } else {
        assert_eq!(dtype, target_type);
    }
}

fn check_lower_bound_cast(input_value: Prop, target_type: PropType) {
    let bound = input_value.clone().cast_bound(&target_type, false).unwrap(); // numeric casting should return a result
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
            assert!(input_value.compare(&v).unwrap().is_gt());
            v.dtype()
        }
    };
    if matches!(target_type, PropType::Decimal { .. }) {
        assert!(matches!(dtype, PropType::Decimal { .. })) // scale doesn't have to match
    } else {
        assert_eq!(dtype, target_type);
    }
}

#[test]
fn cast_upper_bounds_proptest() {
    proptest!(ProptestConfig::with_cases(1000000), |(input_value in numeric_prop_type().prop_flat_map(|dtype| prop(&dtype)), target_type in numeric_prop_type())| {
        check_upper_bound_cast(input_value, target_type);
    })
}

#[test]
fn cast_lower_bounds_proptest() {
    proptest!(ProptestConfig::with_cases(1000000), |(input_value in numeric_prop_type().prop_flat_map(|dtype| prop(&dtype)), target_type in numeric_prop_type())| {
        check_lower_bound_cast(input_value, target_type)
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
