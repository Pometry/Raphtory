#[cfg(test)]
mod secondary_index_col_tests {
    use raphtory::arrow_loader::dataframe::SecondaryIndexCol;
    use raphtory::errors::LoadError;
    use arrow::{
        array::{Float64Array, Int32Array, Int64Array, StringArray, UInt32Array, UInt64Array},
        datatypes::DataType,
    };

    fn values(col: SecondaryIndexCol) -> Vec<usize> {
        col.iter().collect()
    }

    #[test]
    fn uint64_is_taken_as_is() {
        let col = SecondaryIndexCol::new_from_df(&UInt64Array::from(vec![10, 20])).unwrap();
        assert_eq!(values(col), vec![10, 20]);
    }

    #[test]
    fn other_integer_widths_are_cast() {
        let col = SecondaryIndexCol::new_from_df(&Int64Array::from(vec![10, 20])).unwrap();
        assert_eq!(values(col), vec![10, 20]);
        let col = SecondaryIndexCol::new_from_df(&Int32Array::from(vec![10, 20])).unwrap();
        assert_eq!(values(col), vec![10, 20]);
        let col = SecondaryIndexCol::new_from_df(&UInt32Array::from(vec![10, 20])).unwrap();
        assert_eq!(values(col), vec![10, 20]);
    }

    #[test]
    fn a_negative_value_is_an_error_not_a_wrap_or_a_null() {
        let err = SecondaryIndexCol::new_from_df(&Int64Array::from(vec![10, -3]))
            .err()
            .unwrap();
        assert!(
            matches!(err, LoadError::NegativeSecondaryIndex(-3)),
            "{err:?}"
        );
        let err = SecondaryIndexCol::new_from_df(&Int32Array::from(vec![-1]))
            .err()
            .unwrap();
        assert!(
            matches!(err, LoadError::NegativeSecondaryIndex(-1)),
            "{err:?}"
        );
    }

    #[test]
    fn a_non_integer_column_is_an_error_not_a_panic() {
        let err = SecondaryIndexCol::new_from_df(&Float64Array::from(vec![1.0]))
            .err()
            .unwrap();
        assert!(
            matches!(err, LoadError::InvalidSecondaryIndexType(DataType::Float64)),
            "{err:?}"
        );
        let err = SecondaryIndexCol::new_from_df(&StringArray::from(vec!["1"]))
            .err()
            .unwrap();
        assert!(
            matches!(err, LoadError::InvalidSecondaryIndexType(DataType::Utf8)),
            "{err:?}"
        );
    }

    #[test]
    fn a_null_is_still_a_missing_value() {
        let err = SecondaryIndexCol::new_from_df(&Int64Array::from(vec![Some(1), None]))
            .err()
            .unwrap();
        assert!(
            matches!(err, LoadError::MissingSecondaryIndexError),
            "{err:?}"
        );
    }
}
