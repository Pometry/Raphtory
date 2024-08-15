use chrono::{DateTime, Utc};
use polars_arrow::{
    array::{Array, BooleanArray, FixedSizeListArray, ListArray, PrimitiveArray, Utf8Array},
    datatypes::{ArrowDataType as DataType, TimeUnit},
};

use crate::{
    core::{utils::errors::GraphError, IntoPropList},
    io::arrow::dataframe::DFChunk,
    prelude::Prop,
};

pub struct PropIter<'a> {
    inner: Vec<Box<dyn Iterator<Item = Option<(&'a str, Prop)>> + 'a>>,
}

impl<'a> Iterator for PropIter<'a> {
    type Item = Vec<(&'a str, Prop)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .iter_mut()
            .map(|v| v.next())
            .filter_map(|r| match r {
                Some(r1) => match r1 {
                    Some(r2) => Some(Some(r2)),
                    None => None,
                },
                None => Some(None),
            })
            .collect()
    }
}

pub(crate) fn combine_properties<'a>(
    props: &'a [&str],
    indices: &'a [usize],
    df: &'a DFChunk,
) -> Result<PropIter<'a>, GraphError> {
    for idx in indices {
        is_data_type_supported(df.chunk[*idx].data_type())?;
    }
    let zipped = props.iter().zip(indices.iter());
    let iter = zipped.map(|(name, idx)| lift_property(*idx, name, df));
    Ok(PropIter {
        inner: iter.collect(),
    })
}

fn arr_as_prop(arr: Box<dyn Array>) -> Prop {
    match arr.data_type() {
        DataType::Boolean => {
            let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.iter().flatten().into_prop_list()
        }
        DataType::Int32 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::Int64 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::UInt8 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u8>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::UInt16 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u16>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::UInt32 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u32>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::UInt64 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u64>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::Float32 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::Float64 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::Utf8 => {
            let arr = arr.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            arr.iter().flatten().into_prop_list()
        }
        DataType::LargeUtf8 => {
            let arr = arr.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            arr.iter().flatten().into_prop_list()
        }
        DataType::List(_) => {
            let arr = arr.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            arr.iter()
                .flatten()
                .map(|elem| arr_as_prop(elem))
                .into_prop_list()
        }
        DataType::FixedSizeList(_, _) => {
            let arr = arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            arr.iter()
                .flatten()
                .map(|elem| arr_as_prop(elem))
                .into_prop_list()
        }
        DataType::LargeList(_) => {
            let arr = arr.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            arr.iter()
                .flatten()
                .map(|elem| arr_as_prop(elem))
                .into_prop_list()
        }
        _ => panic!("Data type not recognized"),
    }
}

fn is_data_type_supported(dt: &DataType) -> Result<(), GraphError> {
    match dt {
        DataType::Boolean => {}
        DataType::Int32 => {}
        DataType::Int64 => {}
        DataType::UInt8 => {}
        DataType::UInt16 => {}
        DataType::UInt32 => {}
        DataType::UInt64 => {}
        DataType::Float32 => {}
        DataType::Float64 => {}
        DataType::Utf8 => {}
        DataType::LargeUtf8 => {}
        DataType::List(v) => is_data_type_supported(v.data_type())?,
        DataType::FixedSizeList(v, _) => is_data_type_supported(v.data_type())?,
        DataType::LargeList(v) => is_data_type_supported(v.data_type())?,
        DataType::Timestamp(_, _) => {}
        _ => Err(GraphError::UnsupportedDataType)?,
    }
    Ok(())
}

pub(crate) fn lift_property<'a: 'b, 'b>(
    idx: usize,
    name: &'a str,
    df: &'b DFChunk,
) -> Box<dyn Iterator<Item = Option<(&'b str, Prop)>> + 'b> {
    let arr = &df.chunk[idx];
    let r = match arr.data_type() {
        DataType::Boolean => {
            let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            iter_as_prop(name, arr.iter())
        }
        DataType::Int32 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
            iter_as_prop(name, arr.iter().map(|i| i.copied()))
        }
        DataType::Int64 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
            iter_as_prop(name, arr.iter().map(|i| i.copied()))
        }
        DataType::UInt8 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u8>>().unwrap();
            iter_as_prop(name, arr.iter().map(|i| i.copied()))
        }
        DataType::UInt16 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u16>>().unwrap();
            iter_as_prop(name, arr.iter().map(|i| i.copied()))
        }
        DataType::UInt32 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u32>>().unwrap();
            iter_as_prop(name, arr.iter().map(|i| i.copied()))
        }
        DataType::UInt64 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u64>>().unwrap();
            iter_as_prop(name, arr.iter().map(|i| i.copied()))
        }
        DataType::Float32 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap();
            iter_as_prop(name, arr.iter().map(|i| i.copied()))
        }
        DataType::Float64 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
            iter_as_prop(name, arr.iter().map(|i| i.copied()))
        }
        DataType::Utf8 => {
            let arr = arr.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            iter_as_prop(name, arr.iter())
        }
        DataType::LargeUtf8 => {
            let arr = arr.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            iter_as_prop(name, arr.iter())
        }
        DataType::List(_) => {
            let arr = arr.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            iter_as_arr_prop(name, arr.iter())
        }
        DataType::FixedSizeList(_, _) => {
            let arr = arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            iter_as_arr_prop(name, arr.iter())
        }
        DataType::LargeList(_) => {
            let arr = arr.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            iter_as_arr_prop(name, arr.iter())
        }
        DataType::Timestamp(timeunit, timezone) => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
            match timezone {
                Some(_) => match timeunit {
                    TimeUnit::Second => {
                        println!("Timestamp(Second, Some({:?})); ", timezone);
                        let r: Box<dyn Iterator<Item = Option<(&'b str, Prop)>> + 'b> =
                            Box::new(arr.iter().map(move |val| {
                                val.map(|v| {
                                    (
                                        name,
                                        Prop::DTime(
                                            DateTime::<Utc>::from_timestamp(*v, 0)
                                                .expect("DateTime conversion failed"),
                                        ),
                                    )
                                })
                            }));
                        r
                    }
                    TimeUnit::Millisecond => {
                        println!("Timestamp(Millisecond, Some({:?})); ", timezone);
                        let r: Box<dyn Iterator<Item = Option<(&'b str, Prop)>> + 'b> =
                            Box::new(arr.iter().map(move |val| {
                                val.map(|v| {
                                    (
                                        name,
                                        Prop::DTime(
                                            DateTime::<Utc>::from_timestamp_millis(*v)
                                                .expect("DateTime conversion failed"),
                                        ),
                                    )
                                })
                            }));
                        r
                    }
                    TimeUnit::Microsecond => {
                        println!("Timestamp(Microsecond, Some({:?})); ", timezone);
                        let r: Box<dyn Iterator<Item = Option<(&'b str, Prop)>> + 'b> =
                            Box::new(arr.iter().map(move |val| {
                                val.map(|v| {
                                    (
                                        name,
                                        Prop::DTime(
                                            DateTime::<Utc>::from_timestamp_micros(*v)
                                                .expect("DateTime conversion failed"),
                                        ),
                                    )
                                })
                            }));
                        r
                    }
                    TimeUnit::Nanosecond => {
                        println!("Timestamp(Nanosecond, Some({:?})); ", timezone);
                        let r: Box<dyn Iterator<Item = Option<(&'b str, Prop)>> + 'b> =
                            Box::new(arr.iter().map(move |val| {
                                val.map(|v| {
                                    (name, Prop::DTime(DateTime::<Utc>::from_timestamp_nanos(*v)))
                                })
                            }));
                        r
                    }
                },
                None => match timeunit {
                    TimeUnit::Second => {
                        println!("Timestamp(Second, None); ");
                        let r: Box<dyn Iterator<Item = Option<(&'b str, Prop)>> + 'b> =
                            Box::new(arr.iter().map(move |val| {
                                val.map(|v| {
                                    (
                                        name,
                                        Prop::NDTime(
                                            DateTime::from_timestamp(*v, 0)
                                                .expect("DateTime conversion failed")
                                                .naive_utc(),
                                        ),
                                    )
                                })
                            }));
                        r
                    }
                    TimeUnit::Millisecond => {
                        println!("Timestamp(Millisecond, None); ");
                        let r: Box<dyn Iterator<Item = Option<(&'b str, Prop)>> + 'b> =
                            Box::new(arr.iter().map(move |val| {
                                val.map(|v| {
                                    (
                                        name,
                                        Prop::NDTime(
                                            DateTime::from_timestamp_millis(*v)
                                                .expect("DateTime conversion failed")
                                                .naive_utc(),
                                        ),
                                    )
                                })
                            }));
                        r
                    }
                    TimeUnit::Microsecond => {
                        println!("Timestamp(Microsecond, None); ");
                        let r: Box<dyn Iterator<Item = Option<(&'b str, Prop)>> + 'b> =
                            Box::new(arr.iter().map(move |val| {
                                val.map(|v| {
                                    (
                                        name,
                                        Prop::NDTime(
                                            DateTime::from_timestamp_micros(*v)
                                                .expect("DateTime conversion failed")
                                                .naive_utc(),
                                        ),
                                    )
                                })
                            }));
                        r
                    }
                    TimeUnit::Nanosecond => {
                        println!("Timestamp(Nanosecond, None); ");
                        let r: Box<dyn Iterator<Item = Option<(&'b str, Prop)>> + 'b> =
                            Box::new(arr.iter().map(move |val| {
                                val.map(|v| {
                                    (
                                        name,
                                        Prop::NDTime(
                                            DateTime::from_timestamp_nanos(*v).naive_utc(),
                                        ),
                                    )
                                })
                            }));
                        r
                    }
                },
            }
        }
        unsupported => panic!("Data type not supported: {:?}", unsupported),
    };

    r
}

pub(crate) fn lift_layer<'a>(
    layer_name: Option<&str>,
    layer_index: Option<usize>,
    df: &'a DFChunk,
) -> Result<Box<dyn Iterator<Item = Option<String>> + 'a>, GraphError> {
    match (layer_name, layer_index) {
        (None, None) => Ok(Box::new(std::iter::repeat(None))),
        (Some(layer_name), None) => Ok(Box::new(std::iter::repeat(Some(layer_name.to_string())))),
        (None, Some(layer_index)) => {
            if let Some(col) = df.utf8::<i32>(layer_index) {
                Ok(Box::new(col.map(|v| v.map(|v| v.to_string()))))
            } else if let Some(col) = df.utf8::<i64>(layer_index) {
                Ok(Box::new(col.map(|v| v.map(|v| v.to_string()))))
            } else {
                Ok(Box::new(std::iter::repeat(None)))
            }
        }
        _ => Err(GraphError::WrongLayerArgs),
    }
}

fn iter_as_prop<'a, T: Into<Prop> + 'a, I: Iterator<Item = Option<T>> + 'a>(
    name: &'a str,
    is: I,
) -> Box<dyn Iterator<Item = Option<(&'a str, Prop)>> + 'a> {
    Box::new(is.map(move |val| val.map(|v| (name, v.into()))))
}

fn iter_as_arr_prop<'a, I: Iterator<Item = Option<Box<dyn Array>>> + 'a>(
    name: &'a str,
    is: I,
) -> Box<dyn Iterator<Item = Option<(&'a str, Prop)>> + 'a> {
    Box::new(is.map(move |val| val.map(|v| (name, arr_as_prop(v)))))
}
