use arrow2::{
    array::{Array, BooleanArray, FixedSizeListArray, ListArray, PrimitiveArray, Utf8Array},
    datatypes::{DataType, TimeUnit},
};
use chrono::{DateTime, Utc};

use crate::{
    core::{utils::errors::GraphError, IntoPropList},
    prelude::Prop,
    python::graph::pandas::dataframe::PretendDF,
};

pub struct PropIter<'a> {
    inner: Box<dyn Iterator<Item = Vec<(&'a str, Prop)>> + 'a>,
}

impl<'a> Iterator for PropIter<'a> {
    type Item = Vec<(&'a str, Prop)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub(crate) fn get_prop_rows<'a>(
    df: &'a PretendDF,
    props: Option<Vec<&'a str>>,
    const_props: Option<Vec<&'a str>>,
) -> Result<(PropIter<'a>, PropIter<'a>), GraphError> {
    let prop_iter = combine_properties(props, df)?;
    let const_prop_iter = combine_properties(const_props, df)?;
    Ok((prop_iter, const_prop_iter))
}

fn combine_properties<'a>(
    props: Option<Vec<&'a str>>,
    df: &'a PretendDF,
) -> Result<PropIter<'a>, GraphError> {
    let iter = props
        .unwrap_or_default()
        .into_iter()
        .map(|name| lift_property(name, df))
        .reduce(|i1, i2| {
            let i1 = i1?;
            let i2 = i2?;
            Ok(Box::new(i1.zip(i2).map(|(mut v1, v2)| {
                v1.extend(v2);
                v1
            })))
        })
        .unwrap_or_else(|| Ok(Box::new(std::iter::repeat(vec![]))));

    Ok(PropIter { inner: iter? })
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

fn validate_data_types(dt: &DataType) -> Result<(), GraphError> {
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
        DataType::List(v) => validate_data_types(v.data_type())?,
        DataType::FixedSizeList(v, _) => validate_data_types(v.data_type())?,
        DataType::LargeList(v) => validate_data_types(v.data_type())?,
        DataType::Timestamp(_, _) => {}
        _ => Err(GraphError::UnsupportedDataType)?,
    }
    Ok(())
}

pub(crate) fn lift_property<'a: 'b, 'b>(
    name: &'a str,
    df: &'b PretendDF,
) -> Result<Box<dyn Iterator<Item = Vec<(&'b str, Prop)>> + 'b>, GraphError> {
    let idx = df
        .names
        .iter()
        .position(|n| n == name)
        .ok_or_else(|| GraphError::ColumnDoesNotExist(name.to_string()))?;

    if let Some(first_chunk) = df.arrays.get(0) {
        validate_data_types(first_chunk[idx].data_type())?;
    }

    let r = df.arrays.iter().flat_map(move |arr| {
        let arr: &Box<dyn Array> = &arr[idx];
        match arr.data_type() {
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
                            let r: Box<dyn Iterator<Item = Vec<(&'b str, Prop)>> + 'b> =
                                Box::new(arr.iter().map(move |val| {
                                    val.into_iter()
                                        .map(|v| {
                                            (
                                                name,
                                                Prop::DTime(
                                                    DateTime::<Utc>::from_timestamp(*v, 0)
                                                        .expect("DateTime conversion failed"),
                                                ),
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                }));
                            r
                        }
                        TimeUnit::Millisecond => {
                            println!("Timestamp(Millisecond, Some({:?})); ", timezone);
                            let r: Box<dyn Iterator<Item = Vec<(&'b str, Prop)>> + 'b> =
                                Box::new(arr.iter().map(move |val| {
                                    val.into_iter()
                                        .map(|v| {
                                            (
                                                name,
                                                Prop::DTime(
                                                    DateTime::<Utc>::from_timestamp_millis(*v)
                                                        .expect("DateTime conversion failed"),
                                                ),
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                }));
                            r
                        }
                        TimeUnit::Microsecond => {
                            println!("Timestamp(Microsecond, Some({:?})); ", timezone);
                            let r: Box<dyn Iterator<Item = Vec<(&'b str, Prop)>> + 'b> =
                                Box::new(arr.iter().map(move |val| {
                                    val.into_iter()
                                        .map(|v| {
                                            (
                                                name,
                                                Prop::DTime(
                                                    DateTime::<Utc>::from_timestamp_micros(*v)
                                                        .expect("DateTime conversion failed"),
                                                ),
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                }));
                            r
                        }
                        TimeUnit::Nanosecond => {
                            println!("Timestamp(Nanosecond, Some({:?})); ", timezone);
                            let r: Box<dyn Iterator<Item = Vec<(&'b str, Prop)>> + 'b> =
                                Box::new(arr.iter().map(move |val| {
                                    val.into_iter()
                                        .map(|v| {
                                            (
                                                name,
                                                Prop::DTime(DateTime::<Utc>::from_timestamp_nanos(
                                                    *v,
                                                )),
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                }));
                            r
                        }
                    },
                    None => match timeunit {
                        TimeUnit::Second => {
                            println!("Timestamp(Second, None); ");
                            let r: Box<dyn Iterator<Item = Vec<(&'b str, Prop)>> + 'b> =
                                Box::new(arr.iter().map(move |val| {
                                    val.into_iter()
                                        .map(|v| {
                                            (
                                                name,
                                                Prop::NDTime(
                                                    DateTime::from_timestamp(*v, 0)
                                                        .expect("DateTime conversion failed")
                                                        .naive_utc(),
                                                ),
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                }));
                            r
                        }
                        TimeUnit::Millisecond => {
                            println!("Timestamp(Millisecond, None); ");
                            let r: Box<dyn Iterator<Item = Vec<(&'b str, Prop)>> + 'b> =
                                Box::new(arr.iter().map(move |val| {
                                    val.into_iter()
                                        .map(|v| {
                                            (
                                                name,
                                                Prop::NDTime(
                                                    DateTime::from_timestamp_millis(*v)
                                                        .expect("DateTime conversion failed")
                                                        .naive_utc(),
                                                ),
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                }));
                            r
                        }
                        TimeUnit::Microsecond => {
                            println!("Timestamp(Microsecond, None); ");
                            let r: Box<dyn Iterator<Item = Vec<(&'b str, Prop)>> + 'b> =
                                Box::new(arr.iter().map(move |val| {
                                    val.into_iter()
                                        .map(|v| {
                                            (
                                                name,
                                                Prop::NDTime(
                                                    DateTime::from_timestamp_micros(*v)
                                                        .expect("DateTime conversion failed")
                                                        .naive_utc(),
                                                ),
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                }));
                            r
                        }
                        TimeUnit::Nanosecond => {
                            println!("Timestamp(Nanosecond, None); ");
                            let r: Box<dyn Iterator<Item = Vec<(&'b str, Prop)>> + 'b> =
                                Box::new(arr.iter().map(move |val| {
                                    val.into_iter()
                                        .map(|v| {
                                            (
                                                name,
                                                Prop::NDTime(
                                                    DateTime::from_timestamp_nanos(*v).naive_utc(),
                                                ),
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                }));
                            r
                        }
                    },
                }
            }
            unsupported => panic!("Data type not supported: {:?}", unsupported),
        }
    });

    Ok(Box::new(r))
}

pub(crate) fn lift_layer<'a, S: AsRef<str>>(
    layer: Option<S>,
    layer_in_df: bool,
    df: &'a PretendDF,
) -> Box<dyn Iterator<Item = Option<String>> + 'a> {
    if let Some(layer) = layer {
        if layer_in_df {
            if let Some(col) = df.utf8::<i32>(layer.as_ref()) {
                Box::new(col.map(|v| v.map(|v| v.to_string())))
            } else if let Some(col) = df.utf8::<i64>(layer.as_ref()) {
                Box::new(col.map(|v| v.map(|v| v.to_string())))
            } else {
                Box::new(std::iter::repeat(None))
            }
        } else {
            Box::new(std::iter::repeat(Some(layer.as_ref().to_string())))
        }
    } else {
        Box::new(std::iter::repeat(None))
    }
}

fn iter_as_prop<'a, T: Into<Prop> + 'a, I: Iterator<Item = Option<T>> + 'a>(
    name: &'a str,
    is: I,
) -> Box<dyn Iterator<Item = Vec<(&'a str, Prop)>> + 'a> {
    Box::new(is.map(move |val| {
        val.into_iter()
            .map(|v| (name, (v).into()))
            .collect::<Vec<_>>()
    }))
}

fn iter_as_arr_prop<'a, I: Iterator<Item = Option<Box<dyn Array>>> + 'a>(
    name: &'a str,
    is: I,
) -> Box<dyn Iterator<Item = Vec<(&'a str, Prop)>> + 'a> {
    Box::new(is.map(move |val| {
        val.into_iter()
            .map(|v| (name, arr_as_prop(v)))
            .collect::<Vec<_>>()
    }))
}
