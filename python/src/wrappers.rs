use docbrown::core as db_c;
use docbrown::db::perspective;
use docbrown::db::perspective::PerspectiveSet;
use docbrown::db::view_api::vertex::BoxedIter;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::{fmt, i64};

#[derive(FromPyObject, Debug, Clone)]
pub enum Prop {
    Str(String),
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
}

impl fmt::Display for Prop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Prop::Str(value) => write!(f, "{}", value),
            Prop::Bool(value) => write!(f, "{}", value),
            Prop::I64(value) => write!(f, "{}", value),
            Prop::U64(value) => write!(f, "{}", value),
            Prop::F64(value) => write!(f, "{}", value),
        }
    }
}

impl IntoPy<PyObject> for Prop {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            Prop::Str(s) => s.into_py(py),
            Prop::Bool(bool) => bool.into_py(py),
            Prop::I64(i64) => i64.into_py(py),
            Prop::U64(u64) => u64.into_py(py),
            Prop::F64(f64) => f64.into_py(py),
        }
    }
}

impl From<Prop> for db_c::Prop {
    fn from(prop: Prop) -> db_c::Prop {
        match prop {
            Prop::Str(string) => db_c::Prop::Str(string),
            Prop::Bool(bool) => db_c::Prop::Bool(bool),
            Prop::I64(i64) => db_c::Prop::I64(i64),
            Prop::U64(u64) => db_c::Prop::U64(u64),
            Prop::F64(f64) => db_c::Prop::F64(f64),
        }
    }
}

impl From<db_c::Prop> for Prop {
    fn from(prop: db_c::Prop) -> Prop {
        match prop {
            db_c::Prop::Str(string) => Prop::Str(string),
            db_c::Prop::Bool(bool) => Prop::Bool(bool),
            db_c::Prop::I32(i32) => Prop::I64(i32 as i64),
            db_c::Prop::I64(i64) => Prop::I64(i64),
            db_c::Prop::U32(u32) => Prop::U64(u32 as u64),
            db_c::Prop::U64(u64) => Prop::U64(u64),
            db_c::Prop::F64(f64) => Prop::F64(f64),
            db_c::Prop::F32(f32) => Prop::F64(f32 as f64),
        }
    }
}

#[pyclass]
pub struct U64Iter {
    iter: Box<dyn Iterator<Item = u64> + Send>,
}

#[pymethods]
impl U64Iter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<u64> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = u64> + Send>> for U64Iter {
    fn from(value: Box<dyn Iterator<Item = u64> + Send>) -> Self {
        Self { iter: value }
    }
}

#[pyclass]
pub struct NestedU64Iter {
    iter: Box<dyn Iterator<Item = U64Iter> + Send>,
}

#[pymethods]
impl NestedU64Iter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<U64Iter> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = U64Iter> + Send>> for NestedU64Iter {
    fn from(value: Box<dyn Iterator<Item = U64Iter> + Send>) -> Self {
        Self { iter: value }
    }
}

impl From<Box<dyn Iterator<Item = Box<dyn Iterator<Item = u64> + Send>> + Send>> for NestedU64Iter {
    fn from(value: Box<dyn Iterator<Item = Box<dyn Iterator<Item = u64> + Send>> + Send>) -> Self {
        let iter = Box::new(value.map(|iter| iter.into()));
        Self { iter }
    }
}

#[pyclass]
pub struct NestedUsizeIter {
    iter: Box<dyn Iterator<Item = UsizeIter> + Send>,
}

#[pymethods]
impl NestedUsizeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<UsizeIter> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = UsizeIter> + Send>> for NestedUsizeIter {
    fn from(value: Box<dyn Iterator<Item = UsizeIter> + Send>) -> Self {
        Self { iter: value }
    }
}

impl From<Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send>>
    for NestedUsizeIter
{
    fn from(
        value: Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send>,
    ) -> Self {
        let iter = Box::new(value.map(|iter| iter.into()));
        Self { iter }
    }
}

#[pyclass]
pub struct NestedStringIter {
    iter: BoxedIter<StringIter>,
}

#[pymethods]
impl NestedStringIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<StringIter> {
        slf.iter.next()
    }
}

impl From<BoxedIter<BoxedIter<String>>> for NestedStringIter {
    fn from(value: BoxedIter<BoxedIter<String>>) -> Self {
        let iter = Box::new(value.map(|it| it.into()));
        Self { iter }
    }
}

#[pyclass]
pub struct NestedStringVecIter {
    iter: BoxedIter<StringVecIter>,
}

#[pymethods]
impl NestedStringVecIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<StringVecIter> {
        slf.iter.next()
    }
}

impl From<BoxedIter<BoxedIter<Vec<String>>>> for NestedStringVecIter {
    fn from(value: BoxedIter<BoxedIter<Vec<String>>>) -> Self {
        let iter = Box::new(value.map(|it| it.into()));
        Self { iter }
    }
}

#[pyclass]
pub struct NestedBoolIter {
    iter: BoxedIter<BoolIter>,
}

#[pymethods]
impl NestedBoolIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<BoolIter> {
        slf.iter.next()
    }
}

impl From<BoxedIter<BoxedIter<bool>>> for NestedBoolIter {
    fn from(value: BoxedIter<BoxedIter<bool>>) -> Self {
        let iter = Box::new(value.map(|it| it.into()));
        Self { iter }
    }
}

#[pyclass]
pub struct NestedI64Iter {
    iter: BoxedIter<I64Iter>,
}

#[pymethods]
impl NestedI64Iter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<I64Iter> {
        slf.iter.next()
    }
}

impl From<BoxedIter<BoxedIter<i64>>> for NestedI64Iter {
    fn from(value: BoxedIter<BoxedIter<i64>>) -> Self {
        let iter = Box::new(value.map(|it| it.into()));
        Self { iter }
    }
}

#[pyclass]
pub struct NestedOptionI64Iter {
    iter: BoxedIter<OptionI64Iter>,
}

#[pymethods]
impl NestedOptionI64Iter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<OptionI64Iter> {
        slf.iter.next()
    }
}

impl From<BoxedIter<BoxedIter<Option<i64>>>> for NestedOptionI64Iter {
    fn from(value: BoxedIter<BoxedIter<Option<i64>>>) -> Self {
        let iter = Box::new(value.map(|it| it.into()));
        Self { iter }
    }
}

#[pyclass]
pub struct NestedOptionPropIter {
    iter: BoxedIter<OptionPropIter>,
}

#[pymethods]
impl NestedOptionPropIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<OptionPropIter> {
        slf.iter.next()
    }
}

impl From<BoxedIter<BoxedIter<Option<db_c::Prop>>>> for NestedOptionPropIter {
    fn from(value: BoxedIter<BoxedIter<Option<db_c::Prop>>>) -> Self {
        let iter = Box::new(value.map(|it| it.into()));
        Self { iter }
    }
}

#[pyclass]
pub struct NestedPropHistoryIter {
    iter: BoxedIter<PropHistoryIter>,
}

#[pymethods]
impl NestedPropHistoryIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PropHistoryIter> {
        slf.iter.next()
    }
}

impl From<BoxedIter<BoxedIter<Vec<(i64, db_c::Prop)>>>> for NestedPropHistoryIter {
    fn from(value: BoxedIter<BoxedIter<Vec<(i64, db_c::Prop)>>>) -> Self {
        let iter = Box::new(value.map(|it| it.into()));
        Self { iter }
    }
}

#[pyclass]
pub struct NestedPropsIter {
    iter: BoxedIter<PropsIter>,
}

#[pymethods]
impl NestedPropsIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PropsIter> {
        slf.iter.next()
    }
}

impl From<BoxedIter<BoxedIter<HashMap<String, db_c::Prop>>>> for NestedPropsIter {
    fn from(value: BoxedIter<BoxedIter<HashMap<String, db_c::Prop>>>) -> Self {
        let iter = Box::new(value.map(|it| it.into()));
        Self { iter }
    }
}

#[pyclass]
pub struct NestedPropHistoriesIter {
    iter: BoxedIter<PropHistoriesIter>,
}

#[pymethods]
impl NestedPropHistoriesIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PropHistoriesIter> {
        slf.iter.next()
    }
}

impl From<BoxedIter<BoxedIter<HashMap<String, Vec<(i64, db_c::Prop)>>>>>
    for NestedPropHistoriesIter
{
    fn from(value: BoxedIter<BoxedIter<HashMap<String, Vec<(i64, db_c::Prop)>>>>) -> Self {
        let iter = Box::new(value.map(|it| it.into()));
        Self { iter }
    }
}

#[pyclass]
pub struct UsizeIter {
    iter: Box<dyn Iterator<Item = usize> + Send>,
}

#[pymethods]
impl UsizeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<usize> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = usize> + Send>> for UsizeIter {
    fn from(value: Box<dyn Iterator<Item = usize> + Send>) -> Self {
        Self { iter: value }
    }
}

#[pyclass]
pub struct I64Iter {
    iter: Box<dyn Iterator<Item = i64> + Send>,
}

#[pymethods]
impl I64Iter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<i64> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = i64> + Send>> for I64Iter {
    fn from(value: Box<dyn Iterator<Item = i64> + Send>) -> Self {
        Self { iter: value }
    }
}

#[pyclass]
pub struct OptionI64Iter {
    iter: Box<dyn Iterator<Item = Option<i64>> + Send>,
}

#[pymethods]
impl OptionI64Iter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<Option<i64>> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = Option<i64>> + Send>> for OptionI64Iter {
    fn from(value: Box<dyn Iterator<Item = Option<i64>> + Send>) -> Self {
        Self { iter: value }
    }
}

#[pyclass]
pub struct OptionPropIter {
    iter: Box<dyn Iterator<Item = Option<Prop>> + Send>,
}

#[pymethods]
impl OptionPropIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<Option<Prop>> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = Option<db_c::Prop>> + Send>> for OptionPropIter {
    fn from(value: Box<dyn Iterator<Item = Option<db_c::Prop>> + Send>) -> Self {
        Self {
            iter: Box::new(value.map(|p| p.map(|v| v.into()))),
        }
    }
}

#[pyclass]
pub struct PropHistoryIter {
    iter: Box<dyn Iterator<Item = Vec<(i64, Prop)>> + Send>,
}

#[pymethods]
impl PropHistoryIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<Vec<(i64, Prop)>> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = Vec<(i64, db_c::Prop)>> + Send>> for PropHistoryIter {
    fn from(value: Box<dyn Iterator<Item = Vec<(i64, db_c::Prop)>> + Send>) -> Self {
        Self {
            iter: Box::new(value.map(|p| p.into_iter().map(|(t, v)| (t, v.into())).collect())),
        }
    }
}

#[pyclass]
pub struct PropsIter {
    iter: Box<dyn Iterator<Item = HashMap<String, Prop>> + Send>,
}

#[pymethods]
impl PropsIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<HashMap<String, Prop>> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = HashMap<String, db_c::Prop>> + Send>> for PropsIter {
    fn from(value: Box<dyn Iterator<Item = HashMap<String, db_c::Prop>> + Send>) -> Self {
        Self {
            iter: Box::new(value.map(|p| p.into_iter().map(|(k, v)| (k, v.into())).collect())),
        }
    }
}

#[pyclass]
pub struct PropHistoriesIter {
    iter: Box<dyn Iterator<Item = HashMap<String, Vec<(i64, Prop)>>> + Send>,
}

#[pymethods]
impl PropHistoriesIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<HashMap<String, Vec<(i64, Prop)>>> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = HashMap<String, Vec<(i64, db_c::Prop)>>> + Send>>
    for PropHistoriesIter
{
    fn from(
        value: Box<dyn Iterator<Item = HashMap<String, Vec<(i64, db_c::Prop)>>> + Send>,
    ) -> Self {
        Self {
            iter: Box::new(value.map(|p| {
                p.into_iter()
                    .map(|(k, v)| (k, v.into_iter().map(|(t, p)| (t, p.into())).collect()))
                    .collect()
            })),
        }
    }
}

#[pyclass]
pub struct StringVecIter {
    iter: Box<dyn Iterator<Item = Vec<String>> + Send>,
}

#[pymethods]
impl StringVecIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<Vec<String>> {
        slf.iter.next()
    }
}

impl From<BoxedIter<Vec<String>>> for StringVecIter {
    fn from(value: BoxedIter<Vec<String>>) -> Self {
        Self { iter: value }
    }
}

#[pyclass]
pub struct BoolIter {
    iter: BoxedIter<bool>,
}

#[pymethods]
impl BoolIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<bool> {
        slf.iter.next()
    }
}

impl From<BoxedIter<bool>> for BoolIter {
    fn from(value: BoxedIter<bool>) -> Self {
        Self { iter: value }
    }
}

#[pyclass]
pub struct StringIter {
    iter: Box<dyn Iterator<Item = String> + Send>,
}

#[pymethods]
impl StringIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<String> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = String> + Send>> for StringIter {
    fn from(value: Box<dyn Iterator<Item = String> + Send>) -> Self {
        Self { iter: value }
    }
}

#[derive(Clone)]
#[pyclass(name = "Perspective")]
pub struct PyPerspective {
    pub start: Option<i64>,
    pub end: Option<i64>,
}

#[pymethods]
impl PyPerspective {
    #[new]
    #[pyo3(signature = (start=None, end=None))]
    fn new(start: Option<i64>, end: Option<i64>) -> Self {
        PyPerspective { start, end }
    }

    #[staticmethod]
    #[pyo3(signature = (step, start=None, end=None))]
    fn expanding(step: u64, start: Option<i64>, end: Option<i64>) -> PyPerspectiveSet {
        PyPerspectiveSet {
            ps: perspective::Perspective::expanding(step, start, end),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (window, step=None, start=None, end=None))]
    fn rolling(
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyPerspectiveSet {
        PyPerspectiveSet {
            ps: perspective::Perspective::rolling(window, step, start, end),
        }
    }
}

impl From<perspective::Perspective> for PyPerspective {
    fn from(value: perspective::Perspective) -> Self {
        PyPerspective {
            start: value.start,
            end: value.end,
        }
    }
}

impl From<PyPerspective> for perspective::Perspective {
    fn from(value: PyPerspective) -> Self {
        perspective::Perspective {
            start: value.start,
            end: value.end,
        }
    }
}

#[pyclass(name = "PerspectiveSet")]
#[derive(Clone)]
pub struct PyPerspectiveSet {
    pub(crate) ps: PerspectiveSet,
}
