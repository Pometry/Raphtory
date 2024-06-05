use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    cmp::Ordering,
    fmt,
    fmt::{Display, Formatter},
    ops::Deref,
    sync::Arc,
};

use pyo3::{FromPyObject, IntoPy, PyAny, PyObject, PyResult, Python, ToPyObject};

#[derive(Clone, Debug, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct ArcStr(pub Arc<str>);

impl Display for ArcStr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl<T: Into<Arc<str>>> From<T> for ArcStr {
    fn from(value: T) -> Self {
        ArcStr(value.into())
    }
}

impl From<ArcStr> for String {
    fn from(value: ArcStr) -> Self {
        value.to_string()
    }
}

impl From<&ArcStr> for String {
    fn from(value: &ArcStr) -> Self {
        value.clone().into()
    }
}

impl Deref for ArcStr {
    type Target = Arc<str>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Borrow<str> for ArcStr {
    #[inline]
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}

impl<T> AsRef<T> for ArcStr
where
    T: ?Sized,
    <ArcStr as Deref>::Target: AsRef<T>,
{
    fn as_ref(&self) -> &T {
        self.deref().as_ref()
    }
}

impl<T: Borrow<str> + ?Sized> PartialEq<T> for ArcStr {
    fn eq(&self, other: &T) -> bool {
        <ArcStr as Borrow<str>>::borrow(self).eq(other.borrow())
    }
}

impl<T: Borrow<str>> PartialOrd<T> for ArcStr {
    fn partial_cmp(&self, other: &T) -> Option<Ordering> {
        <ArcStr as Borrow<str>>::borrow(self).partial_cmp(other.borrow())
    }
}

impl IntoPy<PyObject> for ArcStr {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.into_py(py)
    }
}

impl ToPyObject for ArcStr {
    fn to_object(&self, py: Python) -> PyObject {
        self.0.to_string().to_object(py)
    }
}

impl<'source> FromPyObject<'source> for ArcStr {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        ob.extract::<String>().map(|v| v.into())
    }
}

pub trait OptionAsStr<'a> {
    fn as_str(self) -> Option<&'a str>;
}

impl<'a, O: AsRef<str> + 'a> OptionAsStr<'a> for &'a Option<O> {
    fn as_str(self) -> Option<&'a str> {
        self.as_ref().map(|s| s.as_ref())
    }
}

impl<'a, O: AsRef<str> + 'a> OptionAsStr<'a> for Option<&'a O> {
    fn as_str(self) -> Option<&'a str> {
        self.map(|s| s.as_ref())
    }
}
