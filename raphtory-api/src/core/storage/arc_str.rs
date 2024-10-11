use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    cmp::Ordering,
    fmt,
    fmt::{Display, Formatter},
    ops::Deref,
    sync::Arc,
};

#[derive(Clone, Debug, Eq, Ord, Hash, Default, Serialize, Deserialize)]
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
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl Borrow<str> for ArcStr {
    #[inline]
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}

impl AsRef<str> for ArcStr {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
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

#[cfg(test)]
mod test_arc_str {
    use crate::core::storage::arc_str::{ArcStr, OptionAsStr};
    use std::sync::Arc;

    #[test]
    fn can_compare_with_str() {
        let test: ArcStr = "test".into();
        assert_eq!(test, "test");
        assert_eq!(test, "test".to_string());
        assert_eq!(test, Arc::from("test"));
        assert_eq!(&test, &"test".to_string())
    }

    #[test]
    fn test_option_conv() {
        let test: Option<ArcStr> = Some("test".into());

        let opt_str = test.as_str();
        assert_eq!(opt_str, Some("test"));

        let test_ref = test.as_ref();
        let opt_str = test_ref.as_str();
        assert_eq!(opt_str, Some("test"));

        let test = Some("test".to_string());
        let opt_str = test.as_str();
        assert_eq!(opt_str, Some("test"));
    }
}
