from collections import abc
from pyraphtory.interop import register, GenericScalaProxy, ScalaClassProxy, to_python, to_jvm
import typing as t

A = t.TypeVar("A", covariant=True)
@register(name="Iterable")
class Iterable(GenericScalaProxy, abc.Iterable[A]):
    """Proxy object that converts scala iterables into python iterables"""
    _classname = "scala.collection.Iterable"

    def __iter__(self) -> t.Iterator[A]:
        return self.iterator()

    def __len__(self) -> int:
        return self.size()


@register(name="Iterator")
class Iterator(GenericScalaProxy, abc.Iterator[A]):
    """Proxy object that converts scala iterators into python iterators"""

    _classname = "scala.collection.Iterator"

    def __next__(self) -> A:
        if self.has_next():
            return self.next()
        else:
            raise StopIteration


@register(name="Sequence")
class Sequence(Iterable, abc.Sequence[A]):
    """Proxy object that converts scala sequence into python sequence"""
    def __len__(self) -> int:
        return self.size()

    def __getitem__(self, item: int) -> A:
        if isinstance(item, slice):
            indices = item.indices(len(self))
            if indices[2] != 1:
                # Scala does not support slice with step!
                return [self.apply(i) for i in range(*indices)]
            return self.slice(indices[0], indices[1])
        else:
            if item < 0:
                # Scala does not support negative index
                return self.apply(len(self)+item)
            else:
                return self.apply(item)

    def __contains__(self, item: A) -> bool:
        return self.contains(item)

    def __reversed__(self) -> t.Iterator[A]:
        return self.reverseIterator()

    def index(self, value: A, start: int = None, stop: int = None) -> int:
        if start < 0:
            start = len(self) + start
        if stop < 0:
            stop = len(self) + stop
        if stop is not None:
            return self.take(stop).index(value, start)
        elif start is not None:
            return self.indexOf(value, start)
        else:
            return self.indexOf(value)


K = t.TypeVar("K")
V = t.TypeVar("V", covariant=True)
@register(name="Mapping")
class Mapping(ScalaClassProxy, abc.Mapping[K, V]):
    _classname = "scala.collection.Map"

    def __len__(self) -> int:
        self.size()

    def __iter__(self) -> t.Iterator[K]:
        return self.keys_iterator()

    def __getitem__(self, key: K) -> V:
        try:
            return self.apply(key)
        except Exception:
            raise KeyError(f"missing key {key}")


class List(ScalaClassProxy, Sequence[A]):
    _classname = "scala.collection.immutable.List"

    @classmethod
    def _build_from_python(cls, iterable: t.Iterable[A]):
        builder = cls.new_builder()
        for element in iterable:
            builder.append(element)
        return builder.to_list()


T = t.TypeVar("T")
@register(name="Array")
class Array(GenericScalaProxy, abc.Sequence[T]):
    """Proxy object for wrapping java arrays"""
    _classname = "scala.Array"

    def __getitem__(self, item: int) -> T:
        return to_python(self.jvm[item])

    def __setitem__(self, index: int, value: T) -> None:
        self.jvm[index] = to_jvm(value)

    def __len__(self) -> int:
        return len(self.jvm)


class ScalaNone(ScalaClassProxy):
    """Convert Scala `None` to python `None`"""
    _classname = "scala.None"

    @classmethod
    def _from_jvm(cls, jvm_object: t.Any) -> None:
        return None

    def __new__(cls, jvm_object=None):
        return None


class ScalaSome(ScalaClassProxy, t.Generic[A]):
    _classname = "scala.Some"

    @classmethod
    def _from_jvm(cls, jvm_object):
        return to_python(jvm_object.get())
