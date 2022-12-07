from collections import abc
from pyraphtory.interop import register, GenericScalaProxy, ScalaClassProxy, to_python, to_jvm


@register(name="Iterable")
class IterableScalaProxy(GenericScalaProxy, abc.Iterable):
    """Proxy object that converts scala iterables into python iterables"""
    def __iter__(self):
        return self.iterator()

    def __len__(self):
        return self.size()


@register(name="Iterator")
class IteratorScalaProxy(GenericScalaProxy, abc.Iterator):
    """Proxy object that converts scala iterators into python iterators"""

    def __next__(self):
        if self.has_next():
            return self.next()
        else:
            raise StopIteration


@register(name="Sequence")
class SequenceScalaProxy(IterableScalaProxy, abc.Sequence):
    """Proxy object that converts scala sequence into python sequence"""
    def __len__(self):
        return self.size()

    def __getitem__(self, item):
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

    def __contains__(self, item):
        return self.contains(item)

    def __reversed__(self):
        return self.reverseIterator()

    def index(self, value, start: int = None, stop: int = None):
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


@register(name="Map")
class Map(ScalaClassProxy, abc.Mapping):
    _classname = "scala.collection.Map"

    def __len__(self) -> int:
        self.size()

    def __iter__(self):
        return self.keys_iterator()

    def __getitem__(self, key):
        try:
            return self.apply(key)
        except Exception:
            raise KeyError(f"missing key {key}")


class List(ScalaClassProxy, SequenceScalaProxy):
    _classname = "scala.collection.immutable.List"

    @classmethod
    def _build_from_python(cls, iterable):
        builder = cls.new_builder()
        for element in iterable:
            builder.append(element)
        return builder.to_list()


@register(name="Array")
class Array(GenericScalaProxy, abc.Sequence):
    """Proxy object for wrapping java arrays"""
    def __getitem__(self, item):
        return to_python(self.jvm[item])

    def __setitem__(self, index, value):
        self.jvm[index] = to_jvm(value)

    def __len__(self):
        return len(self.jvm)


class ScalaNone(ScalaClassProxy):
    """Convert Scala `None` to python `None`"""
    _classname = "scala.None"

    @classmethod
    def _from_jvm(cls, jvm_object):
        return None

    def __new__(cls, jvm_object=None):
        return None


class ScalaSome(ScalaClassProxy):
    _classname = "scala.Some"

    @classmethod
    def _from_jvm(cls, jvm_object):
        return to_python(jvm_object.get())
