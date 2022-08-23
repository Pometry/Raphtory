from abc import abstractmethod
from collections import abc
from typing import overload

from pyraphtory.interop import register, GenericScalaProxy, ScalaClassProxy


@register(name="Iterable")
class IterableScalaProxy(GenericScalaProxy, abc.Iterable):
    """Proxy object that converts scala iterables into python iterables"""
    def __iter__(self):
        return self.iterator()


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
            return self.apply(item)

    def __contains__(self, item):
        return self.contains(item)

    def __reversed__(self):
        return self.reverseIterator()

    def index(self, value, start: int = None, stop: int = None):
        if stop is not None:
            return self.take(stop).index(value, start)
        elif start is not None:
            return self.indexOf(value, start)
        else:
            return self.indexOf(value)


class List(ScalaClassProxy, SequenceScalaProxy):
    _classname = "scala.collection.immutable.List"

    @classmethod
    def _build_from_python(cls, iterable):
        builder = cls.new_builder()
        for element in iterable:
            builder.append(element)
        return builder.to_list()
