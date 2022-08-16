from collections.abc import Iterable, Iterator

from pyraphtory.interop import register, GenericScalaProxy


@register(name="Iterable")
class IterableScalaProxy(GenericScalaProxy, Iterable):
    """Proxy object that converts scala iterables into python iterables"""
    def __iter__(self):
        return self.iterator()


@register(name="Iterator")
class IteratorScalaProxy(GenericScalaProxy, Iterator):
    """Proxy object that converts scala iterators into python iterators"""

    def __next__(self):
        if self.has_next():
            return self.next()
        else:
            raise StopIteration


