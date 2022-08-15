from dataclasses import dataclass, asdict
from typing import List, Optional
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from pemja import findClass
import re


def _is_PyJObject(obj):
    return type(obj).__name__ == "PyJObject"


class _Interop(object):
    _interop = findClass('com.raphtory.internals.management.PythonInterop')
    logger = _interop.logger()

    @staticmethod
    def snake_to_camel(name: str):
        """
        Convert python snake case names to scala-style camelCase preserving leading underscores
        (multiple intermediate underscores are eliminated).
        """
        parts = re.split(r"([^_])", name, 1)
        rest = parts[2].split("_")
        rest[1:] = [v.capitalize() for v in rest[1:]]
        return "".join(parts[:2] + rest)

    @classmethod
    def decode(cls, obj):
        return cls._interop.decode(obj)

    @classmethod
    def to_jvm(cls, value):

        if _is_PyJObject(value):
            cls.logger.trace(f"Converting value {value!r}, already PyJObject")
            return value
        elif isinstance(value, GenericScalaProxy):
            cls.logger.trace(f"Converting value {value!r}, decoding proxy object")
            return cls.decode(value._jvm_object)
        elif isinstance(value, Mapping):
            cls.logger.trace(f"Converting value {value!r}, decoding as Mapping")
            return cls.decode({cls.to_jvm(k): cls.to_jvm(v) for k, v in value.items()})
        elif isinstance(value, Iterable) and not isinstance(value, str):
            cls.logger.trace(f"Converting value {value!r}, decoding as Iterable")
            return cls.decode([cls.to_jvm(v) for v in value])
        else:
            cls.logger.trace(f"Converting value {value!r}, primitive value returned unchanged")
            return value


class GenericScalaProxy(object):
    def __init__(self, jvm_object=None):
        self._jvm_object = jvm_object

    def __getattr__(self, name):
        _name = _Interop.snake_to_camel(name)
        try:
            return GenericScalaProxy(getattr(self._jvm_object, _name))
        except Exception as e:
            raise AttributeError(f"{self!r} object has no method {name!r}")

    def __repr__(self):
        try:
            return self._jvm_object.toString()
        except AttributeError as e:
            return repr(self._jvm_object)

    """
    Underlying proxy object without any python methods. Use this to call the java method when creating a wrapper 
    method in python.
    """
    @property
    def jvm(self):
        return GenericScalaProxy(self._jvm_object)

    def __call__(self, *args, **kwargs):
        args = [_Interop.to_jvm(v) for v in args]
        kwargs = {k: _Interop.to_jvm(v) for k, v in kwargs.items()}
        _Interop.logger.trace(f"Calling {self!r} with *args={args!r} and **kwargs={kwargs!r}")
        res = self._jvm_object(*args,
                               **kwargs)
        t_name = type(res).__name__
        if t_name == "PyJObject":
            return GenericScalaProxy(self._jvm_object(*args,
                                                      **kwargs))
        else:
            return res


class ConstructableScalaProxy(GenericScalaProxy, ABC):
    _classname: str = ""

    @classmethod
    def _constructor(cls):
        _Interop.logger.trace(f"trying to construct {cls._classname}")
        return GenericScalaProxy(findClass(cls._classname))

    @classmethod
    def _construct_from_python(cls, *args, **kwargs):
        return cls._constructor().apply(*args, **kwargs)._jvm_object

    def __init__(self, *args, jvm_object=None, **kwargs):
        if jvm_object is None:
            jvm_object = self._construct_from_python(*args, **kwargs)
        super().__init__(jvm_object)


class Type(ConstructableScalaProxy):
    _classname = "com.raphtory.api.input.Type"

    # @classmethod
    # def _construct_from_python(cls, name):
    #     return cls._constructor().apply(name)

    def __init__(self, name: str):
        super(Type, self).__init__(name)


class StringProperty(ConstructableScalaProxy):
    _classname = "com.raphtory.api.input.StringProperty"

    # @classmethod
    # def _construct_from_python(cls, key, value):
    #     return cls._constructor().apply(key, value)

    def __init__(self, key: str, value: str):
        super(StringProperty, self).__init__(key, value)


class ImmutableProperty(StringProperty):
    _classname = "com.raphtory.api.input.ImmutableProperty"


class Properties(ConstructableScalaProxy):
    _classname = "com.raphtory.api.input.Properties"

    @classmethod
    def _construct_from_python(cls, *args):
        return cls._constructor().apply(args)._jvm_object

    def __init__(self, *args):
        super(Properties, self).__init__(*args)


class BaseBuilder(GenericScalaProxy):
    def __init__(self):
        super().__init__(None)

    # TODO: This should hopefully not be necessary soon and we can construct it normally
    def _set_jvm_builder(self, jvm_builder):
        self._jvm_object = jvm_builder

    def parse_tuple(self, line: str):
        pass

    def add_vertex(self, timestamp: int, src_id: int, props: Properties, tpe: str, index=None):
        if index is None:
            index = self.index()
        self.jvm.add_vertex(timestamp, src_id, props, Type(tpe), index)

    def add_edge(self, timestamp: int, src_id: int, tar_id: int, props: Properties, tpe: str, index=None):
        if index is None:
            index = self.index()
        self.jvm.add_edge(timestamp, src_id, tar_id, props, Type(tpe), index)

    @staticmethod
    def assign_id(s: str):
        return _Interop._interop.assignId(s)
