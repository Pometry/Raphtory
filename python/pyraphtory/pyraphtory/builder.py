from dataclasses import dataclass, asdict
from typing import List, Optional
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from collections import defaultdict
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
    def camel_to_snake(cls, name: str):
        return cls._interop.camel_to_snake(name)

    @classmethod
    def decode(cls, obj):
        return cls._interop.decode(obj)

    @classmethod
    def to_jvm(cls, value):
        if _is_PyJObject(value):
            cls.logger.trace(f"Converting value {value!r}, already PyJObject")
            return cls.decode(value)
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

    @staticmethod
    def to_python(obj):
        if _is_PyJObject(obj):
            return GenericScalaProxy(obj)
        else:
            return obj


class GenericScalaProxy(object):
    _classname = None

    def __init__(self, jvm_object=None):
        self._jvm_object = jvm_object
        self._methods = None

    def _get_classname(self):
        if self._classname is None:
            self._classname = self._jvm_object.getClass().getCanonicalName()
        return self._classname

    @property
    def _method_dict(self):
        if self._methods is None:
            self._methods = defaultdict(list)
            _Interop.logger.trace(f"Getting methods for {self._jvm_object}")
            self._methods = _Interop._interop.methods(self._get_classname())
        return self._methods

    def __getattr__(self, name):
        _Interop.logger.trace(f"__getattr__ called for {self!r} with {name!r}")
        if self._method_dict.contains(name):
            return GenericMethodProxy(name, self._jvm_object, self._method_dict.apply(name))
        else:
            raise AttributeError(f"{self!r} object has no method {name!r}")

    def __repr__(self):
        try:
            return self._jvm_object.toString()
        except Exception as e:
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
        return _Interop.to_python(self._jvm_object(*args, **kwargs))


class GenericMethodProxy(object):
    def __init__(self, name: str, jvm_object, methods):
        self.name = name
        self._jvm_object = jvm_object
        self._methods = methods

    def __call__(self, *args, **kwargs):
        args = [_Interop.to_jvm(v) for v in args]
        kwargs = {k: _Interop.to_jvm(v) for k, v in kwargs.items()}
        _Interop.logger.trace(f"Trying to call method {self.name} with arguments {args=} and {kwargs=}")
        for method in self._methods:
            try:
                extra_args = []
                parameters = method.parameters()
                _Interop.logger.trace(f"Parmeters for candidate are {parameters}")
                defaults = method.defaults()
                _Interop.logger.trace(f"Defaults for candidate are {defaults}")
                n = method.n()
                _Interop.logger.trace(f"Number of parameters for candidate is {n}")
                kwargs_used = 0
                if len(args) + len(kwargs) > n:
                    raise ValueError("Too many arguments")
                if len(args) < n:
                    for i in range(len(args), n):
                        param = parameters[i]
                        if param in kwargs:
                            extra_args.append(kwargs[param])
                            kwargs_used += 1
                        elif defaults.contains(i):
                            extra_args.append(getattr(self._jvm_object, defaults.apply(i))())
                        else:
                            raise ValueError(f"Missing value for parameter {param}")
                if kwargs_used == len(kwargs):
                    return _Interop.to_python(getattr(self._jvm_object, method.name())(*args, *extra_args))
                else:
                    raise ValueError(f"Not all kwargs could be applied")
            except Exception as e:
                _Interop.logger.trace(f"Call failed with exception {e}")
        raise ValueError(f"No matching implementation of method {self.name} with arguments {args=} and {kwargs=}")


class ConstructableScalaProxy(GenericScalaProxy, ABC):
    _classname: str = ""

    @classmethod
    def _constructor(cls):
        _Interop.logger.trace(f"trying to construct {cls._classname}")
        c = findClass(cls._classname)
        _Interop.logger.trace(f"found {c}")
        obj = GenericScalaProxy(findClass(cls._classname))
        obj._classname = cls._classname
        return obj

    @classmethod
    def _construct_from_python(cls, *args, **kwargs):
        return _Interop.to_jvm(cls._constructor().apply(*args, **kwargs))

    def __init__(self, *args, jvm_object=None, **kwargs):
        if jvm_object is None:
            jvm_object = self._construct_from_python(*args, **kwargs)
        super().__init__(jvm_object)


class Type(ConstructableScalaProxy):
    _classname = "com.raphtory.api.input.Type"


class StringProperty(ConstructableScalaProxy):
    _classname = "com.raphtory.api.input.StringProperty"


class ImmutableProperty(ConstructableScalaProxy):
    _classname = "com.raphtory.api.input.ImmutableProperty"


class Properties(ConstructableScalaProxy):
    _classname = "com.raphtory.api.input.Properties"

    def __init__(self, *args):
        super(Properties, self).__init__(args)


class BaseBuilder(GenericScalaProxy):
    def __init__(self):
        _Interop.logger.trace("initialising GraphBuilder")
        super().__init__(None)

    # TODO: This should hopefully not be necessary soon and we can construct it normally
    def _set_jvm_builder(self, jvm_builder):
        self._jvm_object = jvm_builder

    def parse_tuple(self, line: str):
        pass

    def add_vertex(self, timestamp: int, src_id: int, props: Properties, tpe: str, index=None):
        if index is None:
            index = self.index()
        self.jvm.add_vertex(timestamp, src_id, props, Type(tpe))

    def add_edge(self, timestamp: int, src_id: int, tar_id: int, props: Properties, tpe: str, index=None):
        if index is None:
            index = self.index()
        self.jvm.add_edge(timestamp, src_id, tar_id, props, Type(tpe), index)

    @staticmethod
    def assign_id(s: str):
        return _Interop._interop.assign_id(s)
