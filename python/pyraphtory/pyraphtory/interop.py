import inspect
import re
from collections.abc import Iterable, Mapping
import pyraphtory.proxy as proxy
from py4j.java_gateway import JavaObject, JavaClass
import cloudpickle as pickle

_scala_cache = None


class FunctionWrapper(object):
    def __init__(self, fun=None):
        self._fun = fun
        self.n_args = len(inspect.getfullargspec(fun).args)

    def eval_from_jvm(self, *args):
        return to_jvm(self(*(to_python(v) for v in args)))

    def __call__(self, *args, **kwargs):
        return self._fun(*args, **kwargs)


def set_scala_interop(obj):
    global _scala_cache
    _scala_cache = obj


def _scala():
    global _scala_cache
    if _scala_cache is None:
        from pemja import findClass
        _scala_cache = findClass('com.raphtory.internals.management.PythonInterop')
    return _scala_cache


_method_cache = {}
_wrappers = {}


class logger(object):
    @staticmethod
    def error(msg):
        _scala().logger().error(msg)

    @staticmethod
    def warn(msg):
        _scala().logger().warn(msg)

    @staticmethod
    def info(msg):
        _scala().logger().info(msg)

    @staticmethod
    def debug(msg):
        _scala().logger().debug(msg)

    @staticmethod
    def trace(msg):
        _scala().logger().trace(msg)


def register(cls=None, *, name=None):
    if cls is None:
        return lambda x: register(x, name=name)
    else:
        if name is None:
            name = cls._classname
        if name is None:
            raise ValueError(f"Missing name during registration of {cls!r}")
        _wrappers[name] = cls
        return cls


def is_PyJObject(obj):
    return type(obj).__name__ == "PyJObject" or isinstance(obj, JavaObject) or isinstance(obj, JavaClass)


def snake_to_camel(name: str):
    """
    Convert python snake case names to scala-style camelCase preserving leading underscores
    (multiple intermediate underscores are eliminated).
    """
    parts = re.split(r"([^_])", name, 1)
    rest = parts[2].split("_")
    rest[1:] = [v.capitalize() for v in rest[1:]]
    return "".join(parts[:2] + rest)


def camel_to_snake(name: str):
    return _scala().camel_to_snake(name)


def decode(obj):
    return _scala().decode(obj)


def get_methods(obj):
    name = obj.getClass().getName()
    if name in _method_cache:
        logger.trace(f"Retreiving cached methods for {name!r}")
        return _method_cache[name]
    else:
        logger.trace(f"Finding methods for {name!r}")
        res = _scala().methods(obj)
        _method_cache[name] = res
        logger.trace(f"Methods for {name!r} added to cache")
        return res


def get_wrapper(obj):
    name = obj.getClass().getName()
    logger.trace(f"Retrieving wrapper for {name!r}")
    if name in _wrappers:
        logger.trace(f"Found wrapper for {name!r} based on class name")
    else:
        name = _scala().get_wrapper_str(obj)
        logger.trace(f"Wrapper name is {name!r}")
    wrapper = _wrappers.get(name, proxy.GenericScalaProxy)
    logger.trace(f"Wrapper is {wrapper!r}")
    return wrapper


def to_jvm(value):
    if is_PyJObject(value):
        logger.trace(f"Converting value {value!r}, already PyJObject")
        return decode(value)
    elif isinstance(value, proxy.Algorithm):
        logger.trace(f"Converting value {value!r}, finding object based on algorithm path")
        return find_class(value._path)
    elif isinstance(value, proxy.ScalaProxyBase):
        logger.trace(f"Converting value {value!r}, decoding proxy object")
        return decode(value._jvm_object)
    elif isinstance(value, Mapping):
        logger.trace(f"Converting value {value!r}, decoding as Mapping")
        return decode({to_jvm(k): to_jvm(v) for k, v in value.items()})
    elif callable(value):
        logger.trace(f"Converting value {value!r}, decoding as Function")
        return _wrap_python_function(value)
    elif (isinstance(value, Iterable)
          and not isinstance(value, str)
          and not isinstance(value, bytes)
          and not isinstance(value, bytearray)):
        logger.trace(f"Converting value {value!r}, decoding as Iterable")
        return decode([to_jvm(v) for v in value])
    else:
        logger.trace(f"Converting value {value!r}, primitive value returned unchanged")
        return value


def to_python(obj):
    if is_PyJObject(obj):
        wrapper = get_wrapper(obj)
        logger.trace(f"Calling wrapper with jvm_object={obj}")
        return wrapper(jvm_object=obj)
    else:
        logger.trace(f"Primitive object {obj!r} passed to python unchanged")
        return obj


def find_class(path: str):
    c = _scala().find_class(path)
    return _scala().find_class(path)


def assign_id(s: str):
    return _scala().assign_id(s)


def make_varargs(param):
    return _scala().make_varargs(param)


def _wrap_python_function(fun):
    wrapped = FunctionWrapper(fun)
    pickle_bytes = pickle.dumps(wrapped)
    if wrapped.n_args == 1:
        return to_jvm(proxy.Function1(pickle_bytes))
    elif wrapped.n_args == 2:
        return to_jvm(proxy.Function2(pickle_bytes))
    else:
        raise ValueError("Only functions with 1 or 2 arguments are currently implemented when passing to scala")