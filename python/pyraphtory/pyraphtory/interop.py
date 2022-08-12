import inspect
import re
from collections.abc import Iterable, Mapping
import pyraphtory.proxy as proxy
from py4j.java_gateway import JavaObject, JavaClass
import cloudpickle as pickle
from functools import cached_property


def register(cls=None, *, name=None):
    """class decorator for registering wrapper classes.

    Specify a 'name' keyword argument to give the wrapper a name to register it for a range of different types.
    A corresponding match needs to be added on the scala side for the name to have an effect.
    """
    if cls is None:
        return lambda x: register(x, name=name)
    else:
        if name is None:
            name = cls._classname
        if name is None:
            raise ValueError(f"Missing name during registration of {cls!r}")
        _wrappers[name] = cls
        return cls


def set_scala_interop(obj):
    """Provide an object for the scala interop interface (used when initialising from py4j)"""
    global _scala
    _scala.set_interop(obj)


_method_cache = {}
_wrappers = {}


class Scala(object):
    """Class used to lazily initialise the scala interop to avoid import errors before the java connection is established"""
    @cached_property
    def scala(self):
        from pemja import findClass
        return findClass('com.raphtory.internals.management.PythonInterop')

    def set_interop(self, obj):
        """override the default lookup property for initialisation with py4j"""
        self.__dict__["scala"] = obj


_scala = Scala()


def is_PyJObject(obj):
    """Needed because Pemja objects do not support isinstance"""
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
    """Convert scala-style camel-case names to python style snake case"""
    return _scala.scala.camel_to_snake(name)


def decode(obj):
    """call scala decode function to deal with converting java to scala collections"""
    return _scala.scala.decode(obj)


def get_methods(obj):
    """look up methods for a java object"""
    name = obj.getClass().getName()
    if name in _method_cache:
        logger.trace(f"Retreiving cached methods for {name!r}")
        return _method_cache[name]
    else:
        logger.trace(f"Finding methods for {name!r}")
        res = _scala.scala.methods(obj)
        _method_cache[name] = res
        logger.trace(f"Methods for {name!r} added to cache")
        return res


def get_wrapper(obj):
    """get wrapper class for a java object"""
    name = obj.getClass().getName()
    logger.trace(f"Retrieving wrapper for {name!r}")
    try:
        wrapper = _wrappers[name]
        logger.trace(f"Found wrapper for {name!r} based on class name")
    except KeyError:
        # Create a new base class for the jvm wrapper and add methods
        # (note this registers the wrapper as _classname is defined)
        base = type(name + "_jvm", (proxy.GenericScalaProxy,), {"_classname": name})
        base._init_methods(obj)
        # Check if a special wrapper class is registered for the object
        wrap_name = _scala.scala.get_wrapper_str(obj)
        if wrap_name in _wrappers:
            # Add special wrapper class to the top of the mro such that method overloads work
            wrapper = type(name, (_wrappers[wrap_name], base), {"_classname": name})
        else:
            # No special wrapper registered, can use base wrapper directly
            wrapper = base
        logger.trace(f"New wrapper created for {name!r}")
    logger.trace(f"Wrapper is {wrapper!r}")
    return wrapper


def to_jvm(value):
    """convert wrapped object to underlying jvm representation"""
    if is_PyJObject(value):
        logger.trace(f"Converting value {value!r}, already PyJObject")
        return value
    elif isinstance(value, proxy.ScalaProxyBase):
        logger.trace(f"Converting value {value!r}, decoding proxy object")
        if value._jvm_object is None:
            return find_class(value._classname)
        else:
            return value._jvm_object
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
    """convert jvm object to python by wrapping it if needed"""
    if is_PyJObject(obj):
        wrapper = get_wrapper(obj)
        logger.trace(f"Calling wrapper with jvm_object={obj}")
        return wrapper(jvm_object=obj)
    else:
        logger.trace(f"Primitive object {obj!r} passed to python unchanged")
        return obj


def find_class(path: str):
    """get the scala companion object instance for a class path"""
    return _scala.scala.find_class(path)


def assign_id(s: str):
    """call the asign_id function (used by graph builder)"""
    return _scala.scala.assign_id(s)


def make_varargs(param):
    """convert parameter list to varargs-friendly array"""
    return _scala.scala.make_varargs(param)


def _wrap_python_function(fun):
    """take a python function and turn it into a scala function"""
    wrapped = FunctionWrapper(fun)
    pickle_bytes = pickle.dumps(wrapped)
    if wrapped.n_args == 1:
        return to_jvm(proxy.Function1(pickle_bytes))
    elif wrapped.n_args == 2:
        return to_jvm(proxy.Function2(pickle_bytes))
    else:
        raise ValueError("Only functions with 1 or 2 arguments are currently implemented when passing to scala")


class Logger(object):
    """Wrapper for the java logger"""
    @cached_property
    def logger(self):
        _logger = _scala.scala.logger()
        level = _logger.level()
        if level < 5:
            self.trace = self.no_op

        if level < 4:
            self.debug = self.no_op

        if level < 3:
            self.info = self.no_op

        if level < 2:
            self.warn = self.no_op

        if level < 1:
            self.error = self.no_op
        return _logger

    def error(self, msg):
        self.logger.error(msg)

    def warn(self, msg):
        self.logger.warn(msg)

    def info(self, msg):
        self.logger.info(msg)

    def debug(self, msg):
        self.logger.debug(msg)

    def trace(self, msg):
        self.logger.trace(msg)

    def no_op(self, msg):
        pass


logger = Logger()


class FunctionWrapper(object):
    """class used to interface with python functions from scala"""
    def __init__(self, fun=None):
        self._fun = fun
        self.n_args = len(inspect.getfullargspec(fun).args)

    def eval_from_jvm(self, *args):
        return to_jvm(self(*(to_python(v) for v in args)))

    def __call__(self, *args, **kwargs):
        return self._fun(*args, **kwargs)
