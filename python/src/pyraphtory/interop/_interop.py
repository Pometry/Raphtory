from __future__ import annotations

import inspect
import re
import traceback
from abc import ABCMeta, ABC
import os

from typing import *
import cloudpickle as pickle
from functools import cached_property
from threading import Lock, RLock
from copy import copy
from textwrap import indent
from pyraphtory._codegen import _codegen
from pyraphtory._codegen._codegen import jpype_type_converter, LazyStr
from jpype import JObject, JBoolean, JByte, JShort, JInt, JLong, JFloat, JDouble, JString

from pyraphtory._codegen._docstring import convert_docstring


@jpype_type_converter("Long")
def long_converter(value):
    return JLong(value)


@jpype_type_converter("Int")
def int_converter(value):
    return JInt(value)


@jpype_type_converter("Short")
def short_converter(value):
    return JShort(value)


@jpype_type_converter("Byte")
def byte_converter(value):
    return JByte(value)


@jpype_type_converter("Float")
def float_converter(value):
    return JFloat(value)


@jpype_type_converter("Double")
def double_converter(value):
    return JDouble(value)


@jpype_type_converter("Boolean")
def boolean_converter(value):
    return JBoolean(value)


_wrapper_lock = Lock()
_jpype = False


class NoCache:
    def __setitem__(self, key, value):
        pass


_globals = NoCache()


def no_wrapper(jvm_object):
    return jvm_object


_wrappers = {
    "java.lang.Integer": no_wrapper,
    "java.lang.Short": no_wrapper,
    "java.lang.Long": no_wrapper,
    "java.lang.Float": no_wrapper,
    "java.lang.Double": no_wrapper,
}


def repr(obj):
    return str(_scala.repr(obj))


def check_raphtory_logging_env():
    log_level = os.getenv('RAPHTORY_CORE_LOG')
    if log_level is None:
        os.environ["RAPHTORY_CORE_LOG"] = "ERROR"


try:
    from pemja import findClass

    _scala = findClass('com.raphtory.internals.management.PythonInterop')
except (ImportError, ModuleNotFoundError):
    import jpype
    import jpype.config
    jpype.config.onexit = False
    from jpype import JClass
    from pyraphtory import _config

    check_raphtory_logging_env()

    # see https://jpype.readthedocs.io/en/latest/userguide.html#errors-reported-by-python-fault-handler
    try:
        import faulthandler
        faulthandler.enable()
        faulthandler.disable()
    except:
        pass

    jpype.startJVM(_config.java_args, classpath=_config.jars.split(":"))
    _config._jpype_running = True
    from pyraphtory.interop._jpypeinterpreter import JPypeInterpreter, _globals

    _scala = getattr(JClass("com.raphtory.internals.management.PythonInterop$"), "MODULE$")
    _scala.set_interpreter(JPypeInterpreter())
    _jpype = True


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


_JPrimitiveTypes = (JBoolean, JByte, JShort, JInt, JLong, JFloat, JDouble, JString)


def _isJPrimitive(obj):
    return isinstance(obj, _JPrimitiveTypes)


def is_PyJObject(obj):
    """Needed because Pemja objects do not support isinstance"""
    return (type(obj).__name__ == "PyJObject"
            or (isinstance(obj, JObject) and not _isJPrimitive(obj)))


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
    return _scala.camel_to_snake(name)


def decode(obj):
    """call scala decode function to deal with converting java to scala collections"""
    return _scala.decode(obj)


def decode_tuple(obj):
    """call scala decode_tuple function to convert collection to scala tuple"""
    return _scala.decode_tuple(obj)


def get_methods(obj):
    """look up methods for a java object"""
    logger.trace("Finding methods for {obj!r}", obj=obj)
    return _scala.methods(obj)


def get_methods_from_name(name):
    logger.trace("Finding methods for {name} based on name", name=name)
    return _scala.methods_from_name(name)


def get_wrapper_for_name(name: str):
    logger.trace("Retrieving wrapper for {name!r}", name=name)
    wrapper = _wrappers[name]
    logger.trace("Found wrapper for {name!r} based on class name", name=name)
    return wrapper


def get_type_repr(tpe: Any):
    return _scala.get_type_repr(tpe)


def build_wrapper(name: str, obj: Any):
    # Create a new base class for the jvm wrapper and add methods
    with _wrapper_lock:
        if name in _wrappers:
            # a different thread already created the wrapper
            wrapper = _wrappers[name]
            logger.trace("Found wrapper for {name!r} based on class name after initial wait", name=name)
        else:
            # Check if a special wrapper class is registered for the object
            wrap_name = str(_scala.get_wrapper_str(obj))
            if wrap_name in _wrappers:
                # Add special wrapper class to the top of the mro such that method overloads work
                logger.debug("Using wrapper based on name {wrap_name} for {name}", wrap_name=wrap_name, name=name)
                # Note this wrapper is registered automatically as '_classname' is defined
                wrapper = type(name, (_wrappers[wrap_name],), {"_classname": name, "_initialised": False})
            else:
                # No special wrapper registered, can use base wrapper directly
                logger.debug("No wrapper found for name {}, using GenericScalaProxy", name)
                wrapper = type(name, (GenericScalaProxy,), {"_classname": name, "_initialised": False})
            logger.trace("New wrapper created for {name!r}", name=name)
    logger.trace("Wrapper is {wrapper!r}", wrapper=wrapper)
    return wrapper


def get_wrapper(obj):
    """get wrapper class for a java object"""
    name = str(obj.getClass().getName())
    try:
        return get_wrapper_for_name(name)
    except KeyError:
        return build_wrapper(name, obj)


def to_jvm(value):
    """convert wrapped object to underlying jvm representation"""
    if is_PyJObject(value):
        logger.trace("Converting value {value!r}, already PyJObject", value=value)
        return value
    elif isinstance(value, ScalaProxyBase):
        logger.trace("Converting value {value!r}, decoding proxy object", value=value)
        return value.jvm
    elif isinstance(value, Mapping):
        logger.trace("Converting value {value!r}, decoding as Mapping", value=value)
        return decode({to_jvm(k): to_jvm(v) for k, v in value.items()})
    elif callable(value):
        logger.trace("Converting value {value!r}, decoding as Function", value=value)
        return _wrap_python_function(value)
    elif isinstance(value, tuple):
        logger.trace(f"Converting value {value!r}, decoding as Tuple", value=value)
        return decode_tuple([to_jvm(v) for v in value])
    elif (isinstance(value, Iterable)
          and not isinstance(value, str)
          and not isinstance(value, bytes)
          and not isinstance(value, bytearray)):
        logger.trace("Converting value {value!r}, decoding as Iterable", value=value)
        return decode([to_jvm(v) for v in value])
    else:
        logger.trace("Converting value {value!r}, primitive value returned unchanged", value=value)
        return value


def to_python(obj):
    """convert jvm object to python by wrapping it if needed"""
    if is_PyJObject(obj):
        wrapper = get_wrapper(obj)
        logger.trace("Calling wrapper with jvm_object={obj}", obj=obj)
        return wrapper(jvm_object=obj)
    elif isinstance(obj, JString):
        return str(obj)
    else:
        logger.trace("Primitive object {obj!r} passed to python unchanged", obj=obj)
        return obj


def find_class(path: str):
    """get the scala companion object instance for a class path"""
    return _scala.find_class(path)


def assign_id(s: str):
    """call the asign_id function (used by graph builder)"""
    return _scala.assign_id(s)


def make_varargs(param):
    """convert parameter list to varargs-friendly array"""
    return _scala.make_varargs(param)


def _wrap_python_function(fun):
    """take a python function and turn it into a scala function"""
    eval_name = f"wrapped_{id(fun)}"
    wrapped = FunctionWrapper(fun)
    pickle_bytes = pickle.dumps(wrapped)
    _globals[eval_name] = wrapped
    if wrapped.n_args == 1:
        return to_jvm(Function1(pickle_bytes, eval_name))
    elif wrapped.n_args == 2:
        return to_jvm(Function2(pickle_bytes, eval_name))
    else:
        raise ValueError("Only functions with 1 or 2 arguments are currently implemented when passing to scala")


class Logger(object):
    """Wrapper for the java logger"""
    def _format_message(self, msg: str, *args, **kwargs):
        try:
            if args or kwargs:
                msg = msg.format(*args, **kwargs)
        except Exception as e:
            logger.error(f"failed to format {msg=} with {args=} and {kwargs=}, cause: {e}")
        finally:
            return msg

    @cached_property
    def logger(self):
        _logger = _scala.logger()
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

    def error(self, msg: str, *args, **kwargs):
        self.logger.error(self._format_message(msg, *args, **kwargs))

    def warn(self, msg, *args, **kwargs):
        self.logger.warn(self._format_message(msg, *args, **kwargs))

    def info(self, msg, *args, **kwargs):
        self.logger.info(self._format_message(msg, *args, **kwargs))

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(self._format_message(msg, *args, **kwargs))

    def trace(self, msg, *args, **kwargs):
        self.logger.trace(self._format_message(msg, *args, **kwargs))

    def no_op(self, msg, *args, **kwargs):
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


class DefaultValue(object):
    """Wrap a scala default value accessor"""

    def __init__(self, method):
        self.method = method

    def __call__(self, obj):
        return getattr(obj, self.method)()

    def __repr__(self):
        return "DefaultValue()"


def _check_default(obj, value, converter):
    if isinstance(value, DefaultValue):
        return value(obj)
    else:
        return converter(value)


class ScalaProxyBase(object):
    """Base class for wrapping jvm objects"""
    _jvm_object = None

    @property
    def jvm(self):
        """The underlying Scala jvm object
        """
        return self._jvm_object

    @classmethod
    def _add_method(cls, name, method_array):
        name = _codegen.clean_identifier(name)
        output = {}
        methods = []
        if len(method_array) > 1:
            for i, method in enumerate(sorted(method_array, key=lambda m: (m.varargs(), m.n()))):
                try:
                    methods.append(_codegen.build_method(f"{name}{i}", method, _jpype, globals(), output))
                except Exception as e:
                    traceback.print_exc()
                    raise e
            methods = list(output.values())
            method = OverloadedMethod(methods, name)
        else:
            method = method_array[0]
            try:
                method = _codegen.build_method(name, method, _jpype, globals(), output)
            except Exception as e:
                traceback.print_exc()
                raise e
        if any(m.implicits() for m in method_array):
            method = WithImplicits(method)
        setattr(cls, name, InstanceOnlyMethod(method))


class JVMBase(ScalaProxyBase):
    _initialised = False
    _classname = None
    _init_lock = RLock()

    @classmethod
    def _init_methods(cls, jvm_object):
        """
        Create method wrappers for all jvm methods (only called the first time a wrapper is created)

        :param jvm_object: java object to wrap
        """
        with cls._init_lock:
            # this should only happen once in the case of multiple threads
            if not cls._initialised:
                logger.debug(f"Initialising uninitialised class {cls.__name__}")
                if jvm_object is None:
                    if cls._classname is not None:
                        methods = get_methods_from_name(cls._classname)
                    else:
                        raise RuntimeError("Cannot initialise methods for class without object or classname")
                else:
                    if cls._classname is None:
                        cls._classname = jvm_object.getClass().getName()
                    logger.trace(f"Getting methods for {jvm_object}")
                    methods = get_methods(jvm_object)
                try:
                    jvm_base_index = next(i for i, b in enumerate(cls.__bases__) if issubclass(b, JVMBase)
                                          and not issubclass(b, GenericScalaProxy))
                except StopIteration:
                    jvm_base_index = len(cls.__bases__)
                base = type(cls.__name__ + "_jvm", (JVMBase,), {"_classname": cls._classname})
                for (name, method_array) in methods.items():
                    base._add_method(name, method_array)
                cls.__bases__ = (*cls.__bases__[:jvm_base_index], base, *cls.__bases__[jvm_base_index + 1:])
                if cls.__doc__ is None:
                    cls.__doc__ = LazyStr(initial=lambda: convert_docstring(_scala.docstring_for_class(cls._classname)))
                cls._initialised = True
            else:
                logger.trace("_init_method called for initialised class {}", cls.__name__)


class GenericScalaProxy(JVMBase):
    """Base class for proxy objects that are not constructable from python

    If a subclass defines a '_classname' attribute, it will be automatically
    registered as the base class for proxy objects of that java class.
    """

    def __repr__(self):
        if self._jvm_object is not None:
            return repr(self._jvm_object)
        else:
            return super().__repr__()

    __str__ = __repr__

    @property
    def classname(self):
        """The name of the underlying jvm class of this wrapper
        """
        if self._classname is None:
            if self._jvm_object is not None:
                self._classname = self._jvm_object.getClass().getName()
                logger.trace(f"Retrieved name {self._classname!r} from java object")
        logger.trace(f"Return name {self._classname!r}")
        return self._classname

    def __call__(self, *args, **kwargs):
        """
        Calling a wrapper calls the `apply` method
        """
        logger.trace(f"{self!r} called with {args=} and {kwargs=}")
        return self.apply(*args, **kwargs)

    def __new__(cls, jvm_object=None):
        """Create a new instance and trigger method initialisation if needed"""
        if jvm_object is not None:
            if not cls._initialised:
                cls._init_methods(jvm_object)
        self = super().__new__(cls)
        self._jvm_object = jvm_object
        return self

    def __init_subclass__(cls, **kwargs):
        """automatically register wrappers that have a '_classname' defined"""
        super().__init_subclass__(**kwargs)
        if cls._classname is not None:
            cls._initialised = False
            register(cls)
            cls._init_methods(None)


class InstanceOnlyMethod(object):
    """Instance method that does not shadow class method of the same name"""

    def __init__(self, method):
        self.__func__ = method
        self.__name__ = method.__name__

    def __set_name__(self, owner, name):
        self.__name__ = name

    def __get__(self, instance, owner=None):
        if instance is None:
            # May shadow class method of the same name!
            try:
                return object.__getattribute__(owner.__class__, self.__name__).__get__(owner, owner.__class__)
            except Exception as e:
                logger.trace("InstanceOnlyMethod non-shadowed due to exception {}", e)
                return self.__func__
        return self.__func__.__get__(instance, owner)

    def __str__(self):
        return f"{self.__name__}"


class OverloadedMethod:
    def __init__(self, methods, name):
        self.__name__ = name
        self._methods = methods
        self.__signature__ = inspect.signature(self.__class__.__call__)
        self.__doc__ = _codegen.LazyStr(
            initial=lambda: f"Overloaded method with alternatives\n\n"
                            + "\n\n".join(
                f"{self.__name__}{str(inspect.signature(m.__get__(m)))}"
                # hack to get signature as if bound method
                + ("\n" + indent(m.__doc__, "    ") if m.__doc__ else "")
                for m in self._methods)
        )

    def __call__(self, *args, **kwargs):
        errors = []
        for method in self._methods:
            try:
                return method(*args, **kwargs)
            except Exception as e:
                logger.trace("call failed for {name} with exception {e}", e=e, name=self.__name__)
                errors.append(e)
        for e in errors:
            print(e)
        raise RuntimeError(f"No overloaded implementations matched for {self.__name__} with {args=} and {kwargs=}")

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        else:
            bound = copy(self)
            bound.__self__ = instance
            bound._methods = [m.__get__(instance, owner) for m in bound._methods]
            return bound


class WithImplicits:
    """Proxy object for scala method with support for default arguments and implicits"""

    def __init__(self, method):
        self.__name__ = method.__name__
        self._method = method
        self._implicits = []
        self.__doc__ = method.__doc__
        self.__wrapped__ = method

    def __call__(self, *args, **kwargs):
        return self._method(*args, **kwargs, _implicits=self._implicits)

    def __getitem__(self, item):
        """support specifying implicit arguments with [val] syntax"""
        if not isinstance(item, tuple):
            item = (item,)
        self._implicits = item
        return self

    def __get__(self, instance, owner):
        if instance is None:
            return self
        else:
            bound = copy(self)
            bound._method = bound._method.__get__(instance, owner)
            return bound


class ScalaObjectProxy(ScalaProxyBase, ABCMeta, type):
    """Metaclass for wrapping Scala companion objects"""
    _base_initialised = False
    _base_init_lock = RLock()

    @property
    def jvm(self):
        """Underlying Scala companion object instance"""
        if self._jvm_object is not None:
            return self._jvm_object
        elif self._classname is not None:
            return find_class(self._classname)
        else:
            return None

    def __subclasscheck__(self, subclass):
        try:
            value = super().__subclasscheck__(subclass)
            return value
        except AttributeError as e:
            return False

    def _from_jvm(cls, jvm_object):
        if cls._base_initialised:
            return cls
        else:
            with cls._base_init_lock:
                cls._init_base_methods(jvm_object)
                cls._jvm_object = jvm_object
            return cls

    def __new__(mcs, name, bases, attrs, **kwargs):
        """Injects an additional specialised type to avoid interference between different classes"""
        concrete = "_classname" in attrs
        if not concrete:
            actual_mcs = mcs
        else:
            actual_mcs = type.__new__(mcs, name + "_", (mcs,), {"_classname": attrs["_classname"]})
            actual_mcs._init_base_methods(_scala.find_class(actual_mcs._classname))

        cls = type.__new__(actual_mcs, name, bases, attrs, **kwargs)
        if concrete:
            cls._jvm_object = _scala.find_class(actual_mcs._classname)
            register(cls._from_jvm, name=cls._classname + "$")

        return cls

    @classmethod
    def _init_base_methods(mcs, jvm_object):
        """add companion object methods to metaclass"""
        with mcs._base_init_lock:
            # this should only happen once in case of multiple threads
            if not mcs._base_initialised:
                logger.trace(f"uninitialised class {mcs.__name__}")
                if jvm_object is None:
                    raise RuntimeError("Need object to find methods")
                logger.trace(f"Getting methods for {jvm_object}")
                methods = get_methods(jvm_object)
                for (name, method_array) in methods.items():
                    mcs._add_method(name, method_array)
                mcs._base_initialised = True


class ScalaClassProxy(GenericScalaProxy, ABC, metaclass=ScalaObjectProxy):
    """Base class for wrapper objects that are constructable from python"""

    @classmethod
    def _build_from_python(cls, *args, **kwargs):
        """Override to control python-side constructor behaviours (e.g., using a builder for sequence construction)"""
        return cls.apply(*args, **kwargs)

    def __new__(cls, *args, jvm_object=None, **kwargs):
        """New instance construction from python uses the `apply` classmethod."""
        if jvm_object is None:
            # call scala constructor
            return cls._build_from_python(*args, **kwargs)
        else:
            # construct from existing object
            self = super().__new__(cls, jvm_object=jvm_object)
            return self


class ScalaClassProxyWithImplicits(ScalaClassProxy):
    _implicits = ()

    @classmethod
    def _build_from_python(cls, *args, **kwargs):
        """Override to control python-side constructor behaviours (e.g., using a builder for sequence construction)"""
        return cls.apply[cls._implicits](*args, **kwargs)

    def __class_getitem__(cls, item):
        new_cls = copy(cls)
        new_cls._implicits = item
        return new_cls


class Function1(ScalaClassProxy):
    """Proxy object for wrapping python functions with 1 argument"""
    _classname = "com.raphtory.internals.management.PythonFunction1"


class Function2(ScalaClassProxy):
    """Proxy object for wrapping python functions with 2 arguments"""
    _classname = "com.raphtory.internals.management.PythonFunction2"


class ScalaPackage(ScalaProxyBase):
    """Proxy object for looking up scala classes based on path
    """

    @property
    def _jvm_object(self):
        return find_class(self._path)

    def __init__(self, path: str):
        self._path = path

    def __call__(self, *args, **kwargs):
        return to_python(self._jvm_object).apply(*args, **kwargs)

    def __getattr__(self, item):
        return ScalaPackage(".".join((self._path, item)))

    def __getitem__(self, item):
        return to_python(self._jvm_object).apply[item]
