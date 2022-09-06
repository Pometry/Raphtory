import abc
import inspect
import re
import traceback
from abc import ABCMeta
from collections.abc import Iterable, Mapping
from py4j.java_gateway import JavaObject, JavaClass
from py4j.java_collections import JavaArray
import cloudpickle as pickle
from functools import cached_property
from threading import Lock
from copy import copy

from pyraphtory import _codegen

_wrapper_lock = Lock()
_wrappers = {}


def repr(obj):
    return _scala.scala.repr(obj)


# stay sane while debugging this code
JavaArray.__repr__ = repr
JavaArray.__str__ = repr
JavaObject.__repr__ = repr
JavaObject.__str__ = repr
JavaClass.__repr__ = repr
JavaClass.__str__ = repr


def test_scala_reflection(obj):
    return _scala.scala.methods2(obj)


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
    logger.trace("Finding methods for {obj!r}", obj=obj)
    return _scala.scala.methods(obj)


def get_wrapper(obj):
    """get wrapper class for a java object"""
    name = obj.getClass().getName()
    logger.trace("Retrieving wrapper for {name!r}", name=name)
    try:
        wrapper = _wrappers[name]
        logger.trace("Found wrapper for {name!r} based on class name", name=name)
    except KeyError:
        # Create a new base class for the jvm wrapper and add methods
        with _wrapper_lock:
            if name in _wrappers:
                # a different thread already created the wrapper
                wrapper = _wrappers[name]
                logger.trace("Found wrapper for {name!r} based on class name after initial wait", name=name)
            else:
                base = type(name + "_jvm", (GenericScalaProxy,), {})
                # do not register base wrapper here, or it will get picked up by other threads
                base._init_methods(obj)
                # Check if a special wrapper class is registered for the object
                wrap_name = _scala.scala.get_wrapper_str(obj)
                if wrap_name in _wrappers:
                    # Add special wrapper class to the top of the mro such that method overloads work
                    logger.trace("Using wrapper based on name {wrap_name} for {name}", wrap_name=wrap_name, name=name)
                    # Note this wrapper is registered automatically as '_classname' is defined
                    wrapper = type(name, (_wrappers[wrap_name], base), {"_classname": name})
                else:
                    # No special wrapper registered, can use base wrapper directly
                    logger.trace("No wrapper found for name {}", wrap_name)
                    wrapper = base
                    # register the base wrapper in this case
                    wrapper._classname = name
                    register(wrapper)
                logger.trace("New wrapper created for {name!r}", name=name)
    logger.trace("Wrapper is {wrapper!r}", wrapper=wrapper)
    return wrapper


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
    else:
        logger.trace("Primitive object {obj!r} passed to python unchanged", obj=obj)
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
        return to_jvm(Function1(pickle_bytes))
    elif wrapped.n_args == 2:
        return to_jvm(Function2(pickle_bytes))
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

    def error(self, msg: str, *args, **kwargs):
        self.logger.error(msg.format(*args, **kwargs))

    def warn(self, msg, *args, **kwargs):
        self.logger.warn(msg.format(*args, **kwargs))

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg.format(*args, **kwargs))

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg.format(*args, **kwargs))

    def trace(self, msg, *args, **kwargs):
        self.logger.trace(msg.format(*args, **kwargs))

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


def _check_default(obj, value):
    if isinstance(value, DefaultValue):
        return value(obj)
    else:
        return to_jvm(value)


class ScalaProxyBase(object):
    """Base class for wrapping jvm objects"""
    _jvm_object = None

    @property
    def jvm(self):
        """Access the wrapped jvm object directly"""
        return self._jvm_object



    @classmethod
    def _add_method(cls, name, method_array):
        name = _codegen.clean_identifier(name)
        output = {}
        if len(method_array) > 1:
            for i, method in enumerate(sorted(method_array, key=lambda m: m.n())):
                try:
                    exec(_codegen.build_method(f"{name}{i}", method), globals(), output)
                except Exception as e:
                    traceback.print_exc()
                    raise e
            methods = list(output.values())
            method = OverloadedMethod(methods, name)
        else:
            method = method_array[0]
            try:
                exec(_codegen.build_method(name, method), globals(), output)
            except Exception as e:
                traceback.print_exc()
                raise e
            method = output.pop(name)
        if any(m.implicits() for m in method_array):
            method = WithImplicits(method)
        setattr(cls, name, InstanceOnlyMethod(method))


class GenericScalaProxy(ScalaProxyBase):
    """Base class for proxy objects that are not constructable from python

    If a subclass defines a '_classname' attribute, it will be automatically
    registered as the base class for proxy objects of that java class.
    """
    _initialised = False
    _classname = None
    _jvm_object = None
    _init_lock = Lock()

    def __repr__(self):
        if self._jvm_object is not None:
            return repr(self._jvm_object)
        else:
            return super().__repr__()

    __str__ = __repr__

    @classmethod
    def _init_methods(cls, jvm_object):
        """
        Create method wrappers for all jvm methods (only called the first time a wrapper is created)

        :param jvm_object: java object to wrap
        """
        with cls._init_lock:
            # this should only happen once in the case of multiple threads
            if not cls._initialised:
                logger.trace(f"uninitialised class {cls.__name__}")
                if jvm_object is None:
                    raise RuntimeError("Need object to find methods")
                logger.trace(f"Getting methods for {jvm_object}")
                methods = get_methods(jvm_object)
                for (name, method_array) in methods.items():
                    cls._add_method(name, method_array)
                cls._initialised = True

    @property
    def classname(self):
        """The name of the underlying jvm class of this wrapper"""
        if self._classname is None:
            if self._jvm_object is not None:
                self._classname = self._jvm_object.getClass().getName()
                logger.trace(f"Retrieved name {self._classname!r} from java object")
        logger.trace(f"Return name {self._classname!r}")
        return self._classname

    def __call__(self, *args, **kwargs):
        """
        Calling a wrapper calls the 'apply' method of the jvm object
        """
        logger.trace(f"{self!r} called with {args=} and {kwargs=}")
        return self.apply(*args, **kwargs)

    def __getattr__(self, item):
        """Triggers method initialisation if the class is uninitialised, otherwise just raises AttributeError"""
        if self._initialised:
            raise AttributeError(f"Attribute {item} does not exist for {self!r}")
        else:
            if self._jvm_object is None:
                if self._classname is not None:
                    self._jvm_object = find_class(self._classname)
                else:
                    raise AttributeError(f"Uninitialised class {self.__class__.__name__} has no attributes")
            self._init_methods(self._jvm_object)
            return getattr(self, item)

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
            register(cls)


class InstanceOnlyMethod(object):
    """Instance method that does not shadow class method of the same name"""
    def __init__(self, method):
        self.__func__ = method
        self.__name__ = method.__name__
        self.__signature__ = inspect.signature(method)
        self.__doc__ = f"Instance only method {self.__name__}{self.__signature__}"

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
        self.__doc__ = (f"Overloaded method {self.__name__} with alternatives\n"
                        + "\n".join(f"{self.__name__}{inspect.signature(m)}" for m in self._methods))

    def __call__(self, *args, **kwargs):
        for method in self._methods:
            try:
                return method(*args, **kwargs)
            except Exception as e:
                logger.trace("call failed for {name} with exception {e}", e=e, name=self.__name__)
        raise RuntimeError(f"No overloaded implementations matched for {self.__name__} with {args=} and {kwargs=}")

    def __get__(self, instance, owner):
        if instance is None:
            return self
        else:
            bound = copy(self)
            bound._methods = [m.__get__(instance, owner) for m in bound._methods]
            return bound

    __text_signature__ = "($self, *args, **kwargs)"


class WithImplicits:
    """Proxy object for scala method with support for default arguments and implicits"""
    def __init__(self, method):
        self.__name__ = method.__name__
        self._method = method
        self._implicits = []
        self.__signature__ = inspect.signature(method)
        self.__doc__ = (method.__doc__ + "\n\n" if method.__doc__ is not None else "") + "takes implicit arguments"

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
    _init_lock = Lock()

    @property
    def jvm(self):
        if self._jvm_object is not None:
            return self._jvm_object
        elif self._classname is not None:
            return find_class(self._classname)
        else:
            return None

    def __new__(mcs, name, bases, attrs, **kwargs):
        """Injects an additional specialised type to avoid interference between different classes"""
        if "_classname" not in attrs:
            actual_mcs = mcs
        else:
            actual_mcs = type.__new__(mcs, name + "_", (mcs,), {"_classname": attrs["_classname"]})
        return type.__new__(actual_mcs, name, bases, attrs, **kwargs)

    def __getattr__(self, item):
        """Lazy initialisation of class attributes to avoid import errors due to missing java connection"""
        if self._base_initialised:
            raise AttributeError(f"Attribute {item} does not exist for {self!r}")
        else:
            if self._jvm_object is None and self._classname is not None:
                self._jvm_object = find_class(self._classname)
                self._init_base_methods(self._jvm_object)
                return getattr(self, item)
            else:
                raise AttributeError("Uninitialised class has no attributes")

    @classmethod
    def _init_base_methods(mcs, jvm_object):
        """add companion object methods to metaclass"""
        with mcs._init_lock:
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


class ScalaClassProxy(GenericScalaProxy, metaclass=ScalaObjectProxy):
    """Base class for wrapper objects that are constructable from python"""
    @classmethod
    def _build_from_python(cls, *args, **kwargs):
        """Override to control python-side constructor behaviours (e.g., using a builder for sequence construction)"""
        return cls.apply(*args, **kwargs)

    def __new__(cls, *args, jvm_object=None, **kwargs):
        if jvm_object is None:
            # call scala constructor
            return cls._build_from_python(*args, **kwargs)
        else:
            # construct from existing object
            self = super().__new__(cls, jvm_object=jvm_object)
            return self


class Function1(ScalaClassProxy):
    """Proxy object for wrapping python functions with 1 argument"""
    _classname = "com.raphtory.internals.management.PythonFunction1"


class Function2(ScalaClassProxy):
    """Proxy object for wrapping python functions with 2 arguments"""
    _classname = "com.raphtory.internals.management.PythonFunction2"


