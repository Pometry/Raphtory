from collections.abc import Iterable, Iterator
from functools import cached_property

from pyraphtory import interop
from pyraphtory.interop import logger, register, to_jvm, to_python


class ScalaProxyBase(object):
    _methods = None

    @property
    def _method_dict(self):
        if self._methods is None:
            logger.trace(f"Getting methods for {self._jvm_object}")
            self._methods = interop.get_methods(self._jvm_object)
        else:
            logger.trace(f"Retreiving cached methods for {self}")
        return self._methods

    def list_methods(self):
        return self._method_dict.keys().toString()

    def __getattr__(self, name):
        logger.trace(f"__getattr__ called for {self!r} with {name!r}")
        if self._method_dict.contains(name):
            return GenericMethodProxy(name, self._jvm_object, self._method_dict.apply(name))
        else:
            raise AttributeError(f"{self!r} object has no method {name!r}, available methods are {self._methods.keys().toString()}")

    def __call__(self, *args, **kwargs):
        logger.trace(f"{self!r} called with {args=} and {kwargs=}")
        return self.apply(*args, **kwargs)


class GenericScalaProxy(ScalaProxyBase):
    _classname = None

    def __init__(self, jvm_object=None):
        logger.trace(f"GenericScalaProxy initialized with {jvm_object=}")
        if jvm_object is not None:
            self._jvm_object = jvm_object
        self._methods = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls._classname is not None:
            register(cls)

    def _get_classname(self):
        if self._classname is None:
            self._classname = self._jvm_object.getClass().getName()
            logger.trace(f"Retrieved name {self._classname!r} from java object")
        logger.trace(f"Return name {self._classname!r}")
        return self._classname

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


class GenericMethodProxy(object):
    def __init__(self, name: str, jvm_object, methods):
        self.name = name
        self._jvm_object = jvm_object
        self._methods = methods
        self._implicits = []

    def __call__(self, *args, **kwargs):
        args = [interop.to_jvm(v) for v in args]
        kwargs = {k: interop.to_jvm(v) for k, v in kwargs.items()}
        logger.trace(f"Trying to call method {self.name} with arguments {args=} and {kwargs=} and implicits {self._implicits!r}")
        for method in self._methods:
            try:
                parameters = method.parameters()
                logger.trace(f"Parmeters for candidate are {parameters}")
                defaults = method.defaults()
                logger.trace(f"Defaults for candidate are {defaults}")
                n = method.n()
                logger.trace(f"Number of parameters for candidate is {n}")
                if method.varargs():
                    logger.trace(f"Method takes varargs")
                    actual_args = args[:n-len(kwargs)-1]
                    varargs = [interop.make_varargs(interop.to_jvm(args[n-len(kwargs)-1:]))]
                else:
                    actual_args = args[:]
                    varargs = []
                kwargs_used = 0
                if len(actual_args) + len(kwargs) + len(varargs) > n:
                    raise ValueError("Too many arguments")
                if len(actual_args) + len(varargs) < n:
                    for i in range(len(actual_args), n-len(self._implicits)-len(varargs)):
                        param = parameters[i]
                        if param in kwargs:
                            actual_args.append(kwargs[param])
                            kwargs_used += 1
                        elif defaults.contains(i):
                            actual_args.append(getattr(self._jvm_object, defaults.apply(i))())
                        else:
                            raise ValueError(f"Missing value for parameter {param}")
                if kwargs_used == len(kwargs):
                    return interop.to_python(getattr(self._jvm_object, method.name())(*actual_args, *varargs, *self._implicits))
                else:
                    raise ValueError(f"Not all kwargs could be applied")
            except Exception as e:
                logger.trace(f"Call failed with exception {e}")
        raise ValueError(f"No matching implementation of method {self.name} with arguments {args=} and {kwargs=} and implicits {self._implicits}")

    def __getitem__(self, item):
        self._implicits.append(interop.to_jvm(item))
        return self


class ScalaObjectProxy(ScalaProxyBase, type):
    @property
    def _jvm_object(cls):
        if cls._classname is None:
            raise AttributeError("Cannot find jvm object for proxy without classname set.")
        return interop.find_class(cls._classname)

    def __call__(cls, *args, jvm_object=None, **kwargs):
        logger.trace(f"ScalaCompanionObjectProxy called with {args=}, {jvm_object=}, {kwargs=}")
        if jvm_object is not None:
            # Construct from existing jvm object
            logger.trace("calling cls.__new__")
            self = cls.__new__(cls, jvm_object=jvm_object)
            logger.trace("object constructed")
            self.__init__(jvm_object=jvm_object)
            return self
        else:
            # Return the result of calling scala apply method
            logger.trace("calling super()")
            return super().__call__(*args, **kwargs)


class ScalaClassProxy(GenericScalaProxy, metaclass=ScalaObjectProxy):
    pass


@register(name="Iterable")
class IterableScalaProxy(GenericScalaProxy, Iterable):
    def __iter__(self):
        return self.iterator()


@register(name="Iterator")
class IteratorScalaProxy(GenericScalaProxy, Iterator):

    def __next__(self):
        if self.has_next():
            return self.next()
        else:
            raise StopIteration


class Function1(ScalaClassProxy):
    _classname = "com.raphtory.internals.management.PythonFunction1"


class Function2(ScalaClassProxy):
    _classname = "com.raphtory.internals.management.PythonFunction2"


class Algorithm(object):
    def __init__(self, path: str):
        self._path = path

    def __call__(self, *args, **kwargs):
        return GenericScalaProxy(interop.find_class(self._path)).apply(*args, **kwargs)

    def __getattr__(self, item):
        return Algorithm(".".join((self._path, item)))
