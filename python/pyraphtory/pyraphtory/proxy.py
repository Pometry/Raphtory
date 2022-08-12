from collections.abc import Iterable, Iterator
from functools import cached_property

from pyraphtory import interop
from pyraphtory.interop import logger, register


class ScalaProxyBase(object):
    _jvm_object = None


class GenericScalaProxy(ScalaProxyBase):
    _methods = None
    _initialised = False
    _classname = None
    _jvm_object = None

    @classmethod
    def _init_methods(cls, jvm_object):
        if not cls._initialised:
            logger.trace(f"uninitialised class {cls.__name__}")
            if jvm_object is None:
                raise RuntimeError("Need object to find methods")
            logger.trace(f"Getting methods for {jvm_object}")
            methods = interop.get_methods(jvm_object)
            for (name, method_array) in methods.items():
                setattr(cls, name, MethodProxyDescriptor(name, method_array))
            cls._initialised = True

    @property
    def classname(self):
        if self._classname is None:
            if self._jvm_object is not None:
                self._classname = self._jvm_object.getClass().getName()
                logger.trace(f"Retrieved name {self._classname!r} from java object")
            else:
                raise RuntimeError("No classname and no jvm_object initialised")
        logger.trace(f"Return name {self._classname!r}")
        return self._classname

    def __call__(self, *args, **kwargs):
        logger.trace(f"{self!r} called with {args=} and {kwargs=}")
        return self.apply(*args, **kwargs)

    def __getattr__(self, item):
        if self._initialised:
            raise AttributeError(f"Attribute {item} does not exist for {self!r}")
        else:
            if self._jvm_object is None:
                if self._classname is not None:
                    self._jvm_object = interop.find_class(self._classname)
                else:
                    raise AttributeError("Uninitialised class has no attributes")
            self._init_methods(self._jvm_object)
            return getattr(self, item)

    def __repr__(self):
        try:
            return self._jvm_object.toString()
        except Exception as e:
            return repr(self._jvm_object)

    def __new__(cls, jvm_object=None):
        if jvm_object is not None:
            if not cls._initialised:
                cls._init_methods(jvm_object)
        self = super().__new__(cls)
        self._jvm_object = jvm_object
        return self

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls._classname is not None:
            register(cls)


class MethodProxyDescriptor(object):
    def __init__(self, name, methods):
        self.name = name
        self.methods = methods

    def __get__(self, instance, owner=None):
        if instance is None:
            # May shadow class method of the same name!
            try:
                return owner.__class__.__dict__[self.name].__get__(owner, owner.__class__)
            except KeyError:
                raise AttributeError()
        return GenericMethodProxy(self.name, instance._jvm_object, self.methods)


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
    _base_initialised = False

    def __new__(mcs, name, bases, attrs, **kwargs):
        if "_classname" not in attrs:
            actual_mcs = mcs
        else:
            actual_mcs = type.__new__(mcs, name + "_", (mcs,), {"_classname": attrs["_classname"]})
        return type.__new__(actual_mcs, name, bases, attrs, **kwargs)

    def __getattr__(self, item):
        if self._base_initialised:
            raise AttributeError(f"Attribute {item} does not exist for {self!r}")
        else:
            if self._jvm_object is None and self._classname is not None:
                self._jvm_object = interop.find_class(self._classname)
                self._init_base_methods(self._jvm_object)
                return getattr(self, item)
            else:
                raise AttributeError("Uninitialised class has no attributes")

    @classmethod
    def _init_base_methods(mcs, jvm_object):
        if not mcs._base_initialised:
            logger.trace(f"uninitialised class {mcs.__name__}")
            if jvm_object is None:
                raise RuntimeError("Need object to find methods")
            logger.trace(f"Getting methods for {jvm_object}")
            methods = interop.get_methods(jvm_object)
            for (name, method_array) in methods.items():
                setattr(mcs, name, MethodProxyDescriptor(name, method_array))
            mcs._base_initialised = True

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
            logger.trace("calling scala constructor")
            return cls.apply(*args, **kwargs)


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


class BuiltinAlgorithm(object):
    def __init__(self, path: str):
        self._path = path

    def __call__(self, *args, **kwargs):
        return GenericScalaProxy(interop.find_class(self._path)).apply(*args, **kwargs)

    def __getattr__(self, item):
        return BuiltinAlgorithm(".".join((self._path, item)))
