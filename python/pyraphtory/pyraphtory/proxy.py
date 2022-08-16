from pyraphtory import interop
from pyraphtory.interop import logger, register


class ScalaProxyBase(object):
    """Base class for wrapping jvm objects"""
    _jvm_object = None

    @property
    def jvm(self):
        """Access the wrapped jvm object directly"""
        return self._jvm_object


class GenericScalaProxy(ScalaProxyBase):
    """Base class for proxy objects that are not constructable from python

    If a subclass defines a '_classname' attribute, it will be automatically
    registered as the base class for proxy objects of that java class.
    """
    _methods = None
    _initialised = False
    _classname = None
    _jvm_object = None

    @classmethod
    def _init_methods(cls, jvm_object):
        """
        Create method wrappers for all jvm methods (only called the first time a wrapper is created)

        :param jvm_object: java object to wrap
        """
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
        """The name of the underlying jvm class of this wrapper"""
        if self._classname is None:
            if self._jvm_object is not None:
                self._classname = self._jvm_object.getClass().getName()
                logger.trace(f"Retrieved name {self._classname!r} from java object")
            else:
                raise RuntimeError("No classname and no jvm_object initialised")
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
                    self._jvm_object = interop.find_class(self._classname)
                else:
                    raise AttributeError("Uninitialised class has no attributes")
            self._init_methods(self._jvm_object)
            return getattr(self, item)

    def __repr__(self):
        try:
            return self._jvm_object.toString()
        except Exception as e:
            return f"{self.__class__.__name__}({self._jvm_object!r})"

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


class MethodProxyDescriptor(object):
    """Descriptor object for adding scala methods to class"""
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
    """Proxy object for scala method with support for default arguments and implicits"""
    # TODO: This needs to be optimised
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
        """support specifying implicit arguments with [val] syntax"""
        self._implicits.append(interop.to_jvm(item))
        return self


class ScalaObjectProxy(ScalaProxyBase, type):
    """Metaclass for wrapping Scala companion objects"""
    _base_initialised = False

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
                self._jvm_object = interop.find_class(self._classname)
                self._init_base_methods(self._jvm_object)
                return getattr(self, item)
            else:
                raise AttributeError("Uninitialised class has no attributes")

    @classmethod
    def _init_base_methods(mcs, jvm_object):
        """add companion object methods to metaclass"""
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
        """Calls scala constructor if no 'jvm_object' is given, otherwise initialises the wrapper"""
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
    """Base class for wrapper objects that are constructable from python"""
    pass


class Function1(ScalaClassProxy):
    """Proxy object for wrapping python functions with 1 argument"""
    _classname = "com.raphtory.internals.management.PythonFunction1"


class Function2(ScalaClassProxy):
    """Proxy object for wrapping python functions with 2 arguments"""
    _classname = "com.raphtory.internals.management.PythonFunction2"


class BuiltinAlgorithm(ScalaProxyBase):
    """Proxy object for looking up built-in algorithms based on path

    (This actually could be used for looking up other classes as well if needed)
    """
    @property
    def _jvm_object(self):
        return interop.find_class(self._path)

    def __init__(self, path: str):
        self._path = path

    def __call__(self, *args, **kwargs):
        return interop.to_python(self._jvm_object).apply(*args, **kwargs)

    def __getattr__(self, item):
        return BuiltinAlgorithm(".".join((self._path, item)))
