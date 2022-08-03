from collections.abc import Iterable, Iterator

from pyraphtory import interop
from pyraphtory.interop import logger, register
from pemja import findClass


class GenericScalaProxy(object):
    _classname = None

    def __init__(self, jvm_object=None):
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

    @property
    def _method_dict(self):
        if self._methods is None:
            logger.trace(f"Getting methods for {self._jvm_object}")
            self._methods = interop.get_methods(self._get_classname())
        else:
            logger.trace(f"Retreiving cached methods for {self}")
        return self._methods

    def __getattr__(self, name):
        logger.trace(f"__getattr__ called for {self!r} with {name!r}")
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
        args = [interop.to_jvm(v) for v in args]
        kwargs = {k: interop.to_jvm(v) for k, v in kwargs.items()}
        logger.trace(f"Calling {self!r} with *args={args!r} and **kwargs={kwargs!r}")
        return interop.to_python(self._jvm_object(*args, **kwargs))


class GenericMethodProxy(object):
    def __init__(self, name: str, jvm_object, methods):
        self.name = name
        self._jvm_object = jvm_object
        self._methods = methods

    def __call__(self, *args, **kwargs):
        args = [interop.to_jvm(v) for v in args]
        kwargs = {k: interop.to_jvm(v) for k, v in kwargs.items()}
        logger.trace(f"Trying to call method {self.name} with arguments {args=} and {kwargs=}")
        for method in self._methods:
            try:
                extra_args = []
                parameters = method.parameters()
                logger.trace(f"Parmeters for candidate are {parameters}")
                defaults = method.defaults()
                logger.trace(f"Defaults for candidate are {defaults}")
                n = method.n()
                logger.trace(f"Number of parameters for candidate is {n}")
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
                    return interop.to_python(getattr(self._jvm_object, method.name())(*args, *extra_args))
                else:
                    raise ValueError(f"Not all kwargs could be applied")
            except Exception as e:
                logger.trace(f"Call failed with exception {e}")
        raise ValueError(f"No matching implementation of method {self.name} with arguments {args=} and {kwargs=}")


class ConstructableScalaProxy(GenericScalaProxy):
    _classname: str = ""

    @classmethod
    def _constructor(cls):
        logger.trace(f"trying to construct {cls._classname}")
        c = findClass(cls._classname.replace(".", "/"))
        logger.trace(f"Classname is now {cls._classname}")
        logger.trace(f"found {c}")
        obj = GenericScalaProxy(c)
        obj._classname = cls._classname
        return obj

    @classmethod
    def _construct_from_python(cls, *args, **kwargs):
        # TODO: This creates a wrapper and then unwraps it again, clean up
        return interop.to_jvm(cls._constructor().apply(*args, **kwargs))

    def __init__(self, *args, jvm_object=None, **kwargs):
        if jvm_object is None:
            jvm_object = self._construct_from_python(*args, **kwargs)
        super().__init__(jvm_object)


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
