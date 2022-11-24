import jpype.imports
from jpype import JImplements, JOverride, JString, JObject, JClass
from pyraphtory.debug import enable_pydev_debug
from traceback import print_exc

Interpreter = JClass("pemja.core.Interpreter")
from java.util import Map

_globals = globals()

@JImplements(Interpreter)
class JPypeInterpreter:

    @JOverride
    def set(self, name: JString, value: JObject):
        enable_pydev_debug()
        try:
            globals()[name] = value
        except Exception as e:
            print_exc()
            raise e

    @JOverride
    def get(self, *args):
        enable_pydev_debug()
        try:
            obj = globals()[args[0]]
            if len(args) == 1:
                return obj
            else:
                return JObject(obj, args[1])
        except Exception as e:
            print_exc()
            raise e

    @JOverride
    def invoke(self, *args):
        enable_pydev_debug()
        try:
            fun = globals()[args[0]]
            if isinstance(args[1], Map):
                return fun(**args[1])
            else:
                if len(args) > 2:
                    return fun(*args[1], **args[2])
                else:
                    return fun(*args[1])
        except Exception as e:
            print_exc()
            raise e

    @JOverride
    def invokeMethod(self, obj: JString, method: JString, args):
        enable_pydev_debug()
        try:
            obj = globals()[obj]
            method = getattr(obj, str(method))
            result = method(*args)
        except Exception as e:
            print_exc()
            raise e
        return result

    @JOverride
    def exec(self, code: JString):
        enable_pydev_debug()
        try:
            exec(str(code), globals(), globals())
        except Exception as e:
            print_exc()
            raise e
        return

    @JOverride
    def close(self):
        pass
