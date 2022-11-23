import jpype.imports
from jpype import JImplements, JOverride, JString, JObject, JClass
import pydevd
from traceback import print_exc

Interpreter = JClass("pemja.core.Interpreter")
from java.util import Map
from java.lang import Object


@JImplements(Interpreter)
class JPypeInterpreter:
    _locals = {}

    @JOverride
    def set(self, name: JString, value: JObject):
        # pydevd.connected = True
        # pydevd.settrace(suspend=False)
        try:
            self._locals[name] = value
        except Exception as e:
            print_exc()
            raise e

    @JOverride
    def get(self, *args):
        # pydevd.connected = True
        # pydevd.settrace(suspend=False)
        try:
            obj = self._locals[args[0]]
            if len(args) == 1:
                return obj
            else:
                return JObject(obj, args[1])
        except Exception as e:
            print_exc()
            raise e

    @JOverride
    def invoke(self, *args):
        # pydevd.connected = True
        # pydevd.settrace(suspend=False)
        try:
            fun = self._locals[args[0]]
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
        # pydevd.connected = True
        # pydevd.settrace(suspend=False)
        try:
            obj = self._locals[obj]
            method = getattr(obj, str(method))
            result = method(*args)
        except Exception as e:
            print_exc()
            raise e
        return result

    @JOverride
    def exec(self, code: JString):
        # pydevd.connected = True
        # pydevd.settrace(suspend=False)
        try:
            exec(str(code), globals(), self._locals)
        except Exception as e:
            print_exc()
            raise e
        return

    @JOverride
    def close(self):
        # pydevd.connected = True
        # pydevd.settrace(suspend=False)
        self._locals = {}
