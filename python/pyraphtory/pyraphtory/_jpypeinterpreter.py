import jpype.imports
from jpype import JImplements, JOverride, JString, JObject, JClass
import pydevd

Interpreter = JClass("pemja.core.Interpreter")
from java.util import Map
from java.lang import Object


@JImplements(Interpreter)
class JPypeInterpreter:
    _locals = {}

    @JOverride
    def set(self, name: JString, value: JObject):
        pydevd.connected = True
        pydevd.settrace(suspend=False)
        self._locals[name] = value

    @JOverride
    def get(self, *args):
        pydevd.connected = True
        pydevd.settrace(suspend=False)
        obj = self._locals[args[0]]
        if len(args) == 1:
            return obj
        else:
            return JObject(obj, args[1])

    @JOverride
    def invoke(self, *args):
        pydevd.connected = True
        pydevd.settrace(suspend=False)
        fun = self._locals[args[0]]
        if isinstance(args[1], Map):
            return fun(**args[1])
        else:
            if len(args) > 2:
                return fun(*args[1], **args[2])
            else:
                return fun(*args[1])

    @JOverride
    def invokeMethod(self, obj: JString, method: JString, args):
        pydevd.connected = True
        pydevd.settrace(suspend=False)
        obj = self._locals[obj]
        method = getattr(obj, str(method))
        result = method(*args)
        return result

    @JOverride
    def exec(self, code: JString):
        pydevd.connected = True
        pydevd.settrace(suspend=False)
        try:
            exec(str(code), globals(), self._locals)
        except Exception as e:
            print(e)
            raise e
        return

    @JOverride
    def close(self):
        pydevd.connected = True
        pydevd.settrace(suspend=False)
        self._locals = {}
