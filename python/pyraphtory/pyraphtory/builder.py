from pyraphtory.proxy import ScalaClassProxy, GenericScalaProxy
from pyraphtory.interop import logger, assign_id


class Type(ScalaClassProxy):
    _classname = "com.raphtory.api.input.Type"


class StringProperty(ScalaClassProxy):
    _classname = "com.raphtory.api.input.StringProperty"


class ImmutableProperty(ScalaClassProxy):
    _classname = "com.raphtory.api.input.ImmutableProperty"


class Properties(ScalaClassProxy):
    _classname = "com.raphtory.api.input.Properties"

    @classmethod
    def _construct_from_python(cls, *args, **kwargs):
        super()._construct_from_python(args)


class BaseBuilder(GenericScalaProxy):
    def __init__(self):
        # logger.trace("initialising GraphBuilder")
        super().__init__(None)

    # TODO: This should hopefully not be necessary soon and we can construct it normally
    def _set_jvm_builder(self, jvm_builder):
        self._jvm_object = jvm_builder

    def parse_tuple(self, line: str):
        pass

    @staticmethod
    def assign_id(s: str):
        return assign_id(s)
