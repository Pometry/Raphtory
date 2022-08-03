from pyraphtory.proxy import ConstructableScalaProxy, GenericScalaProxy
from pyraphtory.interop import logger, assign_id


class Type(ConstructableScalaProxy):
    _classname = "com.raphtory.api.input.Type"


class StringProperty(ConstructableScalaProxy):
    _classname = "com.raphtory.api.input.StringProperty"


class ImmutableProperty(ConstructableScalaProxy):
    _classname = "com.raphtory.api.input.ImmutableProperty"


class Properties(ConstructableScalaProxy):
    _classname = "com.raphtory.api.input.Properties"

    def __init__(self, *args, jvm_object=None):
        super(Properties, self).__init__(args, jvm_object=jvm_object)


class BaseBuilder(GenericScalaProxy):
    def __init__(self):
        logger.trace("initialising GraphBuilder")
        super().__init__(None)

    # TODO: This should hopefully not be necessary soon and we can construct it normally
    def _set_jvm_builder(self, jvm_builder):
        self._jvm_object = jvm_builder

    def parse_tuple(self, line: str):
        pass

    @staticmethod
    def assign_id(s: str):
        return assign_id(s)
