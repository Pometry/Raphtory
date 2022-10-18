from pyraphtory.interop import ScalaClassProxy


class SchemaProviders(ScalaClassProxy):
    _classname = "com.raphtory.internals.communication.SchemaProviderInstances"

    @classmethod
    def get_schema(cls, obj):
        return cls.generic_schema_provider()
