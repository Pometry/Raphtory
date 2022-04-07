

from threading import RLock

from raphtoryclient.java_gateway import launch_gateway


class RaphtoryContext(object):
    """
    Main entry point for Raphtory functionality. A RaphtoryContext represents the
    connection to a Raphtory cluster. It can be used to submit algorithms to the
    cluster.
    """
    _gateway = None
    _jvm = None
    _lock = RLock()

    def __init__(self, gateway=None, jsc=None):
        if gateway is not None and gateway.gateway_parameters.auth_token is None:
            raise ValueError(
                "You are trying to pass an insecure Py4j gateway to Raphtory. This"
                " is not allowed as it is a security risk.")

        RaphtoryContext._ensure_initialized(self, gateway=gateway)
        # # TODO HERE HAAROON
        # RaphtoryContext.load()

    # def load(self):
    #     # Create the Java SparkContext through Py4J
    #     self._jsc = self._initialize_context()

    def hi(self):
        print("hi")

    def _initialize_context(self):
        self._jvm.JavaRaphtoryContext()


    @classmethod
    def _ensure_initialized(cls, gateway=None, instance=None):
        """
        Checks whether a RaphtoryContext is initialized or not.
        """
        with RaphtoryContext._lock:
            if not RaphtoryContext._gateway:
                RaphtoryContext._gateway = gateway or launch_gateway()
                RaphtoryContext._jvm = RaphtoryContext._gateway.jvm

        if instance:
            RaphtoryContext._active_raphtory_context = instance