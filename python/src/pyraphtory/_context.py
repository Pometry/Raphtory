from pyraphtory._codegen._codegen import LazyStr


def local():
    from pyraphtory.api.context import PyRaphtory
    return PyRaphtory.local()


def _local_docs():
    from pyraphtory.api.context import PyRaphtory
    return str(PyRaphtory.local.__doc__)


local.__doc__ = LazyStr(initial=lambda: _local_docs())


def remote(host: str = None, port: int = None):
    from pyraphtory.api.context import PyRaphtory
    kwargs = {}

    if host is not None:
        kwargs["host"] = host

    if port is not None:
        kwargs["port"] = port

    return PyRaphtory.remote(**kwargs)


def _remote_docs():
    from pyraphtory.api.context import PyRaphtory
    return str(PyRaphtory.remote.__doc__)


remote.__doc__ = LazyStr(initial=lambda: _remote_docs())