try:
    import pydevd
    pydevd.connected = True
    pydevd.settrace(suspend=False)
    def enable_pydev_debug():
        """
        Use this function to ensure the pydevd debugger (used by e.g. Pycharm)
        works for python code called from a thread created by the jvm.

        """
        pydevd.connected = True
        pydevd.settrace(suspend=False)
except Exception as e:
    def enable_pydev_debug():
        pass




